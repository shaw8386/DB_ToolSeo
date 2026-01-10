/**
 * index.js
 * Backend Railway: Hidden Upload Excel (input/output) -> Cloudflare R2
 * - DB Postgres stores R2 accounts
 * - Auto create tables at startup
 * - Rotation: if an account hits quota/rate-limit, auto cooldown and switch to next
 *
 * Required ENV:
 *   - DATABASE_URL
 *   - HIDDEN_UPLOAD_TOKEN
 *
 * Optional ENV:
 *   - R2_KEY_PREFIX (default: seo-web)
 *   - R2_COOLDOWN_HOURS (default: 6)
 *   - R2_MONTHLY_SOFT_LIMIT_GB (default: 0 -> disabled)
 *   - PGSSLMODE (if set, enables ssl rejectUnauthorized:false)
 */

const express = require("express");
const { Pool } = require("pg");
const multer = require("multer");
const crypto = require("crypto");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");

const app = express();
app.use(express.json({ limit: "5mb" }));

// =========================
// ENV
// =========================
const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL || "";

const HIDDEN_UPLOAD_TOKEN = process.env.HIDDEN_UPLOAD_TOKEN || "";
const R2_KEY_PREFIX = process.env.R2_KEY_PREFIX || "seo-web";

const COOLDOWN_HOURS = parseInt(process.env.R2_COOLDOWN_HOURS || "6", 10);
const SOFT_LIMIT_GB = parseFloat(process.env.R2_MONTHLY_SOFT_LIMIT_GB || "0"); // 0 = off

// Multer memory upload (Excel usually small-ish). If huge files, switch to disk/stream.
const upload = multer({ storage: multer.memoryStorage() });

// =========================
// DB
// =========================
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: process.env.PGSSLMODE ? { rejectUnauthorized: false } : undefined
});

// =========================
// Helpers
// =========================
function monthKeyNow() {
  const d = new Date();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  return `${d.getUTCFullYear()}-${mm}`;
}

function safeErr(e, n = 600) {
  const s = e && e.stack ? e.stack : String(e || "");
  return s.length > n ? s.slice(0, n) + "..." : s;
}

function buildObjectKey(prefix, sessionId, kind, filename) {
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const base = filename.replace(/\\/g, "/").split("/").pop();
  const rand = crypto.randomBytes(4).toString("hex");
  return `${prefix}/${sessionId}/${ts}/${kind}/${rand}_${base}`;
}

function buildS3Client(acc) {
  const endpoint = `https://${acc.account_id}.r2.cloudflarestorage.com`;
  return new S3Client({
    region: acc.region || "auto",
    endpoint,
    credentials: {
      accessKeyId: acc.access_key_id,
      secretAccessKey: acc.secret_access_key
    }
  });
}

// “thực chiến”: nhận diện quota / rate limit
function isQuotaOrRateLimitError(err) {
  const msg = (err && err.message ? err.message : String(err || "")).toLowerCase();
  const http = err && err.$metadata && err.$metadata.httpStatusCode ? err.$metadata.httpStatusCode : 0;
  const name = err && err.name ? String(err.name) : "";

  if ([429, 403, 507].includes(http)) return true;
  if (msg.includes("rate") && msg.includes("limit")) return true;
  if (msg.includes("quota") || msg.includes("exceed")) return true;
  if (name.includes("SlowDown") || name.includes("Throttl")) return true;
  return false;
}

// =========================
// Auto CREATE TABLE
// =========================
async function ensureTables() {
  const sql = `
    CREATE TABLE IF NOT EXISTS r2_accounts (
      id                 BIGSERIAL PRIMARY KEY,
      name               TEXT NOT NULL,
      account_id         TEXT NOT NULL,
      access_key_id      TEXT NOT NULL,
      secret_access_key  TEXT NOT NULL,
      bucket             TEXT NOT NULL,
      region             TEXT DEFAULT 'auto',

      is_active          BOOLEAN DEFAULT TRUE,
      status             TEXT DEFAULT 'ok',  -- ok | cooldown | limited | error
      disabled_until     TIMESTAMPTZ,
      last_error         TEXT,
      last_error_at      TIMESTAMPTZ,

      bytes_used_month   BIGINT DEFAULT 0,
      month_key          TEXT,
      last_used_at       TIMESTAMPTZ,

      priority           INT DEFAULT 100,
      cursor_score       BIGINT DEFAULT 0
    );

    CREATE INDEX IF NOT EXISTS idx_r2_accounts_pick
    ON r2_accounts (is_active, status, disabled_until, priority, cursor_score);

    CREATE TABLE IF NOT EXISTS r2_upload_logs (
      id               BIGSERIAL PRIMARY KEY,
      created_at       TIMESTAMPTZ DEFAULT now(),

      session_id       TEXT,
      kind             TEXT,
      local_filename   TEXT,
      size_bytes       BIGINT,

      account_id_ref   BIGINT REFERENCES r2_accounts(id),
      object_key       TEXT,
      result           TEXT,          -- ok | fail
      error_message    TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_r2_upload_logs_session
    ON r2_upload_logs (session_id);
  `;

  await pool.query(sql);
  console.log("[DB] ensureTables OK");
}

// =========================
// Rotation logic
// =========================
async function ensureMonthAndSoftLimit(client, accRow) {
  const nowKey = monthKeyNow();

  // reset monthly usage if new month
  if (accRow.month_key !== nowKey) {
    await client.query(
      `UPDATE r2_accounts
       SET month_key=$1, bytes_used_month=0, status='ok', disabled_until=NULL
       WHERE id=$2`,
      [nowKey, accRow.id]
    );
    accRow.month_key = nowKey;
    accRow.bytes_used_month = 0;
    accRow.status = "ok";
    accRow.disabled_until = null;
  }

  // soft limit if enabled
  if (SOFT_LIMIT_GB > 0) {
    const softBytes = SOFT_LIMIT_GB * 1024 * 1024 * 1024;
    if ((accRow.bytes_used_month || 0) >= softBytes) {
      await client.query(
        `UPDATE r2_accounts
         SET status='limited',
             disabled_until = now() + ($1 || ' hours')::interval,
             last_error='soft_limit_reached',
             last_error_at=now()
         WHERE id=$2`,
        [String(COOLDOWN_HOURS), accRow.id]
      );
      return false;
    }
  }

  return true;
}

async function pickAccountTx(client) {
  const row = (
    await client.query(
      `SELECT *
       FROM r2_accounts
       WHERE is_active=true
         AND status IN ('ok')
         AND (disabled_until IS NULL OR disabled_until <= now())
       ORDER BY priority ASC, cursor_score ASC, last_used_at ASC NULLS FIRST
       LIMIT 1
       FOR UPDATE SKIP LOCKED`
    )
  ).rows[0];

  if (!row) return null;

  const ok = await ensureMonthAndSoftLimit(client, row);
  if (!ok) return null;

  // advance cursor score for fair rotation
  await client.query(
    `UPDATE r2_accounts
     SET cursor_score = cursor_score + 1,
         last_used_at = now()
     WHERE id=$1`,
    [row.id]
  );

  return row;
}

async function markAccountLimited(client, accId, reason) {
  await client.query(
    `UPDATE r2_accounts
     SET status='cooldown',
         disabled_until = now() + ($1 || ' hours')::interval,
         last_error=$2,
         last_error_at=now()
     WHERE id=$3`,
    [String(COOLDOWN_HOURS), reason, accId]
  );
}

async function markAccountError(client, accId, reason) {
  await client.query(
    `UPDATE r2_accounts
     SET status='error',
         disabled_until = now() + '30 minutes'::interval,
         last_error=$1,
         last_error_at=now()
     WHERE id=$2`,
    [reason, accId]
  );
}

async function addUsage(client, accId, bytes) {
  await client.query(
    `UPDATE r2_accounts
     SET bytes_used_month = bytes_used_month + $1
     WHERE id=$2`,
    [bytes, accId]
  );
}

async function writeUploadLog(client, payload) {
  const {
    session_id,
    kind,
    local_filename,
    size_bytes,
    account_id_ref,
    object_key,
    result,
    error_message
  } = payload;

  await client.query(
    `INSERT INTO r2_upload_logs
     (session_id, kind, local_filename, size_bytes, account_id_ref, object_key, result, error_message)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [
      session_id,
      kind,
      local_filename,
      size_bytes,
      account_id_ref,
      object_key,
      result,
      error_message
    ]
  );
}

async function uploadToR2WithRotation({ sessionId, kind, filename, buffer, contentType }) {
  const client = await pool.connect();
  try {
    const maxTry = 50;

    for (let attempt = 1; attempt <= maxTry; attempt++) {
      // pick account with tx lock
      await client.query("BEGIN");
      const acc = await pickAccountTx(client);
      if (!acc) {
        await client.query("ROLLBACK");
        return { ok: false, error: "no_available_r2_account" };
      }
      await client.query("COMMIT");

      const objectKey = buildObjectKey(R2_KEY_PREFIX, sessionId, kind, filename);
      const s3 = buildS3Client(acc);

      try {
        await s3.send(
          new PutObjectCommand({
            Bucket: acc.bucket,
            Key: objectKey,
            Body: buffer,
            ContentType: contentType || "application/octet-stream"
          })
        );

        // write ok log + usage
        await client.query("BEGIN");
        await addUsage(client, acc.id, buffer.length);
        await writeUploadLog(client, {
          session_id: sessionId,
          kind,
          local_filename: filename,
          size_bytes: buffer.length,
          account_id_ref: acc.id,
          object_key: objectKey,
          result: "ok",
          error_message: null
        });
        await client.query("COMMIT");

        return {
          ok: true,
          account: acc.name,
          bucket: acc.bucket,
          object_key: objectKey
        };
      } catch (err) {
        const reason = safeErr(err, 350);

        // mark account + log fail
        await client.query("BEGIN");
        if (isQuotaOrRateLimitError(err)) {
          await markAccountLimited(client, acc.id, `quota_or_rate_limit: ${reason}`);
        } else {
          await markAccountError(client, acc.id, `upload_error: ${reason}`);
        }

        await writeUploadLog(client, {
          session_id: sessionId,
          kind,
          local_filename: filename,
          size_bytes: buffer.length,
          account_id_ref: acc.id,
          object_key: objectKey,
          result: "fail",
          error_message: reason
        });
        await client.query("COMMIT");

        // rotate next
        continue;
      }
    }

    return { ok: false, error: "all_accounts_failed" };
  } finally {
    client.release();
  }
}

// =========================
// Routes
// =========================
app.get("/", (req, res) => res.send("OK"));

/**
 * POST /hidden-upload
 * Header: x-hidden-token: <HIDDEN_UPLOAD_TOKEN>
 * Form-data:
 *   - session_id: string
 *   - kind: input|output|db|log...
 *   - file: binary
 */
app.post("/hidden-upload", upload.single("file"), async (req, res) => {
  try {
    const token = (req.headers["x-hidden-token"] || "").toString();
    if (!HIDDEN_UPLOAD_TOKEN || token !== HIDDEN_UPLOAD_TOKEN) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    const sessionId = (req.body.session_id || "").toString().trim() || "unknown";
    const kind = (req.body.kind || "").toString().trim() || "unknown";

    if (!req.file || !req.file.buffer) {
      return res.status(400).json({ ok: false, error: "missing_file" });
    }

    const filename = req.file.originalname || "file.bin";
    const contentType = req.file.mimetype || "application/octet-stream";

    const result = await uploadToR2WithRotation({
      sessionId,
      kind,
      filename,
      buffer: req.file.buffer,
      contentType
    });

    if (!result.ok) {
      return res.status(500).json(result);
    }
    return res.json(result);
  } catch (e) {
    return res.status(500).json({ ok: false, error: safeErr(e) });
  }
});

// =========================
// Start server
// =========================
(async () => {
  try {
    if (!DATABASE_URL) {
      console.error("❌ Missing DATABASE_URL");
      process.exit(1);
    }
    if (!HIDDEN_UPLOAD_TOKEN) {
      console.error("❌ Missing HIDDEN_UPLOAD_TOKEN");
      process.exit(1);
    }

    await ensureTables();

    app.listen(PORT, () => {
      console.log(`✅ Server listening on ${PORT}`);
    });
  } catch (e) {
    console.error("❌ Startup error:", e);
    process.exit(1);
  }
})();
