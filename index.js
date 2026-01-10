/**
 * index.js (FULL)
 * Railway + Postgres + Cloudflare R2 rotation uploader
 *
 * Features:
 * - Auto CREATE TABLE at startup
 * - Seed initial R2 accounts from ENV JSON (only if table empty)
 * - Auto reset accounts at beginning of month
 * - Upload endpoint (hidden) with token
 * - Admin endpoints: list accounts + list limited + force reset month
 *
 * Required ENV:
 *   - DATABASE_URL
 *   - HIDDEN_UPLOAD_TOKEN
 *
 * Optional ENV:
 *   - R2_ACCOUNTS_SEED_JSON         (seed accounts when DB empty)
 *   - R2_KEY_PREFIX                (default: seo-web)
 *   - R2_COOLDOWN_HOURS            (default: 6)
 *   - R2_MONTHLY_SOFT_LIMIT_GB     (default: 0 => off)
 *   - R2_MONTHLY_RESET_CRON_MIN    (default: 60)  // scheduler interval check
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

const R2_ACCOUNTS_SEED_JSON = process.env.R2_ACCOUNTS_SEED_JSON || "";

const R2_KEY_PREFIX = process.env.R2_KEY_PREFIX || "seo-web";
const COOLDOWN_HOURS = parseInt(process.env.R2_COOLDOWN_HOURS || "6", 10);
const SOFT_LIMIT_GB = parseFloat(process.env.R2_MONTHLY_SOFT_LIMIT_GB || "0"); // 0 = off
const RESET_CRON_MIN = parseInt(process.env.R2_MONTHLY_RESET_CRON_MIN || "60", 10);

// Multer memory upload
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

function requireToken(req, res) {
  const token = (req.headers["x-hidden-token"] || "").toString();
  if (!HIDDEN_UPLOAD_TOKEN || token !== HIDDEN_UPLOAD_TOKEN) {
    res.status(401).json({ ok: false, error: "unauthorized" });
    return false;
  }
  return true;
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

function isQuotaOrRateLimitError(err) {
  const msg = (err && err.message ? err.message : String(err || "")).toLowerCase();
  const http = err && err.$metadata && err.$metadata.httpStatusCode ? err.$metadata.httpStatusCode : 0;
  const name = err && err.name ? String(err.name) : "";

  if ([429, 403, 507].includes(http)) return true;
  if (msg.includes("rate") && msg.includes("limit")) return true;
  if (msg.includes("quota") || msg.includes("exceed")) return true;
  if (name.toLowerCase().includes("slowdown") || name.toLowerCase().includes("throttl")) return true;
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
      cursor_score       BIGINT DEFAULT 0,

      created_at         TIMESTAMPTZ DEFAULT now(),
      updated_at         TIMESTAMPTZ DEFAULT now()
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

    CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
    BEGIN
      NEW.updated_at = now();
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_trigger WHERE tgname = 'trg_r2_accounts_updated_at'
      ) THEN
        CREATE TRIGGER trg_r2_accounts_updated_at
        BEFORE UPDATE ON r2_accounts
        FOR EACH ROW
        EXECUTE FUNCTION touch_updated_at();
      END IF;
    END$$;
  `;
  await pool.query(sql);
  console.log("[DB] ensureTables OK");
}

// =========================
// Seed accounts (Cách A)
// =========================
async function seedAccountsIfEmpty() {
  if (!R2_ACCOUNTS_SEED_JSON.trim()) {
    console.log("[DB] No R2_ACCOUNTS_SEED_JSON provided, skip seeding.");
    return;
  }

  const { rows } = await pool.query("SELECT COUNT(*)::int AS c FROM r2_accounts");
  if ((rows[0] && rows[0].c) > 0) {
    console.log("[DB] r2_accounts not empty, skip seeding.");
    return;
  }

  let items;
  try {
    items = JSON.parse(R2_ACCOUNTS_SEED_JSON);
  } catch (e) {
    console.log("[DB] Seed JSON parse error:", String(e));
    return;
  }

  if (!Array.isArray(items) || items.length === 0) {
    console.log("[DB] Seed JSON empty, skip.");
    return;
  }

  const nowKey = monthKeyNow();
  for (const it of items) {
    const name = String(it.name || "").trim();
    const account_id = String(it.account_id || "").trim();
    const access_key_id = String(it.access_key_id || "").trim();
    const secret_access_key = String(it.secret_access_key || "").trim();
    const bucket = String(it.bucket || "").trim();
    const region = String(it.region || "auto").trim() || "auto";
    const priority = Number.isFinite(Number(it.priority)) ? Number(it.priority) : 100;

    if (!name || !account_id || !access_key_id || !secret_access_key || !bucket) {
      console.log("[DB] Seed item invalid, skip:", { name, account_id, bucket });
      continue;
    }

    await pool.query(
      `INSERT INTO r2_accounts
       (name, account_id, access_key_id, secret_access_key, bucket, region, priority,
        is_active, status, bytes_used_month, month_key)
       VALUES ($1,$2,$3,$4,$5,$6,$7,true,'ok',0,$8)`,
      [name, account_id, access_key_id, secret_access_key, bucket, region, priority, nowKey]
    );
  }

  console.log(`[DB] Seeded accounts from R2_ACCOUNTS_SEED_JSON.`);
}

// =========================
// Monthly reset logic
// =========================
async function resetAccountsForNewMonthIfNeeded() {
  const nowKey = monthKeyNow();

  // Reset all accounts that are not in this month key
  const r = await pool.query(
    `UPDATE r2_accounts
     SET month_key=$1,
         bytes_used_month=0,
         status='ok',
         disabled_until=NULL,
         last_error=NULL,
         last_error_at=NULL
     WHERE month_key IS NULL OR month_key <> $1`,
    [nowKey]
  );

  if (r.rowCount > 0) {
    console.log(`[MONTHLY] Reset ${r.rowCount} account(s) for new month_key=${nowKey}`);
  }
}

// scheduler: check every RESET_CRON_MIN minutes
function startMonthlyResetScheduler() {
  const everyMs = Math.max(1, RESET_CRON_MIN) * 60 * 1000;
  setInterval(async () => {
    try {
      await resetAccountsForNewMonthIfNeeded();
    } catch (e) {
      console.log("[MONTHLY] reset scheduler error:", safeErr(e, 300));
    }
  }, everyMs);

  console.log(`[MONTHLY] Reset scheduler started: every ${Math.max(1, RESET_CRON_MIN)} minute(s)`);
}

// =========================
// Rotation logic
// =========================
async function ensureMonthAndSoftLimit(client, accRow) {
  const nowKey = monthKeyNow();

  if (accRow.month_key !== nowKey) {
    await client.query(
      `UPDATE r2_accounts
       SET month_key=$1, bytes_used_month=0, status='ok', disabled_until=NULL,
           last_error=NULL, last_error_at=NULL
       WHERE id=$2`,
      [nowKey, accRow.id]
    );
    accRow.month_key = nowKey;
    accRow.bytes_used_month = 0;
    accRow.status = "ok";
    accRow.disabled_until = null;
  }

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
 * Header: x-hidden-token
 * Form-data: session_id, kind, file
 */
app.post("/hidden-upload", upload.single("file"), async (req, res) => {
  try {
    if (!requireToken(req, res)) return;

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

    if (!result.ok) return res.status(500).json(result);
    return res.json(result);
  } catch (e) {
    return res.status(500).json({ ok: false, error: safeErr(e) });
  }
});

/**
 * GET /admin/accounts
 * Header: x-hidden-token
 * Return all accounts summary (hide secrets)
 */
app.get("/admin/accounts", async (req, res) => {
  try {
    if (!requireToken(req, res)) return;

    const { rows } = await pool.query(
      `SELECT id, name, bucket, region, priority, is_active, status, disabled_until,
              bytes_used_month, month_key, last_error, last_error_at, last_used_at
       FROM r2_accounts
       ORDER BY priority ASC, id ASC`
    );

    return res.json({ ok: true, month_key: monthKeyNow(), accounts: rows });
  } catch (e) {
    return res.status(500).json({ ok: false, error: safeErr(e) });
  }
});

/**
 * GET /admin/limited
 * Header: x-hidden-token
 * Return accounts in cooldown/limited/error
 */
app.get("/admin/limited", async (req, res) => {
  try {
    if (!requireToken(req, res)) return;

    const { rows } = await pool.query(
      `SELECT id, name, bucket, status, disabled_until, bytes_used_month, month_key, last_error, last_error_at
       FROM r2_accounts
       WHERE status IN ('cooldown','limited','error')
       ORDER BY status ASC, disabled_until ASC NULLS LAST, id ASC`
    );

    return res.json({ ok: true, month_key: monthKeyNow(), limited: rows });
  } catch (e) {
    return res.status(500).json({ ok: false, error: safeErr(e) });
  }
});

/**
 * POST /admin/reset-month
 * Header: x-hidden-token
 * Force reset all accounts usage/status for current month_key (for testing)
 */
app.post("/admin/reset-month", async (req, res) => {
  try {
    if (!requireToken(req, res)) return;

    await resetAccountsForNewMonthIfNeeded();
    return res.json({ ok: true, month_key: monthKeyNow() });
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
    await seedAccountsIfEmpty();

    // run once at startup
    await resetAccountsForNewMonthIfNeeded();
    // schedule periodic check
    startMonthlyResetScheduler();

    app.listen(PORT, () => {
      console.log(`✅ Server listening on ${PORT}`);
      console.log(`[CFG] prefix=${R2_KEY_PREFIX} cooldownHours=${COOLDOWN_HOURS} softLimitGB=${SOFT_LIMIT_GB}`);
    });
  } catch (e) {
    console.error("❌ Startup error:", e);
    process.exit(1);
  }
})();
