"use strict";

const { loadEnv } = require("./config/env-loader");
loadEnv();

const http = require("http");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const crypto = require("crypto");
const { initPool, poolStats } = require("./utils/browser-pool");
const { cleanupOldChromeBinaries } = require("./utils/tmp-cleanup");
const { getUsage, recordScan, minutesUntilNextAllowed } = require("./utils/scan-usage");

// ── Configuration ─────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.PORT || "3000", 10);
const TOOL_DIR = process.env.TOOL_DIR || "/usr/local/ind_leads/ssl-checker-tool";
const OUTPUT_DIR = process.env.OUTPUT_DIR || "/home/ind/ind_leads_inputs";
const SCAN_ROOT = process.env.SCAN_ROOT || "/home/ind";

if (!TOOL_DIR) {
  console.error("[api] ERROR: TOOL_DIR is not set");
  process.exit(1);
}

if (!OUTPUT_DIR) {
  console.error("[api] ERROR: OUTPUT_DIR is not set");
  process.exit(1);
}

console.log(`[api] Configuration:`);
console.log(`[api]   PORT: ${PORT}`);
console.log(`[api]   TOOL_DIR: ${TOOL_DIR}`);
console.log(`[api]   OUTPUT_DIR: ${OUTPUT_DIR}`);
console.log(`[api]   SCAN_ROOT: ${SCAN_ROOT}`);

const API_BATCH_DIR = path.join(OUTPUT_DIR, "api_batches");
const GLOBAL_FAILED_LOG_PATH = path.join(OUTPUT_DIR, "failed_scans.csv");
const MAX_ACTIVE_RESERVED_DOMAINS = parseInt(
  process.env.MAX_ACTIVE_RESERVED_DOMAINS || "6",
  10
);

const activeJobs = new Map();
const batchTimeWatchers = new Map();
const BATCH_TIME_WATCH_MS = parseInt(process.env.BATCH_TIME_WATCH_MS || "1000", 10);

const ALL_TOOL_KEYS = [
  'ssl',
  'sucuri',
  'pagespeed',
  'pingdom',
  'dns',
  'pagerank',
  'server',
  'intodns',
];

function normalizeSelectedTools(rawTools) {
  if (!Array.isArray(rawTools)) return [];
  const allowed = new Set(ALL_TOOL_KEYS);
  return rawTools
    .map((v) => String(v || '').trim().toLowerCase())
    .filter((v) => allowed.has(v));
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function ensureDir(dirPath) {
  try {
    fs.mkdirSync(dirPath, { recursive: true });
  } catch (_) {}
}

ensureDir(OUTPUT_DIR);
ensureDir(API_BATCH_DIR);

initPool().catch((err) => {
  console.error(`[api] browser pool init failed: ${err.message}`);
  // Keep the API/monitor online even when Chrome/Puppeteer cannot start.
  // Scanner routes may fail until browser.js/browser-pool.js are fixed, but
  // /server-stats and FileMaker monitoring remain available.
  if (String(process.env.API_EXIT_ON_BROWSER_POOL_FAIL || '0') === '1') {
    process.exit(1);
  }
});

function jsonResponse(res, statusCode, data) {
  let body;
  try {
    body = JSON.stringify(data ?? { ok: false, error: "Empty response body" });
  } catch (e) {
    body = JSON.stringify({ ok: false, error: "JSON serialization failed", detail: e.message });
    statusCode = 500;
  }

  if (res.writableEnded) return;

  try {
    res.writeHead(statusCode, {
      "Content-Type": "application/json; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers": "Content-Type",
      "Cache-Control": "no-store",
      "Content-Length": Buffer.byteLength(body),
    });
    res.end(body);
  } catch (e) {
    console.error(`[api] jsonResponse failed: ${e.message}`);
    try { res.end(body); } catch (_) {}
  }
}

function jsonError(res, statusCode, message, extra = {}) {
  return jsonResponse(res, statusCode, {
    ok: false,
    error: message,
    ...extra,
  });
}

function readFileOrNull(filePath) {
  try {
    return fs.readFileSync(filePath, "utf8");
  } catch (_) {
    return null;
  }
}

function writeTextSafe(filePath, text) {
  try {
    ensureDir(path.dirname(filePath));
    fs.writeFileSync(filePath, text, "utf8");
  } catch (_) {}
}

function appendTextSafe(filePath, text) {
  try {
    ensureDir(path.dirname(filePath));
    fs.appendFileSync(filePath, text, "utf8");
  } catch (_) {}
}

function readJsonOrNull(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (_) {
    return null;
  }
}

function writeJsonSafe(filePath, obj) {
  try {
    ensureDir(path.dirname(filePath));
    fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), "utf8");
  } catch (_) {}
}


const FAILED_LOG_HEADER = "time,domain,reason,source,batch_id,job_id";

function csvEscape(value) {
  const str = String(value == null ? "" : value);
  return /[",\r\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
}

function failedLogPathForBatch(batchId) {
  if (!isSafeBatchId(batchId)) return null;
  return path.join(OUTPUT_DIR, `failed_${batchId}.csv`);
}

function ensureFailedLogHeader(filePath) {
  try {
    ensureDir(path.dirname(filePath));
    if (!fs.existsSync(filePath) || fs.statSync(filePath).size === 0) {
      fs.writeFileSync(filePath, FAILED_LOG_HEADER + "\n", "utf8");
    }
  } catch (_) {}
}

function appendFailedScanLog(entry = {}) {
  const time = entry.time || nowIso();
  const domain = sanitizeDomain(entry.domain || "") || String(entry.domain || "").trim();
  const reason = String(entry.reason || "Unknown failure").replace(/\s+/g, " ").trim();
  const source = String(entry.source || "").trim();
  const batchId = String(entry.batchId || entry.batch_id || "").trim();
  const jobId = String(entry.jobId || entry.job_id || "").trim();

  const row = [
    time,
    domain,
    reason.slice(0, 1000),
    source,
    batchId,
    jobId,
  ].map(csvEscape).join(",") + "\n";

  ensureFailedLogHeader(GLOBAL_FAILED_LOG_PATH);
  appendTextSafe(GLOBAL_FAILED_LOG_PATH, row);

  const batchPath = failedLogPathForBatch(batchId);
  if (batchPath) {
    ensureFailedLogHeader(batchPath);
    appendTextSafe(batchPath, row);
  }
}

function stripAnsi(text) {
  return String(text || "").replace(/\x1B\[[0-?]*[ -/]*[@-~]/g, "");
}

function inferFailureReasonFromLog(filePath, fallback = "Scan failed") {
  try {
    const raw = stripAnsi(fs.readFileSync(filePath, "utf8"));
    const lines = raw
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);

    const interesting = lines
      .filter((line) => /(failed|failure|error|fatal|exception|timeout|timed out|cannot|unreachable|not reachable|denied|refused|crash|exit code|ERR_|❌|⚠️)/i.test(line))
      .filter((line) => !/^\[progress\]/i.test(line));

    const picked = interesting.length ? interesting[interesting.length - 1] : lines[lines.length - 1];
    return (picked || fallback).slice(0, 1000);
  } catch (_) {
    return fallback;
  }
}

function collectFailedLogCsv(batchId = "", options = {}) {
  // limit  – max number of data rows returned (most recent kept). Default 500.
  // since  – ISO timestamp string; rows older than this are skipped.
  const limit = Math.max(1, parseInt(options.limit || "500", 10));
  const since = options.since ? new Date(options.since) : null;

  const out  = [FAILED_LOG_HEADER];
  const seen = new Set();

  function addFile(filePath) {
    try {
      if (!filePath || !fs.existsSync(filePath)) return;
      const lines = fs.readFileSync(filePath, "utf8").split(/\r?\n/);
      for (const line of lines) {
        const t = line.trim();
        if (!t || t === FAILED_LOG_HEADER) continue;
        if (seen.has(t)) continue;

        // Filter by timestamp (first CSV column) when since= is provided
        if (since) {
          const rawTs = t.split(",")[0].replace(/^"|"$/g, "").trim();
          if (rawTs) {
            const rowDate = new Date(rawTs);
            if (!isNaN(rowDate) && rowDate < since) continue;
          }
        }

        seen.add(t);
        out.push(t);
      }
    } catch (_) {}
  }

  if (batchId) {
    addFile(failedLogPathForBatch(batchId));
  } else {
    addFile(GLOBAL_FAILED_LOG_PATH);
    try {
      if (fs.existsSync(OUTPUT_DIR)) {
        for (const entry of fs.readdirSync(OUTPUT_DIR)) {
          if (/^failed_.+\.csv$/i.test(entry) &&
              entry !== path.basename(GLOBAL_FAILED_LOG_PATH)) {
            addFile(path.join(OUTPUT_DIR, entry));
          }
        }
      }
    } catch (_) {}
  }

  // Keep only the most recent `limit` data rows so FileMaker never receives
  // a response large enough to hang Insert from URL
  const header   = out[0];
  const dataRows = out.slice(1).slice(-limit);
  return [header, ...dataRows].join("\n") + "\n";
}

// ── Trim the global failed-scan log to the last N rows ────────────────────────
function trimFailedScanLog(keepRows = 1000) {
  try {
    if (!fs.existsSync(GLOBAL_FAILED_LOG_PATH)) return { ok: true, trimmed: 0, kept: 0 };
    const lines  = fs.readFileSync(GLOBAL_FAILED_LOG_PATH, "utf8")
                     .split(/\r?\n/).filter(Boolean);
    const header = lines[0] || FAILED_LOG_HEADER;
    const rows   = lines.slice(1);
    if (rows.length <= keepRows) return { ok: true, trimmed: 0, kept: rows.length };
    const kept = rows.slice(-keepRows);
    fs.writeFileSync(GLOBAL_FAILED_LOG_PATH, [header, ...kept].join("\n") + "\n", "utf8");
    return { ok: true, trimmed: rows.length - kept.length, kept: kept.length };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

function nowIso() {
  return new Date().toISOString();
}

const API_STARTED_AT = nowIso();

function sanitizeDomain(raw) {
  let s = String(raw || "").trim().toLowerCase();

  s = s.replace(/^https?:\/\//i, "");
  s = s.replace(/^www\./i, "");

  s = s.split("/")[0];
  s = s.split("?")[0];
  s = s.split("#")[0];

  s = s.replace(/:\d+$/, "");
  s = s.replace(/\.+$/, "");

  return s;
}

function uniqueDomains(list) {
  const out = [];
  const seen = new Set();
  for (const item of list || []) {
    const d = sanitizeDomain(item);
    if (!d) continue;
    if (!seen.has(d)) {
      seen.add(d);
      out.push(d);
    }
  }
  return out;
}

function uniqueDomainTargets(list) {
  const out = [];
  const seen = new Set();
  for (const item of list || []) {
    const raw = String(item || '').trim();
    const domain = sanitizeDomain(raw);
    if (!domain || seen.has(domain)) continue;
    seen.add(domain);
    out.push({ domain, raw: raw || domain });
  }
  return out;
}

function createBatchId() {
  const n = new Date();
  const pad = (v) => String(v).padStart(2, "0");
  const ts = `${n.getFullYear()}${pad(n.getMonth() + 1)}${pad(n.getDate())}_${pad(n.getHours())}${pad(n.getMinutes())}${pad(n.getSeconds())}`;
  const rnd = crypto.randomBytes(2).toString("hex");
  return `batch_${ts}_${rnd}`;
}

function createJobId(domain) {
  const safe = domain.replace(/[^a-z0-9._-]/gi, "_").slice(0, 40);
  const rnd = crypto.randomBytes(2).toString("hex");
  return `${safe}_${Date.now()}_${rnd}`;
}

// ── Per-job file paths ────────────────────────────────────────────────────────

function progressFilePath(jobId) {
  return path.join(OUTPUT_DIR, `progress_${jobId}.txt`);
}
function logFilePath(jobId) {
  return path.join(OUTPUT_DIR, `progress_${jobId}.log`);
}
function doneFlagPath(jobId) {
  return path.join(OUTPUT_DIR, `done_${jobId}.flag`);
}
function latestPathFile(jobId) {
  return path.join(OUTPUT_DIR, `latest_path_${jobId}.txt`);
}
function domainLockPath(domain) {
  return path.join(OUTPUT_DIR, `lock_${domain}.pid`);
}
function batchMetaPath(batchId) {
  return path.join(API_BATCH_DIR, `${batchId}.json`);
}

// ── Domain lock helpers ───────────────────────────────────────────────────────

function releaseDomainLock(domain, pid) {
  const lockFile = domainLockPath(domain);
  try {
    const owner = (readFileOrNull(lockFile) || "").trim();
    if (owner === String(pid)) {
      fs.unlinkSync(lockFile);
      console.log(`[lock] Released lock for ${domain} (PID: ${pid})`);
    }
  } catch (_) {}
}

function releaseBatchDomainLocks(domains, pid) {
  for (const domain of domains || []) releaseDomainLock(domain, pid);
}

function tryAcquireDomainLock(domain, pid, forceRescan = false) {
  const lockFile = domainLockPath(domain);

  try {
    if (forceRescan && fs.existsSync(lockFile)) {
      fs.unlinkSync(lockFile);
    }
  } catch (_) {}

  try {
    fs.writeFileSync(lockFile, String(pid), { flag: "wx" });
    console.log(`[lock] Acquired lock for ${domain} (PID: ${pid})`);
    return { ok: true, reason: "new_lock" };
  } catch (e) {
    if (e.code !== "EEXIST") throw e;

    try {
      const ownerPid = parseInt(readFileOrNull(lockFile) || "0", 10);

      if (!Number.isFinite(ownerPid) || ownerPid <= 0) {
        try { fs.unlinkSync(lockFile); } catch (_) {}
        fs.writeFileSync(lockFile, String(pid), { flag: "wx" });
        return { ok: true, reason: "recovered_invalid_lock" };
      }

      try {
        process.kill(ownerPid, 0);
        return { ok: false, reason: "active_lock", existingPid: ownerPid };
      } catch (_) {
        try { fs.unlinkSync(lockFile); } catch (_) {}
        fs.writeFileSync(lockFile, String(pid), { flag: "wx" });
        return { ok: true, reason: "recovered_stale_lock", previousPid: ownerPid };
      }
    } catch (_) {
      return { ok: false, reason: "lock_read_failed" };
    }
  }
}

function acquireBatchDomainLocks(domains, pid) {
  const acquired = [];
  const conflicts = [];

  for (const item of domains) {
    const domain = typeof item === "string" ? item : item.domain;
    const forceRescan = !!(item && typeof item === "object" && item.forceRescan);

    try {
      const result = tryAcquireDomainLock(domain, pid, forceRescan);
      if (result.ok) {
        acquired.push(domain);
      } else {
        conflicts.push(domain);
      }
    } catch (_) {
      conflicts.push(domain);
    }
  }

  if (conflicts.length) {
    for (const domain of acquired) {
      releaseDomainLock(domain, pid);
    }
  }

  return { acquired, conflicts };
}

function currentReservedDomainCount() {
  let count = 0;
  for (const job of activeJobs.values()) {
    if (job.type === "single" && job.domain) count += 1;
    else if (job.type === "multi") {
      count += job.domainCount || 0;
    }
  }
  return count;
}

function findActiveJobForDomain(domain) {
  for (const job of activeJobs.values()) {
    if (job.type === "single" && job.domain === domain) {
      return {
        busy: true,
        type: "single",
        jobId: job.jobId,
        pid: job.pid,
        startedAt: job.startedAt,
      };
    }

    if (job.type === "multi" && Array.isArray(job.domains) && job.domains.includes(domain)) {
      return {
        busy: true,
        type: "multi",
        batchId: job.batchId,
        jobId: job.jobId,
        pid: job.pid,
        startedAt: job.startedAt,
      };
    }
  }

  return { busy: false };
}

// ── Progress helpers ──────────────────────────────────────────────────────────

function initJobProgressFile(jobId, domain, total) {
  writeTextSafe(
    progressFilePath(jobId),
    `completed=0\ntotal=${total}\nlast_domain=${domain}\nlast_finish=\nstatus=STARTING\njob_id=${jobId}\ndomain=${domain}`
  );
  writeTextSafe(logFilePath(jobId), "");
}

function readProgress(jobId) {
  const raw = readFileOrNull(progressFilePath(jobId));
  if (!raw) return null;
  const obj = {};
  raw.split("\n").forEach((line) => {
    const eq = line.indexOf("=");
    if (eq > -1) obj[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  });
  return obj;
}

// ── Batch meta helpers ────────────────────────────────────────────────────────

function getBatchMeta(batchId) {
  return readJsonOrNull(batchMetaPath(batchId));
}
function saveBatchMeta(meta) {
  writeJsonSafe(batchMetaPath(meta.batchId), meta);
}

function saveBatchMetaStrict(meta) {
  ensureDir(API_BATCH_DIR);
  fs.writeFileSync(batchMetaPath(meta.batchId), JSON.stringify(meta, null, 2), "utf8");
}

function isSafeBatchId(batchId) {
  return /^[A-Za-z0-9_-]+$/.test(String(batchId || ""));
}

function queuedDomainListPath(batchId) {
  if (!isSafeBatchId(batchId)) {
    throw new Error("Unsafe batch_id");
  }
  return path.join(API_BATCH_DIR, `${batchId}_domains.txt`);
}

function countDomainsInFile(filePath) {
  // Count unique normalized domains, not raw lines. This prevents FileMaker retries
  // or repeated /multi-enqueue calls from inflating totals in /jobs and the monitor.
  return readDomainsFromFile(filePath).length;
}

function readDomainsFromFile(filePath) {
  try {
    if (!filePath || !fs.existsSync(filePath)) return [];
    return uniqueDomains(
      fs.readFileSync(filePath, "utf8")
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line && !line.startsWith("#"))
    );
  } catch (_) {
    return [];
  }
}

function deleteFileSafe(filePath) {
  if (!filePath) return { deleted: false, reason: "empty path" };
  try {
    if (!fs.existsSync(filePath)) {
      return { deleted: false, reason: "file not found", file: filePath };
    }
    fs.unlinkSync(filePath);
    return { deleted: true, file: filePath };
  } catch (e) {
    return { deleted: false, file: filePath, error: e.message };
  }
}

function cleanupBatchDomainListFile(batchId, savedMeta = null, job = null) {
  try {
    const meta = savedMeta || getBatchMeta(batchId) || {};
    const filePath = (job && job.domainListFile) || meta.domainListFile || null;

    // Snapshot the list before deleting the temp .txt file.
    const domainsSnapshot = domainsForBatch(batchId, job, meta);
    if (domainsSnapshot.length) {
      meta.domains = domainsSnapshot;
      meta.domainCount = domainsSnapshot.length;
      meta.totalQueued = meta.totalQueued || domainsSnapshot.length;
    }

    const result = deleteFileSafe(filePath);
    meta.domainListFileOriginal = filePath;
    meta.domainListFileDeleted = result.deleted ? 1 : 0;
    meta.domainListFileDeletedAt = nowIso();
    if (result.error) meta.domainListFileDeleteError = result.error;
    if (result.reason) meta.domainListFileDeleteReason = result.reason;
    saveBatchMeta(meta);

    if (result.deleted) {
      console.log(`[cleanup] deleted domain list for batch=${batchId}: ${filePath}`);
    } else {
      console.log(`[cleanup] domain list not deleted for batch=${batchId}: ${result.reason || result.error || "unknown"}`);
    }

    return result;
  } catch (e) {
    console.error(`[cleanup] domain-list cleanup failed batch=${batchId}: ${e.stack || e.message}`);
    return { deleted: false, error: e.message };
  }
}

function splitProgressList(value) {
  return String(value || "")
    .split(",")
    .map((v) => sanitizeDomain(v))
    .filter(Boolean);
}

function domainsForBatch(batchId, job = null, meta = null) {
  const paths = [];
  if (job && job.domainListFile) paths.push(job.domainListFile);
  if (meta && meta.domainListFile) paths.push(meta.domainListFile);
  if (batchId && isSafeBatchId(batchId)) paths.push(queuedDomainListPath(batchId));

  // While the batch is running, prefer the temp file because /multi-enqueue
  // and multi-audit.js can live-tail domains added to the file.
  for (const p of paths) {
    const domains = readDomainsFromFile(p);
    if (domains.length) return domains;
  }

  // After the batch finishes, cleanupBatchDomainListFile() deletes the temp
  // *_domains.txt and stores this snapshot in meta.domains.
  if (job && Array.isArray(job.domains) && job.domains.length) {
    return uniqueDomains(job.domains);
  }

  if (meta && Array.isArray(meta.domains) && meta.domains.length) {
    return uniqueDomains(meta.domains);
  }

  return [];
}


function batchChildJobId(batchId, domain) {
  return `${batchId}_${String(domain || '').replace(/[^a-z0-9._-]/gi, '_')}`;
}

function batchRootFromLatestPathText(latestPathText) {
  const latestPath = String(latestPathText || '').trim();
  if (!latestPath) return null;

  // latest_path_<job>.txt normally points to:
  //   <batchRoot>/<domain>/summary.csv
  // or sometimes:
  //   <batchRoot>/<domain>/<domain>_results.csv
  // In both cases dirname(dirname(latestPath)) is the batch root.
  const root = path.dirname(path.dirname(latestPath));
  return root && fs.existsSync(root) ? root : null;
}

function resolveBatchRootForMultiResult(batchId, meta = null) {
  // 0) FIX: api-server now stores scanBatchPath/batchRoot in meta at spawn time.
  //    Check this first — it's reliable and avoids the entire latest_path traversal
  //    when the batch root is already known.
  const metaRootDirect = meta && (meta.scanBatchPath || meta.batchRoot || meta.batchPath);
  if (metaRootDirect && fs.existsSync(metaRootDirect)) return metaRootDirect;

  // 1) Backward-compatible direct latest_path_<batchId>.txt.
  // Usually absent for multi batches, but safe if present.
  try {
    const directLatest = readFileOrNull(latestPathFile(batchId));
    const directRoot = batchRootFromLatestPathText(directLatest);
    if (directRoot) return directRoot;
  } catch (_) {}

  // 2) Correct multi-batch lookup.
  // multi-audit.js runs each domain as child JOB_ID:
  //   <batchId>_<domain>
  // index.js writes latest_path_<childJobId>.txt for each completed domain.
  // Use only those files so /multi-result never reads the newest unrelated
  // /home/ind/<date> folder from another batch.
  const domains = domainsForBatch(batchId, null, meta);
  for (const domain of domains) {
    const childJobId = batchChildJobId(batchId, domain);
    try {
      const childLatest = readFileOrNull(latestPathFile(childJobId));
      const childRoot = batchRootFromLatestPathText(childLatest);
      if (childRoot) return childRoot;
    } catch (_) {}
  }

  return null;
}

function buildDomainStatusList(domains, progress = null, running = false, meta = null, resultDomainSet = null) {
  const completedSet = new Set(splitProgressList(progress && progress.completed_domains));
  const failedSet = new Set(splitProgressList(progress && progress.failed_domains));
  const resultSet = resultDomainSet instanceof Set ? resultDomainSet : new Set();
  const currentDomain = sanitizeDomain(progress && progress.current_domain);
  const metaStatus = String((meta && meta.status) || "").toUpperCase();

  const total = parseInt((progress && progress.total) || (meta && (meta.totalQueued || meta.domainCount)) || (domains && domains.length) || "0", 10) || 0;
  const completed = parseInt((progress && progress.completed) || "0", 10) || 0;
  const batchDone = !running &&
    (metaStatus === "DONE" || metaStatus === "FINISHED" || metaStatus === "COMPLETE" || metaStatus === "COMPLETED") &&
    (total <= 0 || completed >= total);

  return (domains || []).map((domain) => {
    const d = sanitizeDomain(domain);
    let status = "queued";

    // Result CSV is the same evidence FileMaker uses before it marks Upload = done.
    // A real/fallback result row must beat failed_domains so FileMaker can import it.
    // failed_domains only means "needs fallback upload" when no row exists yet.
    if (resultSet.has(d)) {
      status = "done";
    } else if (completedSet.has(d) || batchDone) {
      // The scanner says the domain finished, but /multi-result must not mark it
      // uploadable until a CSV/result row exists. FileMaker should keep polling.
      status = "finalizing";
    } else if (failedSet.has(d)) {
      status = "failed";
    } else if (running && currentDomain && d === currentDomain) {
      status = "processing";
    }

    return { domain: d, status };
  });
}

function formatProcessDuration(ms) {
  const totalSeconds = Math.max(0, Math.floor((parseInt(ms, 10) || 0) / 1000));
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return `${minutes}m ${seconds}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

function withDomainProcessTimes(batchId, domainStatuses, running = false, meta = null) {
  if (!Array.isArray(domainStatuses)) return [];
  if (!batchId) return domainStatuses;

  const saved = meta || getBatchMeta(batchId) || null;
  if (!saved) return domainStatuses;

  if (!saved.domainTimes || typeof saved.domainTimes !== "object" || Array.isArray(saved.domainTimes)) {
    saved.domainTimes = {};
  }

  const nowMs = Date.now();
  const nowIsoText = new Date(nowMs).toISOString();
  let changed = false;

  for (const item of domainStatuses) {
    const d = sanitizeDomain(item && item.domain);
    if (!d) continue;

    const status = String(item.status || "queued").toLowerCase();
    const rec = saved.domainTimes[d] || {};

    if (status === "processing" || status === "scanning" || status === "running" || status === "checking") {
      if (!rec.startedAt) {
        rec.startedAt = nowIsoText;
        changed = true;
      }
      if (rec.finishedAt) {
        delete rec.finishedAt;
        changed = true;
      }
      rec.lastSeenAt = nowIsoText;
    } else if ((status === "done" || status === "failed") && rec.startedAt && !rec.finishedAt) {
      rec.finishedAt = nowIsoText;
      changed = true;
    }

    if (rec.lastSeenAt && status !== "queued") {
      item.last_seen_at = rec.lastSeenAt;
    }

    const startMs = Date.parse(rec.startedAt || "");
    const finishMs = (status === "processing" || status === "scanning" || status === "running" || status === "checking")
      ? nowMs
      : Date.parse(rec.finishedAt || "");

    item.started_at = rec.startedAt || null;
    item.finished_at = rec.finishedAt || null;

    if (!Number.isNaN(startMs) && !Number.isNaN(finishMs) && ((status === "processing" || status === "scanning" || status === "running" || status === "checking") || rec.finishedAt)) {
      item.process_time_ms = Math.max(0, finishMs - startMs);
      item.process_time = formatProcessDuration(item.process_time_ms);
    } else {
      item.process_time_ms = null;
      item.process_time = null;
    }

    saved.domainTimes[d] = rec;
  }

  if (changed && saved.batchId) {
    try { saveBatchMeta(saved); } catch (_) {}
  }

  return domainStatuses;
}

function updateBatchDomainTimesFromProgress(batchId, progress = null, running = false, meta = null) {
  if (!batchId) return [];

  const saved = meta || getBatchMeta(batchId) || null;
  if (!saved) return [];

  const liveProgress = progress || readProgress(batchId) || null;
  const domains = domainsForBatch(batchId, null, saved);
  const resultSet = resultDomainsForBatch(batchId, saved);
  const statuses = buildDomainStatusList(domains, liveProgress, running, saved, resultSet);
  return withDomainProcessTimes(batchId, statuses, running, saved);
}

function startBatchTimeWatcher(batchId) {
  if (!batchId || batchTimeWatchers.has(batchId)) return;

  const tick = () => {
    const batchKey = `multi:${batchId}`;
    const running = activeJobs.has(batchKey);
    const meta = getBatchMeta(batchId) || null;
    const progress = readProgress(batchId) || null;

    try {
      updateBatchDomainTimesFromProgress(batchId, progress, running, meta);
    } catch (err) {
      console.error(`[api] domain time watcher failed batch_id=${batchId}: ${err.message}`);
    }

    const metaStatus = String((meta && meta.status) || "").toUpperCase();
    const progressStatus = String((progress && progress.status) || "").toUpperCase();
    const finished = !running && (
      metaStatus === "DONE" || metaStatus === "ERROR" || metaStatus === "FINISHED" || metaStatus === "COMPLETE" || metaStatus === "COMPLETED" ||
      progressStatus.indexOf("DONE") === 0
    );

    if (finished) {
      stopBatchTimeWatcher(batchId);
    }
  };

  const timer = setInterval(tick, Math.max(250, BATCH_TIME_WATCH_MS));
  batchTimeWatchers.set(batchId, timer);
  tick();
}

function stopBatchTimeWatcher(batchId) {
  const timer = batchTimeWatchers.get(batchId);
  if (timer) clearInterval(timer);
  batchTimeWatchers.delete(batchId);
}

function finalizeBatchTimeWatcher(batchId, meta = null) {
  try {
    updateBatchDomainTimesFromProgress(batchId, readProgress(batchId), false, meta || getBatchMeta(batchId));
  } catch (err) {
    console.error(`[api] final domain time update failed batch_id=${batchId}: ${err.message}`);
  }
  stopBatchTimeWatcher(batchId);
}


function isFinalBatchStatus(status) {
  const s = String(status || "").toUpperCase();
  return s === "DONE" || s === "ERROR" || s === "FAILED" || s === "STOPPED" ||
    s === "FINISHED" || s === "COMPLETE" || s === "COMPLETED";
}

function listVisibleQueuedBatchMetas() {
  const out = [];
  try {
    if (!fs.existsSync(API_BATCH_DIR)) return out;

    const files = fs.readdirSync(API_BATCH_DIR)
      .filter((name) => name.endsWith(".json"))
      .map((name) => path.join(API_BATCH_DIR, name));

    for (const file of files) {
      const meta = readJsonOrNull(file);
      if (!meta || !meta.batchId || !isSafeBatchId(meta.batchId)) continue;
      if (activeJobs.has(`multi:${meta.batchId}`)) continue;

      const status = String(meta.status || "QUEUED").toUpperCase();

      // Final orphaned metadata must not appear in /jobs.
      // A PM2/API restart can leave old DONE/ERROR metadata with stale queued domain_statuses.
      // The monitor is for active work only, so hide terminal batches here.
      if (isFinalBatchStatus(status)) continue;

      const progress = readProgress(meta.batchId);
      const domains = domainsForBatch(meta.batchId, null, meta);
      const progressTotal = parseInt((progress && progress.total) || meta.totalQueued || meta.domainCount || domains.length || "0", 10) || 0;
      const progressCompleted = parseInt((progress && progress.completed) || "0", 10) || 0;
      const resultCount = resultDomainsForBatch(meta.batchId, meta).size;
      const trueCompleted = Math.max(progressCompleted, resultCount);
      const allDomainsAlreadyProcessed = progressTotal > 0 && trueCompleted >= progressTotal;
      const trulyFinal = isFinalBatchStatus(status) && (progressTotal <= 0 || trueCompleted >= progressTotal);

      // If the worker finished and activeJobs was removed but meta/progress is a little late,
      // do not keep showing a stale completed batch in /jobs or the monitors.
      if (trulyFinal || allDomainsAlreadyProcessed) continue;

      // Startup cleanup removes old non-final batches after PM2/API restart.
      // While this API process is alive, do not hide a batch just because activeJobs
      // is briefly empty between domain hand-offs.
      const monitorVisibleStatuses = {
        QUEUED: true,
        WAITING_GAP: true,
        STARTING: true,
        PENDING: true,
        RUNNING: true,
        PROCESSING: true,
        SCANNING: true,
        ACTIVE: true,
        STARTED: true
      };
      if (!monitorVisibleStatuses[status] && !progress) {
        continue;
      }

      if (!domains.length && !(meta.totalQueued || meta.domainCount)) continue;

      out.push({ meta, domains, file });
    }
  } catch (err) {
    console.error(`[api] listVisibleQueuedBatchMetas failed: ${err.message}`);
  }

  out.sort((a, b) => {
    const at = Date.parse(a.meta.enqueuedAt || a.meta.lastChunkAt || a.meta.createdAt || "") || 0;
    const bt = Date.parse(b.meta.enqueuedAt || b.meta.lastChunkAt || b.meta.createdAt || "") || 0;
    return at - bt;
  });

  return out;
}


function clearVisibleQueuedBatchFiles(reason = "startup", options = {}) {
  const includeFinal = !!(options && options.includeFinal);
  const cleared = [];
  const skipped = [];
  const errors = [];

  try {
    if (!fs.existsSync(API_BATCH_DIR)) {
      return { ok: true, reason, cleared, skipped, errors };
    }

    const files = fs.readdirSync(API_BATCH_DIR)
      .filter((name) => name.endsWith(".json"))
      .map((name) => path.join(API_BATCH_DIR, name));

    for (const file of files) {
      let meta = null;
      try {
        meta = readJsonOrNull(file);
      } catch (e) {
        errors.push({ file, error: e.message });
        continue;
      }

      if (!meta || !meta.batchId || !isSafeBatchId(meta.batchId)) {
        skipped.push({ file, reason: "invalid metadata" });
        continue;
      }

      const status = String(meta.status || "QUEUED").toUpperCase();

      if (activeJobs.has(`multi:${meta.batchId}`)) {
        skipped.push({ batch_id: meta.batchId, status, reason: "active in current API process" });
        continue;
      }

      // Clear non-final orphaned batches by default. If include_final=1 is passed,
      // also clear final DONE/ERROR/FAILED metadata from the monitor queue only.
      // This does not delete the real scan result folders under SCAN_ROOT.
      if (isFinalBatchStatus(status) && !includeFinal) {
        skipped.push({ batch_id: meta.batchId, status, reason: "final batch metadata kept" });
        continue;
      }

      const deletedFiles = [];
      const candidates = [
        file,
        meta.domainListFile || null,
        queuedDomainListPath(meta.batchId),
        progressFilePath(meta.batchId),
        logFilePath(meta.batchId),
        doneFlagPath(meta.batchId),
      ].filter(Boolean);

      for (const candidate of Array.from(new Set(candidates))) {
        try {
          if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) {
            fs.unlinkSync(candidate);
            deletedFiles.push(candidate);
          }
        } catch (e) {
          errors.push({ batch_id: meta.batchId, file: candidate, error: e.message });
        }
      }

      activeJobs.delete(`multi:${meta.batchId}`);
      stopBatchTimeWatcher(meta.batchId);
      cleared.push({ batch_id: meta.batchId, status, deleted_files: deletedFiles });
    }
  } catch (err) {
    errors.push({ error: err.message });
  }

  return { ok: errors.length === 0, reason, cleared, skipped, errors };
}

function progressForBatchStatus(meta, progress, domains, running = false) {
  const status = String((meta && meta.status) || "").toUpperCase();
  const total = domains.length || parseInt((meta && (meta.totalQueued || meta.domainCount)) || "0", 10) || 0;

  if (progress && progress.status) return progress;

  const serverName = (meta && meta.serverName) ? String(meta.serverName) : "server";

  if (status === "WAITING_GAP") {
    return {
      ...(progress || {}),
      completed: 0,
      total,
      status: `Queued in ${serverName}`,
      current_domain: "",
      waiting_gap: 1,
      minutes_remaining: meta.minutesRemaining || 0,
      scheduled_start_at: meta.scheduledStartAt || null,
    };
  }

  if (status === "QUEUED" || status === "PENDING") {
    return {
      ...(progress || {}),
      completed: 0,
      total,
      status: `Queued in ${serverName}`,
      current_domain: "",
    };
  }

  if (status === "STARTING" || (running && !progress)) {
    return {
      ...(progress || {}),
      completed: 0,
      total,
      status: `Starting in ${serverName}`,
      current_domain: "",
    };
  }

  return progress || { completed: 0, total, status: "queued", current_domain: "" };
}

// ── CSV parser ────────────────────────────────────────────────────────────────

function parseCSV(raw) {
  const lines = raw
    .split("\n")
    .map((l) => l.replace(/\r/g, ""))
    .filter((l) => l.trim());

  if (lines.length < 2) return [];

  function parseLine(line) {
    const cols = [];
    let cur = "";
    let inQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (inQuotes) {
        if (ch === '"') {
          if (line[i + 1] === '"') {
            cur += '"';
            i++;
          } else {
            inQuotes = false;
          }
        } else {
          cur += ch;
        }
      } else {
        if (ch === '"') {
          inQuotes = true;
        } else if (ch === ",") {
          cols.push(cur);
          cur = "";
        } else {
          cur += ch;
        }
      }
    }

    cols.push(cur);
    return cols;
  }

  const headers = parseLine(lines[0]);
  return lines.slice(1).map((line) => {
    const cols = parseLine(line);
    const obj = {};
    headers.forEach((h, i) => {
      obj[h] = cols[i] !== undefined ? cols[i] : "";
    });
    return obj;
  });
}

// ── Result lookup helpers ─────────────────────────────────────────────────────

function findLatestDomainCSV(domain) {
  let bestMtime = 0;
  let bestPath = null;

  console.log(`[api] Looking for direct result files for domain: ${domain}`);

  try {
    const entries = fs.readdirSync(SCAN_ROOT);
    for (const entry of entries) {
      if (!/^\d{4}-\d{2}-\d{2}/.test(entry)) continue;

      const batchPath = path.join(SCAN_ROOT, entry);
      const candidates = [
        path.join(batchPath, domain, "summary.csv"),
        path.join(batchPath, domain, `${domain}_results.csv`),
        path.join(batchPath, `${domain}_results.csv`),
      ];

      for (const candidate of candidates) {
        if (!fs.existsSync(candidate)) continue;
        const mtime = fs.statSync(candidate).mtimeMs;
        console.log(`[api] Found candidate: ${candidate}`);
        if (mtime > bestMtime) {
          bestMtime = mtime;
          bestPath = candidate;
        }
      }
    }
  } catch (_) {}

  if (!bestPath) {
    console.log(`[api] No direct result file found for ${domain}`);
  } else {
    console.log(`[api] Best result file for ${domain}: ${bestPath}`);
  }

  return bestPath;
}

function resultFromCsvPath(csvPath) {
  if (!csvPath || !fs.existsSync(csvPath)) return null;

  const rows = parseCSV(fs.readFileSync(csvPath, "utf8"));
  if (!rows.length) return null;

  return {
    csv_path: csvPath,
    data: rows[rows.length - 1],
  };
}

function findSingleResultByJobId(jobId) {
  if (!jobId) return null;

  const latestPathText = readFileOrNull(latestPathFile(jobId));
  const csvPath = String(latestPathText || "").trim();

  // When FileMaker asks with job_id, return only the file created by that job.
  // This prevents a dead/unreachable scan from accidentally returning an older
  // successful result for the same domain.
  return resultFromCsvPath(csvPath);
}

function findSingleResult(domain) {
  return resultFromCsvPath(findLatestDomainCSV(domain));
}

function findBatchCSV(batchRoot) {
  if (!batchRoot || !fs.existsSync(batchRoot)) return null;

  const candidates = [
    path.join(batchRoot, "summary.csv"),
    path.join(batchRoot, "all_domains_summary.csv"),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }

  return null;
}


function domainFromResultRow(row = {}) {
  return sanitizeDomain(
    row.Domain ||
    row.domain ||
    row.DOMAIN ||
    row.Domain_Name ||
    row.domain_name ||
    row.URL ||
    row.url ||
    ""
  );
}

function lastRowFromCsvPath(csvPath) {
  try {
    if (!csvPath || !fs.existsSync(csvPath)) return null;
    const rows = parseCSV(fs.readFileSync(csvPath, "utf8"));
    return rows && rows.length ? rows[rows.length - 1] : null;
  } catch (_) {
    return null;
  }
}

function isBatchFinalFromMetaOrProgress(meta = null, progress = null) {
  if (isFinalBatchStatus(meta && meta.status)) return true;

  const statusText = String((progress && progress.status) || "").toUpperCase();
  if (statusText.indexOf("DONE") === 0) return true;

  const completed = parseInt((progress && progress.completed) || "0", 10) || 0;
  const total = parseInt((progress && progress.total) || (meta && (meta.totalQueued || meta.domainCount)) || "0", 10) || 0;
  return total > 0 && completed >= total;
}

function collectRowsForBatch(batchId, batchRoot, meta = null, progress = null) {
  const rowsByDomain = new Map();
  const extraRows = [];

  const addRow = (row) => {
    if (!row || typeof row !== "object") return;
    const d = domainFromResultRow(row);
    if (!d) {
      extraRows.push(row);
      return;
    }
    rowsByDomain.set(d, row);
  };

  const batchCsvPath = findBatchCSV(batchRoot);
  if (batchCsvPath && fs.existsSync(batchCsvPath)) {
    try {
      const rows = parseCSV(fs.readFileSync(batchCsvPath, "utf8"));
      for (const row of rows || []) addRow(row);
    } catch (err) {
      console.error(`[api] could not parse batch CSV for ${batchId}: ${err.message}`);
    }
  }

  const domains = domainsForBatch(batchId, null, meta);

  // Backfill from each per-domain summary. This protects FileMaker when
  // batch summary.csv has only the reachable rows but dead/unreachable domains
  // already wrote their own summary.csv / *_results.csv file.
  if (batchRoot && fs.existsSync(batchRoot)) {
    for (const domain of domains) {
      const d = sanitizeDomain(domain);
      if (!d || rowsByDomain.has(d)) continue;

      const candidates = [
        path.join(batchRoot, d, "summary.csv"),
        path.join(batchRoot, d, `${d}_results.csv`),
      ];

      for (const candidate of candidates) {
        const row = lastRowFromCsvPath(candidate);
        if (row) {
          addRow(row);
          break;
        }
      }
    }
  }

  // Do not synthesize API-level N/A rows.
  // If a result row is missing, /multi-result returns 404/retry and FileMaker keeps polling.
  // Tool-level fallbacks belong in multi-audit.js/index.js, where successful tool
  // values can be preserved and only the missing tool fields are filled.

  const ordered = [];
  const seen = new Set();

  for (const domain of domains) {
    const d = sanitizeDomain(domain);
    if (d && rowsByDomain.has(d) && !seen.has(d)) {
      ordered.push(rowsByDomain.get(d));
      seen.add(d);
    }
  }

  for (const [d, row] of rowsByDomain.entries()) {
    if (!seen.has(d)) {
      ordered.push(row);
      seen.add(d);
    }
  }

  return [...ordered, ...extraRows];
}


function resultDomainsForBatch(batchId, meta = null) {
  const out = new Set();

  try {
    const batchRoot = resolveBatchRootForMultiResult(batchId, meta);
    const progress = readProgress(batchId);
    const rows = collectRowsForBatch(batchId, batchRoot, meta, progress);

    for (const row of rows || []) {
      const d = domainFromResultRow(row);
      if (d) out.add(d);
    }

    // Extra safety: scan per-domain CSV files even if collectRowsForBatch could not
    // resolve the batch-level CSV yet. This keeps domains-monitor and FileMaker
    // aligned when index.js writes /<batch>/<domain>/<domain>_results.csv first.
    const domains = domainsForBatch(batchId, null, meta);
    if (batchRoot && fs.existsSync(batchRoot)) {
      for (const domain of domains) {
        const d = sanitizeDomain(domain);
        if (!d || out.has(d)) continue;

        const candidates = [
          path.join(batchRoot, d, "summary.csv"),
          path.join(batchRoot, d, `${d}_results.csv`),
        ];

        for (const candidate of candidates) {
          const row = lastRowFromCsvPath(candidate);
          if (row && domainFromResultRow(row) === d) {
            out.add(d);
            break;
          }
        }
      }
    }
  } catch (err) {
    console.error(`[api] resultDomainsForBatch failed batch_id=${batchId}: ${err.message}`);
  }

  return out;
}


// ── Cleanup helpers ───────────────────────────────────────────────────────────

function cleanupOldApiHelperFiles(hours = 24) {
  const now = Date.now();
  const cutoffMs = hours * 60 * 60 * 1000;
  const cutoffTime = now - cutoffMs;

  const deleted = [];
  const skipped = [];
  const errors = [];

  try {
    if (!fs.existsSync(OUTPUT_DIR)) {
      return {
        ok: true,
        deleted,
        skipped,
        errors,
        message: "OUTPUT_DIR does not exist, nothing to clean.",
      };
    }

    const entries = fs.readdirSync(OUTPUT_DIR);

    for (const entry of entries) {
      const fullPath = path.join(OUTPUT_DIR, entry);

      let stat;
      try {
        stat = fs.statSync(fullPath);
      } catch (e) {
        errors.push({ file: entry, error: e.message });
        continue;
      }

      if (!stat.isFile()) {
        skipped.push({ file: entry, reason: "not a file" });
        continue;
      }

      const isApiHelperFile =
        /^progress_.+\.txt$/.test(entry) ||
        /^progress_.+\.log$/.test(entry) ||
        /^done_.+\.flag$/.test(entry) ||
        /^latest_path_.+\.txt$/.test(entry);

      if (!isApiHelperFile) {
        skipped.push({ file: entry, reason: "not an API helper file" });
        continue;
      }

      if (stat.mtimeMs > cutoffTime) {
        skipped.push({ file: entry, reason: "newer than cutoff" });
        continue;
      }

      try {
        fs.unlinkSync(fullPath);
        deleted.push(entry);
      } catch (e) {
        errors.push({ file: entry, error: e.message });
      }
    }

    return {
      ok: true,
      deleted,
      skipped,
      errors,
      cutoff_iso: new Date(cutoffTime).toISOString(),
    };
  } catch (e) {
    return {
      ok: false,
      deleted,
      skipped,
      errors,
      error: e.message,
    };
  }
}

function killProcess(pid) {
  try {
    process.kill(pid, "SIGTERM");
    return true;
  } catch (_) {
    return false;
  }
}

function purgeStaleJobs(maxAgeMs) {
  const now = Date.now();
  const killed = [];

  for (const [key, job] of activeJobs.entries()) {
    const started = new Date(job.startedAt).getTime();
    if (!started || now - started < maxAgeMs) continue;

    const ok = killProcess(job.pid);
    killed.push({
      key,
      pid: job.pid,
      type: job.type,
      domain: job.domain || null,
      batchId: job.batchId || null,
      startedAt: job.startedAt,
      killed: ok,
    });

    if (job.type === "single" && job.domain) {
      releaseDomainLock(job.domain, job.pid);
    } else if (job.type === "multi" && Array.isArray(job.domains)) {
      releaseBatchDomainLocks(job.domains, job.pid);
    }

    activeJobs.delete(key);
  }

  return { killed };
}

// ── Request body parser ───────────────────────────────────────────────────────

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    const maxBytes = parseInt(process.env.MAX_REQUEST_BODY_BYTES || String(50 * 1024 * 1024), 10);
    const chunks = [];
    let received = 0;
    let tooLarge = false;

    req.on("data", (chunk) => {
      received += chunk.length;
      if (received > maxBytes) {
        tooLarge = true;
        // Keep draining the request so the socket can still receive a JSON error response.
        return;
      }
      chunks.push(chunk);
    });

    req.on("end", () => {
      if (tooLarge) {
        return reject(new Error(`Request body too large. Limit=${maxBytes} bytes, received=${received} bytes`));
      }

      const body = Buffer.concat(chunks).toString("utf8").trim();
      if (!body) return resolve({});

      try {
        resolve(JSON.parse(body));
      } catch (e) {
        reject(new Error(`Invalid JSON body: ${e.message}`));
      }
    });

    req.on("error", reject);
  });
}

// ── Main handler ──────────────────────────────────────────────────────────────

async function handleRequest(req, res) {
  const method = req.method || "GET";
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

  if (method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers": "Content-Type",
    });
    return res.end();
  }

  // ── GET /health ────────────────────────────────────────────────────────────
  if (method === "GET" && url.pathname === "/health") {
    return jsonResponse(res, 200, {
      ok: true,
      status: "healthy",
      active_jobs: activeJobs.size,
      reserved_domains: currentReservedDomainCount(),
      max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
      pool_stats: poolStats(),
      now: nowIso(),
    });
  }

  // ── POST /scan-capacity ────────────────────────────────────────────────────
  //
  // Called by FileMaker "Server Scan - Check Capacity" before every scan.
  // Validates the five scan_settings fields from the Server Settings record.
  //
  // Request body:
  //   {
  //     "domain_count":  1,
  //     "mode":          "single" | "multi",
  //     "scan_settings": {
  //       "scan_enabled":      1,
  //       "max_concurrent":    2,
  //       "minutes_gap":       20,
  //       "domains_per_hour":  3,
  //       "domains_per_day":   72,
  //       "scan_notes":        ""
  //     }
  //   }
  //
  // FileMaker reads: ok, error, active_slots, usage.hour_count, usage.day_count

  if (method === "POST" && url.pathname === "/scan-capacity") {
    let parsed;
    try {
      parsed = await readJsonBody(req);
    } catch (e) {
      return jsonResponse(res, 400, { ok: false, error: e.message });
    }

    const domainCount = Math.max(1, parseInt(parsed.domain_count, 10) || 1);
    const mode = String(parsed.mode || "single");
    const ss = parsed.scan_settings || {};

    // Final rule:
    //   scan_enabled  = hard block
    //   max_concurrent = active-job safety block
    //   minutes_gap = real pacing rule
    //   domains_per_hour / domains_per_day = display only, not blockers
    const scanEnabled = parseInt(ss.scan_enabled, 10);
    const maxConcurrent = parseInt(ss.max_concurrent, 10) || 0;
    const minutesGap = parseFloat(ss.minutes_gap) || 0;
    const domainsPerHour = parseInt(ss.domains_per_hour, 10) || 0; // display only
    const domainsPerDay = parseInt(ss.domains_per_day, 10) || 0;   // display only

    const usage = getUsage();
    const activeSlots = currentReservedDomainCount();

    function blocked(msg, extra = {}) {
      return jsonResponse(res, 200, {
        ok: 0,
        error: msg,
        active_slots: activeSlots,
        usage: {
          hour_count: usage.hour_count,
          day_count: usage.day_count,
          last_scan_at: usage.last_scan_at,
          hour_window_start: usage.hour_window_start,
          day_date: usage.day_date,
        },
        display_limits: {
          domains_per_hour: domainsPerHour,
          domains_per_day: domainsPerDay,
        },
        ...extra,
      });
    }

    if (scanEnabled !== 1) {
      return blocked("Scanning is currently disabled on this server.");
    }

    const minsRemaining = minutesUntilNextAllowed(minutesGap, usage.last_scan_at);
    if (minsRemaining > 0) {
      const nextAvailableSlot = new Date(Date.now() + (minsRemaining * 60000)).toISOString();

      return blocked(
        `Please wait ${minsRemaining} more minute(s) before starting another scan. ` +
        `Minimum gap between scans: ${minutesGap} minute(s).`,
        {
          allowed_count: 0,
          minutes_remaining: minsRemaining,
          estimated_wait_minutes: minsRemaining,
          next_available_slot: nextAvailableSlot,
          minutes_gap: minutesGap,
          max_concurrent: maxConcurrent,
        }
      );
    }

    const allowedByConcurrent = maxConcurrent > 0
      ? Math.max(0, maxConcurrent - activeSlots)
      : domainCount;

    const allowedCount = Math.min(domainCount, allowedByConcurrent);

    if (allowedCount === 0) {
      return blocked(
        `Max concurrent scans reached (${activeSlots} active, limit is ${maxConcurrent}). ` +
        `Please wait for running scans to finish.`,
        {
          allowed_count: 0,
          max_concurrent: maxConcurrent,
          minutes_gap: minutesGap,
        }
      );
    }

    console.log(
      `[scan-capacity] OK — mode=${mode} requested=${domainCount} allowed=${allowedCount}` +
      (maxConcurrent > 0 ? ` concurrent=${activeSlots}/${maxConcurrent}` : "") +
      ` gap=${minutesGap}m display_hour=${usage.hour_count}/${domainsPerHour || "display"}` +
      ` display_day=${usage.day_count}/${domainsPerDay || "display"}`
    );

    return jsonResponse(res, 200, {
      ok: 1,
      allowed_count: allowedCount,
      active_slots: activeSlots,
      domain_count: domainCount,
      mode,
      max_concurrent: maxConcurrent,
      minutes_gap: minutesGap,
      usage: {
        hour_count: usage.hour_count,
        day_count: usage.day_count,
        last_scan_at: usage.last_scan_at,
        hour_window_start: usage.hour_window_start,
        day_date: usage.day_date,
      },
      display_limits: {
        domains_per_hour: domainsPerHour,
        domains_per_day: domainsPerDay,
      },
    });
  }

  // ── POST /scan ─────────────────────────────────────────────────────────────

  if (method === "POST" && url.pathname === "/scan") {
    let parsed;
    try {
      parsed = await readJsonBody(req);
    } catch (e) {
      return jsonResponse(res, 400, { ok: false, error: e.message });
    }

    
    // Keep FileMaker's normalized root domain for records, but allow an exact URL
    // target for sites where path/query matters, such as popup/age-gate pages.
    const rawTarget = String(parsed.target_url || parsed.url || parsed.domain || '').trim();
    const domain = sanitizeDomain(parsed.domain || rawTarget);
    const forceRescan = !!parsed.force_rescan;
    const selectedTools = normalizeSelectedTools(parsed.tools);
    const enabledTools = selectedTools.length ? selectedTools : ALL_TOOL_KEYS;
    const scanSettings = parsed.scan_settings || {};
    const minutesGap = parseFloat(scanSettings.minutes_gap || 0) || 0;
    const maxConcurrentSetting =
      parseInt(scanSettings.max_concurrent || process.env.MAX_CONCURRENT || "3", 10) || 3;

    const domainLaunchDelayMs =
      minutesGap > 0 ? Math.round(minutesGap * 60 * 1000) : 3000;

    const effectiveMaxConcurrent =
      minutesGap > 0 ? 1 : maxConcurrentSetting;

    if (!domain) {
      return jsonResponse(res, 400, { ok: false, error: "Missing domain" });
    }

    const jobKey = `single:${domain}`;

    let busy = findActiveJobForDomain(domain);

    if (busy.busy && busy.jobId) {
      const donePath = doneFlagPath(busy.jobId);
      if (fs.existsSync(donePath)) {
        const singleKey = `single:${domain}`;
        if (activeJobs.has(singleKey)) {
          console.log(`[api] Clearing stale active job for ${domain} because done flag exists (${busy.jobId})`);
          activeJobs.delete(singleKey);
        }
        busy = findActiveJobForDomain(domain);
      }
    }

    if (busy.busy) {
      return jsonResponse(res, 409, {
        ok: false,
        error:
          busy.type === "multi"
            ? `Domain ${domain} is already included in an active batch scan`
            : `A scan is already running for ${domain}`,
        domain,
        active_job_type: busy.type,
        job_id: busy.jobId || null,
        batch_id: busy.batchId || null,
        pid: busy.pid || null,
        started_at: busy.startedAt || null,
      });
    }

    if (currentReservedDomainCount() >= MAX_ACTIVE_RESERVED_DOMAINS) {
      return jsonResponse(res, 429, {
        ok: false,
        error: "Max active reserved domains reached. Try again later.",
        reserved_domains: currentReservedDomainCount(),
        max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
      });
    }

    const jobId = createJobId(domain);
    const startedAt = nowIso();

    initJobProgressFile(jobId, domain, 1);

    if (forceRescan && activeJobs.has(jobKey)) {
      const staleJob = activeJobs.get(jobKey);
      activeJobs.delete(jobKey);
      console.log(
        `[api] force_rescan cleared in-memory reservation for ${domain} (old job_id=${staleJob.jobId})`
      );
    }

    console.log(
      `[api] Starting single scan for ${domain} with job_id=${jobId}${forceRescan ? " (force_rescan)" : ""}`
    );

    // Always clear any stale lock file before spawning the child
    try {
      const lockFile = domainLockPath(domain);
      if (fs.existsSync(lockFile)) {
        fs.unlinkSync(lockFile);
        console.log(`[api] Cleared stale lock file for ${domain} before spawn`);
      }
    } catch (_) {}

    const child = spawn("node", ["index.js", rawTarget || domain], {
      cwd: TOOL_DIR,
      env: {
        ...process.env,
        PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH,
        JOB_ID: jobId,
        BROWSER_POOL_SKIP: "1",
        ENABLED_TOOLS: JSON.stringify(enabledTools),
        FORCE_RESCAN: forceRescan ? "1" : "0",
        TARGET_RAW_URL: rawTarget || domain,
        MINUTES_GAP: String(minutesGap),
        DOMAIN_LAUNCH_DELAY_MS: String(domainLaunchDelayMs),
        MAX_CONCURRENT: String(effectiveMaxConcurrent),
        
      },
      detached: false,
      stdio: ["ignore", "pipe", "pipe"],
    });

    const logFile = logFilePath(jobId);
    child.stdout.on("data", (chunk) => appendTextSafe(logFile, chunk.toString()));
    child.stderr.on("data", (chunk) => appendTextSafe(logFile, chunk.toString()));

    activeJobs.set(jobKey, {
      key: jobKey,
      type: "single",
      domain,
      tools: enabledTools,
      jobId,
      pid: child.pid,
      startedAt,
      logFile,
      forceRescan,
    });

    child.on("exit", (code, signal) => {
      console.log(`[api] Single scan exited for ${domain}. code=${code} signal=${signal}`);

      writeTextSafe(
        doneFlagPath(jobId),
        `done\ncode=${code}\nsignal=${signal}\nfinishedAt=${nowIso()}\ndomain=${domain}`
      );

      releaseDomainLock(domain, child.pid);

      if (code !== 0 || signal) {
        appendFailedScanLog({
          domain,
          reason: inferFailureReasonFromLog(logFile, `Single scan failed. code=${code} signal=${signal || ""}`),
          source: "single",
          jobId,
        });
      }

      activeJobs.delete(jobKey);
    });

    // Record usage against hourly/daily counters
    recordScan(1);

    return jsonResponse(res, 200, {
      ok: true,
      queued: true,
      mode: "single",
      domain,
      tools: enabledTools,
      force_rescan: forceRescan,
      job_id: jobId,
      pid: child.pid,
      started_at: startedAt,
      pool_stats: poolStats(),
    });
  }

  // ── POST /multi-scan ───────────────────────────────────────────────────────

  if (method === "POST" && url.pathname === "/multi-scan") {
    let parsed;
    try {
      parsed = await readJsonBody(req);
    } catch (e) {
      return jsonResponse(res, 400, { ok: false, error: e.message });
    }

    const domainTargets = uniqueDomainTargets(parsed.domains || []);
    const domains = domainTargets.map((item) => item.domain);
    const rawDomainTargets = domainTargets.map((item) => item.raw);
    const forceRescan = !!parsed.force_rescan;
    const selectedTools = normalizeSelectedTools(parsed.tools);
    const enabledTools = selectedTools.length ? selectedTools : ALL_TOOL_KEYS;
    
    const scanSettings = parsed.scan_settings || {};
    const minutesGap = parseFloat(scanSettings.minutes_gap || 0) || 0;
    const maxConcurrentSetting =
      parseInt(scanSettings.max_concurrent || process.env.MAX_CONCURRENT || "3", 10) || 3;

    const domainLaunchDelayMs =
      minutesGap > 0 ? Math.round(minutesGap * 60 * 1000) : 3000;

    const effectiveMaxConcurrent =
      minutesGap > 0 ? 1 : maxConcurrentSetting;

    if (!domains.length) {
      return jsonResponse(res, 400, { ok: false, error: "No domains provided" });
    }

    const batchId = createBatchId();
    const startedAt = nowIso();
    const batchKey = `multi:${batchId}`;

    // Soft check — log a warning but never block multi-scan.
    // With MINUTES_GAP the worker runs one domain at a time so memory stays flat
    // regardless of how many domains are queued in the .txt file.
    if (domains.length > 10000) {
      console.warn(`[multi-scan] Large batch: ${domains.length} domains. Make sure MINUTES_GAP is set.`);
    }

    const lockCheck = acquireBatchDomainLocks(
      domains.map((d) => ({ domain: d, forceRescan })),
      process.pid
    );

    if (lockCheck.conflicts.length) {
      return jsonResponse(res, 409, {
        ok: false,
        error: "Some domains are already locked",
        conflicts: lockCheck.conflicts,
      });
    }

    releaseBatchDomainLocks(lockCheck.acquired, process.pid);

    // ── RESPOND IMMEDIATELY ────────────────────────────────────────────────
    // Send the batch_id back to FileMaker right away — before any file I/O or
    // spawn(). This prevents FileMaker's --max-time 30 from expiring when the
    // server is under load (running many concurrent scans), which was causing
    // the "Invalid JSON response" / Error 1631 error on large domain lists.
    // All setup work (saveBatchMeta, spawn, recordScan) runs asynchronously
    // after the HTTP response is flushed via setImmediate().
    jsonResponse(res, 200, {
      ok: true,
      queued: true,
      mode: "multi",
      batch_id: batchId,
      count: domains.length,
      started_at: startedAt,
    });

    // ── ASYNC SETUP — runs after response is sent ──────────────────────────
    setImmediate(async () => {
      const meta = {
        batchId,
        startedAt,
        status: "QUEUED",
        domainCount: domains.length,
        totalQueued: domains.length,
        domains,
        tools: enabledTools,
        childPid: null,
        progressFile: progressFilePath(batchId),
        logFile: logFilePath(batchId),
      };

      try {
        saveBatchMeta(meta);
        initJobProgressFile(batchId, "", domains.length);
      } catch (err) {
        console.error(`[api] /multi-scan meta init failed batch_id=${batchId}: ${err.message}`);
      }

      console.log(`[api] Starting multi-scan batch_id=${batchId} domains=${domains.length}`);

      // For large batches, do not pass every domain as a CLI argument.
      // Save the domain list to a text file and pass only the filename to multi-audit.js.
      // This keeps /multi-scan stable for 100+ domains and keeps minutes_gap scheduling intact.
      const domainListFileName = `${batchId}_domains.txt`;
      const domainListFilePath = path.join(TOOL_DIR, domainListFileName);

      try {
        fs.writeFileSync(domainListFilePath, rawDomainTargets.join("\n") + "\n", "utf8");
        meta.domainListFile = domainListFilePath;
        saveBatchMeta(meta);
        console.log(`[api] Wrote multi-scan domain list: ${domainListFilePath}`);
      } catch (writeErr) {
        console.error(`[api] failed to write domain list batch_id=${batchId}: ${writeErr.message}`);
        try {
          meta.status = "ERROR";
          meta.error = `Failed to write domain list file: ${writeErr.message}`;
          meta.finishedAt = nowIso();
          saveBatchMeta(meta);
          writeTextSafe(
            doneFlagPath(batchId),
            `done\ncode=-1\nsignal=domain_list_write_error\nfinishedAt=${nowIso()}\nerror=${writeErr.message}`
          );
        } catch (_) {}
        return;
      }

      let child;
      try {
        child = spawn("node", ["multi-audit.js", domainListFileName], {
          cwd: TOOL_DIR,
          env: {
            ...process.env,
            PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH,
            SCAN_BATCH_ID: batchId,
            JOB_ID: batchId,
            ENABLED_TOOLS: JSON.stringify(enabledTools),
            FORCE_RESCAN: forceRescan ? "1" : "0",
            MINUTES_GAP: String(minutesGap),
            DOMAIN_LAUNCH_DELAY_MS: String(domainLaunchDelayMs),
            MAX_CONCURRENT: String(effectiveMaxConcurrent),
            DOMAIN_LIST_FILE: domainListFilePath,
          },
          detached: false,
          stdio: ["ignore", "pipe", "pipe"],
        });
      } catch (spawnErr) {
        console.error(`[api] spawn failed batch_id=${batchId}: ${spawnErr.message}`);
        try {
          meta.status = "ERROR";
          meta.error = spawnErr.message;
          meta.finishedAt = nowIso();
          saveBatchMeta(meta);
          writeTextSafe(
            doneFlagPath(batchId),
            `done\ncode=-1\nsignal=spawn_exception\nfinishedAt=${nowIso()}\nerror=${spawnErr.message}`
          );
        } catch (_) {}
        return;
      }

      meta.childPid = child.pid;
      meta.status = "RUNNING";
      try { saveBatchMeta(meta); } catch (_) {}

      const logFile = logFilePath(batchId);
      child.stdout.on("data", (chunk) => appendTextSafe(logFile, chunk.toString()));
      child.stderr.on("data", (chunk) => appendTextSafe(logFile, chunk.toString()));

      child.on("error", (err) => {
        console.error(`[api] Multi-scan spawn error batch_id=${batchId}: ${err.message}`);

        const saved = getBatchMeta(batchId) || meta;
        saved.status = "ERROR";
        saved.error = err.message;
        saved.finishedAt = nowIso();
        try { saveBatchMeta(saved); } catch (_) {}

        writeTextSafe(
          doneFlagPath(batchId),
          `done\ncode=-1\nsignal=spawn_error\nfinishedAt=${nowIso()}\nerror=${err.message}`
        );

        activeJobs.delete(batchKey);
        finalizeBatchTimeWatcher(batchId, saved);
      });

      activeJobs.set(batchKey, {
        key: batchKey,
        type: "multi",
        batchId,
        domains,
        domainCount: domains.length,
        domainListFile: domainListFilePath,
        tools: enabledTools,
        jobId: batchId,
        pid: child.pid,
        startedAt,
        logFile,
        forceRescan,
      });
      startBatchTimeWatcher(batchId);

      child.on("exit", (code, signal) => {
        console.log(`[api] Multi-scan exited batch_id=${batchId}. code=${code} signal=${signal}`);
        const jobBeforeDelete = activeJobs.get(batchKey) || null;
        activeJobs.delete(batchKey);

        const saved = getBatchMeta(batchId) || meta;
        saved.status = code === 0 ? "DONE" : "ERROR";
        saved.exitCode = code;
        saved.signal = signal;
        saved.finishedAt = nowIso();
        try { saveBatchMeta(saved); } catch (_) {}

        if (code !== 0 || signal) {
          const reason = inferFailureReasonFromLog(logFile, `Multi scan failed. code=${code} signal=${signal || ""}`);
          for (const d of domains || []) {
            appendFailedScanLog({ domain: d, reason, source: "multi", batchId, jobId: batchId });
          }
        }

        finalizeBatchTimeWatcher(batchId, saved);

        // Delete the generated *_domains.txt only after a clean batch exit.
        // If Node/multi-audit crashes, keep the file for resume/debugging.
        if (code === 0) {
          cleanupBatchDomainListFile(batchId, saved, jobBeforeDelete);
        }
      });

      try { recordScan(domains.length); } catch (_) {}
    });

    return; // response already sent above
  }

  // ── GET /progress ──────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/progress") {
    const jobId = (url.searchParams.get("job_id") || "").trim();
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");

    let progress = null;

    if (jobId) {
      progress = readProgress(jobId);
    } else if (domain) {
      const jobKey = `single:${domain}`;
      if (activeJobs.has(jobKey)) {
        progress = readProgress(activeJobs.get(jobKey).jobId);
      }
    }

    if (!progress) {
      return jsonResponse(res, 404, {
        ok: false,
        error: "No progress found",
      });
    }

    return jsonResponse(res, 200, {
      ok: true,
      progress,
    });
  }

  // ── GET /multi-progress ────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/multi-progress") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing batch_id" });
    }

    const meta = getBatchMeta(batchId);
    const progress = readProgress(batchId);

    if (!meta && !progress) {
      return jsonResponse(res, 404, { ok: false, error: "Batch not found" });
    }

    const key = `multi:${batchId}`;
    const job = activeJobs.get(key) || null;
    const running = !!job;
    const domains = domainsForBatch(batchId, job, meta);
    const effectiveProgress = progressForBatchStatus(meta, progress, domains, running);
    const resultSet = resultDomainsForBatch(batchId, meta);
    const domainStatuses = withDomainProcessTimes(
      batchId,
      buildDomainStatusList(domains, effectiveProgress, running, meta, resultSet),
      running,
      meta
    );
    const completedDomains = domainStatuses
      .filter((item) => item.status === "done")
      .map((item) => item.domain);
    const failedDomains = domainStatuses
      .filter((item) => item.status === "failed")
      .map((item) => item.domain);

    return jsonResponse(res, 200, {
      ok: true,
      batch_id: batchId,
      meta,
      progress: effectiveProgress,
      domains,
      domain_statuses: domainStatuses,
      completed_domains: completedDomains,
      failed_domains: failedDomains,
      queued_count: domainStatuses.filter((item) => item.status === "queued").length,
      processing_count: domainStatuses.filter((item) => item.status === "processing").length,
      done_count: completedDomains.length,
      failed_count: failedDomains.length,
    });
  }

  // ── GET /result ────────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/result") {
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");
    const jobId = (url.searchParams.get("job_id") || "").trim();

    if (!domain && !jobId) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "Missing domain or job_id",
      });
    }

    let resolvedDomain = domain;

    if (!resolvedDomain && jobId) {
      for (const job of activeJobs.values()) {
        if (job.type === "single" && job.jobId === jobId) {
          resolvedDomain = job.domain;
          break;
        }
      }
    }

    if (!resolvedDomain && jobId) {
      const progress = readProgress(jobId);
      if (progress && progress.domain) {
        resolvedDomain = sanitizeDomain(progress.domain);
      }
    }

    if (!resolvedDomain) {
      return jsonResponse(res, 404, {
        ok: false,
        error: "Could not resolve domain for result lookup",
      });
    }

    const found = jobId ? findSingleResultByJobId(jobId) : findSingleResult(resolvedDomain);
    if (!found) {
      // If a single scan has already exited and no result row was written,
      // return fallback N/A so FileMaker can still create a Test_Results row.
      const doneText = jobId ? readFileOrNull(doneFlagPath(jobId)) : "";
      const stillRunning = !!(jobId && Array.from(activeJobs.values()).some((j) => j.type === "single" && j.jobId === jobId));

      if (jobId && doneText && !stillRunning) {
        return jsonResponse(res, 404, {
          ok: false,
          retry: true,
          fallback: false,
          error: `Scan finished but the real result row is not ready for ${resolvedDomain}`,
          domain: resolvedDomain,
          job_id: jobId,
        });
      }

      return jsonResponse(res, 404, {
        ok: false,
        error: jobId
          ? `No result file found yet for job ${jobId} / ${resolvedDomain}`
          : `No result found yet for ${resolvedDomain}`,
      });
    }

    return jsonResponse(res, 200, {
      ok: true,
      domain: resolvedDomain,
      csv_path: found.csv_path,
      data: found.data,
    });
  }

  // ── GET /multi-result ──────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/multi-result") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    const requestedDomain = sanitizeDomain(url.searchParams.get("domain") || "");

    if (!batchId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing batch_id" });
    }

    const meta = getBatchMeta(batchId);
    if (!meta) {
      return jsonResponse(res, 404, { ok: false, error: "Batch not found" });
    }

    const batchRoot = resolveBatchRootForMultiResult(batchId, meta);
    const progress = readProgress(batchId);
    const csvPath = findBatchCSV(batchRoot);
    let rows = collectRowsForBatch(batchId, batchRoot, meta, progress);

    // PATCH: FileMaker can now fetch only the domain that just finished:
    //   /multi-result?batch_id=...&domain=example.com
    // This prevents each poll from downloading/re-importing the whole batch.
    if (requestedDomain) {
      rows = rows.filter((row) => domainFromResultRow(row) === requestedDomain);

      if (!rows.length) {
        const job = activeJobs.get(`multi:${batchId}`) || null;
        const domains = domainsForBatch(batchId, job, meta);
        const resultSet = resultDomainsForBatch(batchId, meta);
        const effectiveProgress = progressForBatchStatus(meta, progress, domains, !!job);
        const statusList = buildDomainStatusList(
          domains,
          effectiveProgress,
          !!job,
          meta,
          resultSet
        );

        const statusRow = statusList.find((item) => item.domain === requestedDomain);
        const status = String((statusRow && statusRow.status) || "").toLowerCase();

        // FileMaker scope:
        // - Precheck unreachable must not stop scanning; index.js continues all tools.
        // - If a real result row exists, return it above.
        // - If the domain is terminal but no row exists, do NOT synthesize API N/A.
        //   Return retry so FileMaker keeps polling until multi-audit/index.js writes
        //   the scanner-final row. The scanner layer owns tool-level fallbacks.
        const isTerminalFailedDomain =
          status === "failed" ||
          status === "error" ||
          status.indexOf("dead") >= 0 ||
          status.indexOf("unreachable") >= 0;

        if (isTerminalFailedDomain || isBatchFinalFromMetaOrProgress(meta, progress)) {
          return jsonResponse(res, 404, {
            ok: false,
            retry: true,
            fallback: false,
            error: `No real result row found yet for ${requestedDomain}`,
            batch_id: batchId,
            domain: requestedDomain,
            status,
          });
        }
      }
    }

    if (!rows.length) {
      return jsonResponse(res, 404, {
        ok: false,
        error: requestedDomain
          ? `No result row found yet for ${requestedDomain}`
          : "No batch result rows found yet",
        batch_id: batchId,
        domain: requestedDomain || null,
        batch_root: batchRoot,
        csv_path: csvPath,
      });
    }

    return jsonResponse(res, 200, {
      ok: true,
      batch_id: batchId,
      domain: requestedDomain || null,
      batch_root: batchRoot,
      csv_path: csvPath,
      count: rows.length,
      results: rows,
      data: rows,
      tools: meta.tools || [],
    });
  }

  // ── POST /stop ─────────────────────────────────────────────────────────────

  if (method === "POST" && url.pathname === "/stop") {
    let parsed;
    try {
      parsed = await readJsonBody(req);
    } catch (e) {
      return jsonResponse(res, 400, { ok: false, error: e.message });
    }

    const domain = sanitizeDomain(parsed.domain);
    const jobId = (parsed.job_id || "").trim();

    let keyToStop = null;
    let job = null;

    if (domain) {
      const key = `single:${domain}`;
      if (activeJobs.has(key)) {
        keyToStop = key;
        job = activeJobs.get(key);
      }
    } else if (jobId) {
      for (const [key, j] of activeJobs.entries()) {
        if (j.type === "single" && j.jobId === jobId) {
          keyToStop = key;
          job = j;
          break;
        }
      }
    }

    if (!job) {
      return jsonResponse(res, 404, { ok: false, error: "Single job not found" });
    }

    const killed = killProcess(job.pid);
    if (job.domain) releaseDomainLock(job.domain, job.pid);
    activeJobs.delete(keyToStop);

    return jsonResponse(res, 200, {
      ok: true,
      stopped: killed,
      domain: job.domain,
      job_id: job.jobId,
      pid: job.pid,
    });
  }

  // ── POST /stop-multi ───────────────────────────────────────────────────────

  if (method === "POST" && url.pathname === "/stop-multi") {
    let parsed;
    try {
      parsed = await readJsonBody(req);
    } catch (e) {
      return jsonResponse(res, 400, { ok: false, error: e.message });
    }

    const batchId = (parsed.batch_id || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing batch_id" });
    }

    const key = `multi:${batchId}`;
    if (!activeJobs.has(key)) {
      return jsonResponse(res, 404, { ok: false, error: "Batch job not found" });
    }

    const job = activeJobs.get(key);
    const killed = killProcess(job.pid);
    releaseBatchDomainLocks(job.domains, job.pid);
    activeJobs.delete(key);

    const meta = getBatchMeta(batchId);
    if (meta) {
      meta.status = "STOPPED";
      meta.finishedAt = nowIso();
      saveBatchMeta(meta);
    }

    return jsonResponse(res, 200, {
      ok: true,
      stopped: killed,
      batch_id: batchId,
      pid: job.pid,
      domains: job.domains,
    });
  }

  // ── GET /status ────────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/status") {
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");
    const jobId = (url.searchParams.get("job_id") || "").trim();

    let job = null;
    if (domain) {
      job = activeJobs.get(`single:${domain}`) || null;
    } else if (jobId) {
      for (const j of activeJobs.values()) {
        if (j.type === "single" && j.jobId === jobId) {
          job = j;
          break;
        }
      }
    }

    if (!job) {
      return jsonResponse(res, 200, {
        ok: true,
        running: false,
      });
    }

    return jsonResponse(res, 200, {
      ok: true,
      running: true,
      domain: job.domain,
      job_id: job.jobId,
      pid: job.pid,
      started_at: job.startedAt,
      tools: job.tools || [],
    });
  }

  // ── GET /multi-status ──────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/multi-status") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing batch_id" });
    }

    const key = `multi:${batchId}`;
    const job = activeJobs.get(key) || null;
    const meta = getBatchMeta(batchId) || null;

    const progress = readProgress(batchId);
    const domains = domainsForBatch(batchId, job, meta);
    const resultSet = resultDomainsForBatch(batchId, meta);
    const domainStatuses = withDomainProcessTimes(batchId, buildDomainStatusList(domains, progress, !!job, meta, resultSet), !!job, meta);

    return jsonResponse(res, 200, {
      ok: true,
      running: !!job,
      batch_id: batchId,
      job: job
        ? {
            pid: job.pid,
            started_at: job.startedAt,
            domains,
            domain_statuses: domainStatuses,
            tools: job.tools || [],
          }
        : null,
      meta,
      domains,
      domain_statuses: domainStatuses,
    });
  }

  // ── GET /multi-log ─────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/multi-log") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing batch_id" });
    }

    const log = readFileOrNull(logFilePath(batchId)) || "(no log yet)";
    res.writeHead(200, {
      "Content-Type": "text/plain; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    });
    return res.end(log);
  }

  // ── GET /done ──────────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/done") {
    const jobId = (url.searchParams.get("job_id") || "").trim();
    if (!jobId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing job_id" });
    }
    const flagPath = doneFlagPath(jobId);
    const isDone = fs.existsSync(flagPath);
    return jsonResponse(res, 200, { ok: true, done: isDone });
  }

  // ── GET /log ───────────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/log") {
    const jobId = (url.searchParams.get("job_id") || "").trim();
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");

    let logContent = null;

    if (jobId) {
      logContent = readFileOrNull(logFilePath(jobId));
    } else if (domain) {
      const jobKey = `single:${domain}`;
      if (activeJobs.has(jobKey)) {
        logContent = readFileOrNull(activeJobs.get(jobKey).logFile);
      }
    }

    const log = logContent || "(no log yet)";
    res.writeHead(200, {
      "Content-Type": "text/plain; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    });
    return res.end(log);
  }

  // ── GET /tester ────────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/tester") {
    try {
      const html = fs.readFileSync(path.join(TOOL_DIR, "api-tester.html"), "utf8");
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
      });
      return res.end(html);
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: "Could not load api-tester.html: " + e.message });
    }
  }

  if (method === "GET" && url.pathname === "/tester-multi") {
    try {
      const html = fs.readFileSync(path.join(TOOL_DIR, "api-tester-multi.html"), "utf8");
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
      });
      return res.end(html);
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: "Could not load api-tester-multi.html: " + e.message });
    }
  }

  if (method === "GET" && url.pathname === "/jobs-monitor") {
    try {
      const html = fs.readFileSync(path.join(TOOL_DIR, "jobs-monitor.html"), "utf8");
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
      });
      return res.end(html);
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: "Could not load jobs-monitor.html: " + e.message });
    }
  }


  if (method === "GET" && url.pathname === "/domains-monitor") {
    try {
      const html = fs.readFileSync(path.join(TOOL_DIR, "domains-monitor.html"), "utf8");
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
      });
      return res.end(html);
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: "Could not load domains-monitor.html: " + e.message });
    }
  }

  // ── GET /server-stats ────────────────────────────────────────────────────────
  if (method === "GET" && url.pathname === "/server-stats") {
    const { execSync } = require("child_process");
    const os = require("os");

    function fmtBytes(b) {
      b = Number(b) || 0;
      if (b <= 0) return "0 B";
      const units = ["B", "KB", "MB", "GB", "TB"];
      const i = Math.min(units.length - 1, Math.floor(Math.log(b) / Math.log(1024)));
      return (b / Math.pow(1024, i)).toFixed(1) + " " + units[i];
    }

    function fmtDuration(seconds) {
      seconds = Math.max(0, parseInt(seconds || "0", 10) || 0);
      const d = Math.floor(seconds / 86400);
      const h = Math.floor((seconds % 86400) / 3600);
      const m = Math.floor((seconds % 3600) / 60);
      if (d > 0) return `${d}d ${h}h ${m}m`;
      if (h > 0) return `${h}h ${m}m`;
      return `${m}m`;
    }

    function shellQuote(value) {
      return "'" + String(value || "").replace(/'/g, "'\\''") + "'";
    }

    function safeExec(cmd, options = {}) {
      try {
        return execSync(cmd, {
          timeout: options.timeout || 5000,
          maxBuffer: options.maxBuffer || 1024 * 1024,
          encoding: "utf8",
          shell: "/bin/bash",
        });
      } catch (_) {
        return "";
      }
    }

    function duDir(dir) {
      try {
        const out = safeExec(`du -sb ${shellQuote(dir)} 2>/dev/null || echo "0\\t${String(dir).replace(/\"/g, '')}"`, { timeout: 10000 })
          .toString().trim();
        const bytes = parseInt(out.split("\t")[0], 10) || 0;
        return { path: dir, bytes, human: fmtBytes(bytes) };
      } catch (e) {
        return { path: dir, bytes: 0, human: "error", error: e.message };
      }
    }

    function dfDisk(dir) {
      try {
        const out = safeExec(`df -B1 --output=size,used,avail ${shellQuote(dir)} 2>/dev/null | tail -1`, { timeout: 10000 })
          .toString().trim().split(/\s+/);
        const total = parseInt(out[0], 10) || 0;
        const used  = parseInt(out[1], 10) || 0;
        const avail = parseInt(out[2], 10) || 0;
        return {
          total, used, available: avail,
          totalHuman:     fmtBytes(total),
          usedHuman:      fmtBytes(used),
          availableHuman: fmtBytes(avail),
          usedPct: total > 0 ? Math.round((used / total) * 100) : 0,
        };
      } catch (e) { return { error: e.message }; }
    }

    function readMemInfo() {
      const result = {};
      try {
        const raw = fs.readFileSync("/proc/meminfo", "utf8");
        raw.split(/\r?\n/).forEach((line) => {
          const m = line.match(/^([A-Za-z_()]+):\s+(\d+)\s+kB/i);
          if (m) result[m[1]] = parseInt(m[2], 10) * 1024;
        });
      } catch (_) {}
      return result;
    }

    function systemMemoryStats() {
      const mi = readMemInfo();
      const total = mi.MemTotal || os.totalmem() || 0;
      const free = mi.MemFree || os.freemem() || 0;
      const available = mi.MemAvailable || free;
      const used = Math.max(0, total - available);
      const swapTotal = mi.SwapTotal || 0;
      const swapFree = mi.SwapFree || 0;
      const swapUsed = Math.max(0, swapTotal - swapFree);
      return {
        total,
        free,
        available,
        used,
        totalHuman: fmtBytes(total),
        freeHuman: fmtBytes(free),
        availableHuman: fmtBytes(available),
        usedHuman: fmtBytes(used),
        usedPct: total > 0 ? Math.round((used / total) * 100) : 0,
        swap: {
          total: swapTotal,
          free: swapFree,
          used: swapUsed,
          totalHuman: fmtBytes(swapTotal),
          freeHuman: fmtBytes(swapFree),
          usedHuman: fmtBytes(swapUsed),
          usedPct: swapTotal > 0 ? Math.round((swapUsed / swapTotal) * 100) : 0,
        },
      };
    }

    function readCpuTimes() {
      try {
        const firstLine = fs.readFileSync("/proc/stat", "utf8").split(/\r?\n/)[0] || "";
        const parts = firstLine.trim().split(/\s+/).slice(1).map((v) => parseInt(v, 10) || 0);
        if (parts.length < 4) return null;
        const idle = (parts[3] || 0) + (parts[4] || 0); // idle + iowait
        const total = parts.reduce((sum, n) => sum + n, 0);
        return { idle, total };
      } catch (_) {
        return null;
      }
    }

    function cpuUsageSnapshot(sampleMs) {
      sampleMs = Math.max(100, Math.min(parseInt(sampleMs || "250", 10) || 250, 1000));
      const before = readCpuTimes();
      if (!before) {
        return { usedPct: 0, idlePct: 0, sampleMs, error: "Could not read /proc/stat" };
      }

      // Short local sample. This gives real CPU usage for the dashboard, not only load average.
      safeExec(`sleep ${Math.max(0.1, sampleMs / 1000).toFixed(3)}`, { timeout: sampleMs + 1000, maxBuffer: 8 * 1024 });

      const after = readCpuTimes();
      if (!after) {
        return { usedPct: 0, idlePct: 0, sampleMs, error: "Could not read /proc/stat after sample" };
      }

      const totalDelta = after.total - before.total;
      const idleDelta = after.idle - before.idle;
      const usedPct = totalDelta > 0 ? Math.max(0, Math.min(100, Math.round(((totalDelta - idleDelta) / totalDelta) * 100))) : 0;
      return {
        usedPct,
        idlePct: Math.max(0, Math.min(100, 100 - usedPct)),
        sampleMs,
      };
    }

    function parsePsLine(line) {
      const m = String(line || "").match(/^\s*(\d+)\s+(\d+)\s+(\S+)\s+([0-9.]+)\s+([0-9.]+)\s+(\d+)\s+(\d+)\s+(\S+)\s+(.*)$/);
      if (!m) return null;
      const rssBytes = (parseInt(m[6], 10) || 0) * 1024;
      const etimes = parseInt(m[7], 10) || 0;
      return {
        pid: parseInt(m[1], 10) || 0,
        ppid: parseInt(m[2], 10) || 0,
        stat: m[3],
        cpuPct: parseFloat(m[4]) || 0,
        memPct: parseFloat(m[5]) || 0,
        rssBytes,
        rssHuman: fmtBytes(rssBytes),
        elapsedSeconds: etimes,
        elapsedHuman: fmtDuration(etimes),
        command: m[8],
        args: String(m[9] || "").slice(0, 240),
      };
    }

    function psTop(sortBy, limit) {
      limit = Math.max(1, Math.min(parseInt(limit || "6", 10) || 6, 20));
      const sort = sortBy === "cpu" ? "-pcpu" : "-rss";
      const out = safeExec(`ps -eo pid,ppid,stat,pcpu,pmem,rss,etimes,comm,args --sort=${sort} --no-headers 2>/dev/null | head -n ${limit}`, {
        timeout: 5000,
        maxBuffer: 1024 * 1024,
      });
      return out.split(/\r?\n/).map(parsePsLine).filter(Boolean).slice(0, limit);
    }

    function suspectProcesses(limit) {
      limit = Math.max(1, Math.min(parseInt(limit || "8", 10) || 8, 20));
      const stuckSeconds = parseInt(process.env.SERVER_STATS_STUCK_SECONDS || "1800", 10) || 1800;
      const out = safeExec("ps -eo pid,ppid,stat,pcpu,pmem,rss,etimes,comm,args --no-headers 2>/dev/null", {
        timeout: 5000,
        maxBuffer: 3 * 1024 * 1024,
      });
      const rows = out.split(/\r?\n/).map(parsePsLine).filter(Boolean);
      return rows
        .filter((p) => {
          const args = String(p.args || "").toLowerCase();
          const command = String(p.command || "").toLowerCase();
          const stateBad = /[DZ]/.test(p.stat || "");
          const longScanner = p.elapsedSeconds >= stuckSeconds && /(node|chrome|chromium|multi-audit|api-server|playwright|puppeteer)/i.test(command + " " + args);
          const resourceHeavy = (p.cpuPct >= 80 || p.rssBytes >= 750 * 1024 * 1024) && p.elapsedSeconds >= 300;
          return stateBad || longScanner || resourceHeavy;
        })
        .sort((a, b) => {
          if (/[DZ]/.test(a.stat) !== /[DZ]/.test(b.stat)) return /[DZ]/.test(a.stat) ? -1 : 1;
          return b.elapsedSeconds - a.elapsedSeconds;
        })
        .slice(0, limit);
    }

    function redactLogLine(line) {
      return String(line || "")
        .replace(/\x1B\[[0-?]*[ -/]*[@-~]/g, "")
        .replace(/((?:password|passwd|pwd|token|secret|api[_-]?key)\s*[=:]\s*)[^\s,;]+/ig, "$1***")
        .slice(0, 500);
    }

    function addCandidateLogFile(list, seen, filePath) {
      try {
        if (!filePath || seen.has(filePath)) return;
        const st = fs.statSync(filePath);
        if (!st.isFile()) return;
        seen.add(filePath);
        list.push({ file: filePath, size: st.size, mtimeMs: st.mtimeMs });
      } catch (_) {}
    }

    function candidateLogFiles() {
      const list = [];
      const seen = new Set();
      const envFiles = String(process.env.SERVER_STATS_LOG_FILES || "")
        .split(",")
        .map((v) => v.trim())
        .filter(Boolean);
      envFiles.forEach((f) => addCandidateLogFile(list, seen, f));

      addCandidateLogFile(list, seen, GLOBAL_FAILED_LOG_PATH);
      addCandidateLogFile(list, seen, path.join(OUTPUT_DIR, "failed_scans.csv"));
      addCandidateLogFile(list, seen, path.join(TOOL_DIR, "api-server.log"));
      addCandidateLogFile(list, seen, path.join(TOOL_DIR, "api-server-error.log"));

      const pm2Dirs = [
        path.join(os.homedir() || "", ".pm2", "logs"),
        "/home/ind/.pm2/logs",
        "/root/.pm2/logs",
      ];

      for (const dir of pm2Dirs) {
        try {
          if (!dir || !fs.existsSync(dir)) continue;
          const entries = fs.readdirSync(dir)
            .filter((name) => /\.log$/i.test(name))
            .map((name) => path.join(dir, name));
          entries.forEach((filePath) => addCandidateLogFile(list, seen, filePath));
        } catch (_) {}
      }

      for (const job of activeJobs.values()) {
        if (job && job.logFile) addCandidateLogFile(list, seen, job.logFile);
        if (job && job.jobId) addCandidateLogFile(list, seen, logFilePath(job.jobId));
        if (job && job.batchId) addCandidateLogFile(list, seen, logFilePath(job.batchId));
      }

      return list.sort((a, b) => b.mtimeMs - a.mtimeMs).slice(0, 12);
    }

    function tailLines(filePath, lineCount) {
      lineCount = Math.max(20, Math.min(parseInt(lineCount || "160", 10) || 160, 1000));
      return safeExec(`tail -n ${lineCount} ${shellQuote(filePath)} 2>/dev/null`, {
        timeout: 4000,
        maxBuffer: 512 * 1024,
      }).split(/\r?\n/).filter(Boolean);
    }

    function collectRecentLogErrors() {
      if (String(process.env.SERVER_STATS_INCLUDE_LOGS || "1") === "0") return [];

      const tailCount = parseInt(process.env.SERVER_STATS_LOG_TAIL_LINES || "160", 10) || 160;
      const perFileLimit = parseInt(process.env.SERVER_STATS_LOG_ERROR_LIMIT || "6", 10) || 6;
      const pattern = /(error|failed|failure|fatal|exception|timeout|timed out|cannot|unreachable|denied|refused|crash|killed|oom|out of memory|ENOMEM|EADDRINUSE|ERR_|WARN|⚠|❌)/i;

      const groups = [];
      for (const item of candidateLogFiles()) {
        const picked = tailLines(item.file, tailCount)
          .map(redactLogLine)
          .filter((line) => pattern.test(line))
          .slice(-perFileLimit);
        if (!picked.length) continue;
        groups.push({
          file: item.file,
          fileName: path.basename(item.file),
          sizeHuman: fmtBytes(item.size),
          mtime: new Date(item.mtimeMs).toISOString(),
          lines: picked,
        });
      }
      return groups.slice(0, 5);
    }

    function activeJobSnapshot() {
      const now = Date.now();
      const out = [];
      for (const [key, job] of activeJobs.entries()) {
        try {
          const startedAt = job.startedAt || job.started_at || job.createdAt || null;
          const startedMs = startedAt ? new Date(startedAt).getTime() : null;
          const ageSeconds = startedMs && !isNaN(startedMs) ? Math.max(0, Math.floor((now - startedMs) / 1000)) : null;
          const progress = job.jobId ? readProgress(job.jobId) : null;
          out.push({
            key,
            type: job.type || "",
            jobId: job.jobId || "",
            batchId: job.batchId || "",
            domain: job.domain || "",
            domainCount: job.domainCount || (Array.isArray(job.domains) ? job.domains.length : 0),
            pid: job.pid || "",
            startedAt,
            ageSeconds,
            ageHuman: ageSeconds == null ? "" : fmtDuration(ageSeconds),
            status: progress && progress.status ? progress.status : "",
            lastDomain: progress && progress.last_domain ? progress.last_domain : "",
          });
        } catch (_) {}
      }
      return out.slice(0, 12);
    }

    const mem  = process.memoryUsage();
    const sysMem = systemMemoryStats();
    const disk = dfDisk(SCAN_ROOT);
    const dirs = {
      scan_root:  duDir(SCAN_ROOT),
      output_dir: duDir(OUTPUT_DIR),
    };

    let batchFolderCount = 0;
    let domainFolderCount = 0;
    try {
      for (const e of fs.readdirSync(SCAN_ROOT)) {
        if (!/^\d{4}-\d{2}-\d{2}/.test(e)) continue;
        batchFolderCount++;
        try {
          for (const d of fs.readdirSync(path.join(SCAN_ROOT, e))) {
            if (fs.statSync(path.join(SCAN_ROOT, e, d)).isDirectory()) domainFolderCount++;
          }
        } catch (_) {}
      }
    } catch (_) {}

    const uptimeSec = Math.floor(process.uptime());
    const ud = Math.floor(uptimeSec / 86400);
    const uh = Math.floor((uptimeSec % 86400) / 3600);
    const um = Math.floor((uptimeSec % 3600) / 60);

    const cpuCount = Math.max(1, (os.cpus() || []).length || 1);
    const loadavg = os.loadavg ? os.loadavg() : [0, 0, 0];
    const cpuUsage = cpuUsageSnapshot(process.env.SERVER_STATS_CPU_SAMPLE_MS || "250");
    const processLimit = parseInt(process.env.SERVER_STATS_PROCESS_LIMIT || "6", 10) || 6;
    const topMem = psTop("mem", processLimit);
    const topCpu = psTop("cpu", processLimit);
    const suspects = suspectProcesses(process.env.SERVER_STATS_SUSPECT_LIMIT || "8");
    const recentLogErrors = collectRecentLogErrors();
    const activeJobDetails = activeJobSnapshot();

    const alerts = [];
    const memWarnPct = parseInt(process.env.SERVER_STATS_MEM_WARN_PCT || "85", 10) || 85;
    const diskWarnPct = parseInt(process.env.SERVER_STATS_DISK_WARN_PCT || "85", 10) || 85;
    const cpuWarnPct = parseInt(process.env.SERVER_STATS_CPU_WARN_PCT || "85", 10) || 85;
    if ((cpuUsage.usedPct || 0) >= cpuWarnPct) alerts.push(`High CPU usage: ${cpuUsage.usedPct}% used`);
    if (sysMem.usedPct >= memWarnPct) alerts.push(`High system memory: ${sysMem.usedPct}% used (${sysMem.usedHuman} / ${sysMem.totalHuman})`);
    if ((disk.usedPct || 0) >= diskWarnPct) alerts.push(`High disk usage: ${disk.usedPct}% used (${disk.usedHuman} / ${disk.totalHuman})`);
    if ((loadavg[0] || 0) > cpuCount) alerts.push(`High load average: ${loadavg[0].toFixed(2)} on ${cpuCount} CPU cores`);
    if (suspects.length) alerts.push(`${suspects.length} possible stuck/heavy process(es) found`);
    if (recentLogErrors.length) alerts.push(`${recentLogErrors.length} log file(s) have recent warning/error lines`);

    return jsonResponse(res, 200, {
      ok: true,
      timestamp: new Date().toISOString(),
      disk: { mountpoint: SCAN_ROOT, ...disk },
      directories: dirs,
      batch_folders:  batchFolderCount,
      domain_folders: domainFolderCount,
      active_jobs:    activeJobs.size,
      active_job_details: activeJobDetails,
      memory: {
        rss:        fmtBytes(mem.rss),
        heapUsed:   fmtBytes(mem.heapUsed),
        heapTotal:  fmtBytes(mem.heapTotal),
        external:   fmtBytes(mem.external),
        rssBytes:       mem.rss,
        heapUsedBytes:  mem.heapUsed,
        heapTotalBytes: mem.heapTotal,
        systemTotal: sysMem.totalHuman,
        systemUsed: sysMem.usedHuman,
        systemAvailable: sysMem.availableHuman,
        systemUsedPct: sysMem.usedPct,
      },
      system_memory: sysMem,
      system_cpu: {
        ...cpuUsage,
        cpuCount,
        load1: Number((loadavg[0] || 0).toFixed(2)),
        load5: Number((loadavg[1] || 0).toFixed(2)),
        load15: Number((loadavg[2] || 0).toFixed(2)),
      },
      load: {
        load1: Number((loadavg[0] || 0).toFixed(2)),
        load5: Number((loadavg[1] || 0).toFixed(2)),
        load15: Number((loadavg[2] || 0).toFixed(2)),
        cpuCount,
        cpuUsagePct: cpuUsage.usedPct,
        cpuIdlePct: cpuUsage.idlePct,
        cpuSampleMs: cpuUsage.sampleMs,
      },
      top_processes_by_memory: topMem,
      top_processes_by_cpu: topCpu,
      suspect_processes: suspects,
      recent_log_errors: recentLogErrors,
      alerts,
      node_version:    process.version,
      uptime_seconds:  uptimeSec,
      uptime_human:    `${ud}d ${uh}h ${um}m`,
    });
  }


  // ── GET /server-stats-all ────────────────────────────────────────────────────
  // Server-side monitor aggregator. This lets FileMaker Web Viewer load one URL
  // from VM 1 and avoids browser/Web Viewer CORS + old JavaScript API problems.
  if (method === "GET" && url.pathname === "/server-stats-all") {
    const https = require("https");

    function decodeRepeated(value) {
      let out = String(value || "").trim();

      // FileMaker Web Viewer can double-encode query parameters.
      // Example received by Node after one decode:
      // VM%201%7Chttp%3A%2F%2Fv1.in-depth.com%3A3000
      for (let i = 0; i < 5; i++) {
        const before = out;
        try {
          out = decodeURIComponent(out.replace(/\+/g, " ")).trim();
        } catch (_) {
          break;
        }
        if (out === before) break;
      }

      return out;
    }

    function parseMonitorServers(rawServers) {
      const raw = decodeRepeated(rawServers);

      if (!raw) {
        const fallbackHost = req.headers.host || `localhost:${PORT}`;
        return [{ name: "Server", base: `http://${fallbackHost}` }];
      }

      return raw
        .split(",")
        .map((row) => decodeRepeated(row).trim())
        .filter(Boolean)
        .map((row) => {
          const pipeAt = row.indexOf("|");
          const name = decodeRepeated(pipeAt >= 0 ? row.slice(0, pipeAt).trim() : row.trim());
          const base = decodeRepeated(pipeAt >= 0 ? row.slice(pipeAt + 1).trim() : row.trim());
          return {
            name: name || base,
            base: String(base || "").replace(/\/+$/, ""),
          };
        })
        .filter((srv) => srv.name && srv.base);
    }

    function normalizeStatsUrl(base) {
      const cleanBase = String(base || "").replace(/\/+$/, "");
      return new URL(cleanBase + "/server-stats");
    }

    function getJsonFromUrl(targetUrl, timeoutMs) {
      return new Promise((resolve) => {
        const startedAt = Date.now();
        let finished = false;
        let body = "";
        let reqStats = null;

        function done(payload) {
          if (finished) return;
          finished = true;
          resolve({ ...payload, ms: Date.now() - startedAt });
        }

        try {
          const client = targetUrl.protocol === "https:" ? https : http;
          reqStats = client.request(
            targetUrl,
            {
              method: "GET",
              headers: {
                "Accept": "application/json",
                "Cache-Control": "no-cache",
              },
            },
            (remoteRes) => {
              remoteRes.setEncoding("utf8");

              remoteRes.on("data", (chunk) => {
                body += chunk;
                if (body.length > 2 * 1024 * 1024) {
                  try { reqStats.destroy(new Error("Response too large")); } catch (_) {}
                }
              });

              remoteRes.on("end", () => {
                const statusCode = remoteRes.statusCode || 0;
                if (statusCode < 200 || statusCode >= 300) {
                  return done({
                    ok: false,
                    error: `HTTP ${statusCode}: ${body.slice(0, 300)}`,
                  });
                }

                try {
                  return done({ ok: true, data: JSON.parse(body) });
                } catch (e) {
                  return done({
                    ok: false,
                    error: `Invalid JSON: ${body.slice(0, 300)}`,
                  });
                }
              });
            }
          );

          reqStats.setTimeout(timeoutMs, () => {
            done({ ok: false, error: `Timeout after ${timeoutMs}ms` });
            try { reqStats.destroy(); } catch (_) {}
          });

          reqStats.on("error", (e) => {
            done({ ok: false, error: e.message || String(e) });
          });

          reqStats.end();
        } catch (e) {
          done({ ok: false, error: e.message || String(e) });
        }
      });
    }

    async function fetchOneServerStats(srv) {
      const timeoutRaw = parseInt(
        url.searchParams.get("timeout_ms") || process.env.SERVER_STATS_TIMEOUT_MS || "8000",
        10
      );
      const timeoutMs = Math.max(1000, Math.min(timeoutRaw || 8000, 60000));

      let statsUrl;
      try {
        statsUrl = normalizeStatsUrl(srv.base);
      } catch (e) {
        return {
          ok: false,
          name: srv.name,
          base: srv.base,
          url: srv.base,
          ms: 0,
          error: "Invalid server URL: " + srv.base,
        };
      }

      const result = await getJsonFromUrl(statsUrl, timeoutMs);

      if (!result.ok) {
        return {
          ok: false,
          name: srv.name,
          base: srv.base,
          url: statsUrl.toString(),
          ms: result.ms || 0,
          error: result.error || "Unknown error",
        };
      }

      return {
        ok: !!(result.data && result.data.ok),
        name: srv.name,
        base: srv.base,
        url: statsUrl.toString(),
        ms: result.ms || 0,
        data: result.data,
        error: result.data && result.data.ok ? undefined : "Server returned ok=false",
      };
    }

    const servers = parseMonitorServers(url.searchParams.get("servers"));
    const results = await Promise.all(servers.map(fetchOneServerStats));

    return jsonResponse(res, 200, {
      ok: true,
      timestamp: new Date().toISOString(),
      total: results.length,
      online: results.filter((item) => item.ok).length,
      offline: results.filter((item) => !item.ok).length,
      servers: results,
    });
  }

  if (method === "GET" && url.pathname === "/server-stats-monitor") {
    try {
      const html = fs.readFileSync(path.join(TOOL_DIR, "server-stats.html"), "utf8");
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
        "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
      });
      return res.end(html);
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: "Could not load server-stats.html: " + e.message });
    }
  }

  // ── Date/folder helpers (used by cleanup endpoints) ───────────────────────

  function formatLocalYmd(dateObj) {
    return (
      dateObj.getFullYear() +
      "-" +
      String(dateObj.getMonth() + 1).padStart(2, "0") +
      "-" +
      String(dateObj.getDate()).padStart(2, "0")
    );
  }

  function extractFolderDateMs(entryName) {
    const m = String(entryName || "").match(/^(\d{4})-(\d{2})-(\d{2})(?:_|$)/);
    if (!m) return null;

    const yyyy = parseInt(m[1], 10);
    const mm = parseInt(m[2], 10) - 1;
    const dd = parseInt(m[3], 10);

    return new Date(yyyy, mm, dd, 0, 0, 0, 0).getTime();
  }

  function isCurrentDayFolder(entryName) {
    const folderDate = String(entryName || "").split("_")[0];
    return folderDate === formatLocalYmd(new Date());
  }

  // ── GET /cleanup-api-files ─────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/cleanup-api-files") {
    const hours = parseInt(url.searchParams.get("hours") || "24", 10);

    if (isNaN(hours) || hours <= 0) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "hours parameter must be a positive integer",
      });
    }

    const result = cleanupOldApiHelperFiles(hours);

    return jsonResponse(res, result.ok ? 200 : 500, {
      ok: result.ok,
      hours,
      deleted_count: result.deleted.length,
      skipped_count: result.skipped.length,
      deleted: result.deleted,
      skipped: result.skipped,
      errors: result.errors,
      cutoff_iso: result.cutoff_iso || null,
      error: result.error || null,
    });
  }



  // ── GET /failed-log ──────────────────────────────────────────────────────────
  // Downloads failed scan logs.
  // Query params:
  //   batch_id  – filter to a specific batch (optional)
  //   format    – "csv" (default) or "txt" for plain-text content-type
  //   limit     – max rows to return, most recent first (default 500)
  //   since     – ISO timestamp; only rows on/after this date are returned
  //
  // FileMaker calls this as: /failed-log?format=txt&limit=200&since=<ISO>
  if (method === "GET" && url.pathname === "/failed-log") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    const format  = (url.searchParams.get("format")   || "csv").toLowerCase();
    const limit   = parseInt(url.searchParams.get("limit") || "500", 10);
    const since   = (url.searchParams.get("since")   || "").trim();

    if (batchId && !isSafeBatchId(batchId)) {
      return jsonError(res, 400, "Invalid batch_id");
    }

    const csv = collectFailedLogCsv(batchId, { limit, since });
    const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    const filename = batchId
      ? `failed-scans-${batchId}-${stamp}.csv`
      : `failed-scans-${stamp}.csv`;

    // Honour ?format=txt so FileMaker Insert from URL receives the
    // content-type it expects and does not stall on a CSV disposition.
    const contentType = format === "txt"
      ? "text/plain; charset=utf-8"
      : "text/csv; charset=utf-8";

    if (res.writableEnded) return;
    res.writeHead(200, {
      "Content-Type": contentType,
      "Content-Disposition": `attachment; filename="${filename}"`,
      "Access-Control-Allow-Origin": "*",
      "Cache-Control": "no-store",
      "Content-Length": Buffer.byteLength(csv),
    });
    res.end(csv);
    return;
  }

  // ── GET /failed-log/trim ──────────────────────────────────────────────────
  // Trims failed_scans.csv to the last N rows so it never grows unbounded.
  // Call periodically from a FileMaker scheduled script or a cron job:
  //   GET /failed-log/trim?keep=1000
  if (method === "GET" && url.pathname === "/failed-log/trim") {
    const keep = parseInt(url.searchParams.get("keep") || "1000", 10);
    if (isNaN(keep) || keep <= 0) {
      return jsonError(res, 400, "keep must be a positive integer");
    }
    const result = trimFailedScanLog(keep);
    return jsonResponse(res, result.ok ? 200 : 500, {
      ok: result.ok,
      trimmed: result.trimmed || 0,
      kept: result.kept || 0,
      file: GLOBAL_FAILED_LOG_PATH,
      error: result.error || null,
    });
  }

  // ── GET /queue/clear ───────────────────────────────────────────────────────
  // Clears queued/waiting multi batches from disk. Useful after API restart when
  // old queued batches should not remain visible in the domains monitor.

  if ((method === "GET" || method === "DELETE") && url.pathname === "/queue/clear") {
    const includeFinal = ["1", "true", "yes"].includes(String(url.searchParams.get("include_final") || "").toLowerCase());
    const result = clearVisibleQueuedBatchFiles("manual", { includeFinal });
    return jsonResponse(res, result.ok ? 200 : 500, {
      ok: result.ok,
      cleared_count: result.cleared.length,
      skipped_count: result.skipped.length,
      cleared: result.cleared,
      skipped: result.skipped,
      errors: result.errors,
    });
  }

  // ── DELETE /jobs/stale ─────────────────────────────────────────────────────

  if ((method === "DELETE" || method === "GET") && url.pathname === "/jobs/stale") {
    const hours = parseFloat(url.searchParams.get("hours") || "2");
    if (isNaN(hours) || hours <= 0) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "hours parameter must be a positive number",
      });
    }
    const maxAgeMs = hours * 60 * 60 * 1000;
    const { killed } = purgeStaleJobs(maxAgeMs);
    return jsonResponse(res, 200, {
      ok: true,
      purged: killed.length,
      threshold_hours: hours,
      killed,
    });
  }

  // ── GET /jobs ──────────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/jobs") {
    const jobs = [...activeJobs.values()].map((j) => {
      if (j.type !== "multi") {
        return {
          key: j.key,
          type: j.type,
          domain: j.domain || null,
          batchId: j.batchId || null,
          jobId: j.jobId,
          pid: j.pid,
          startedAt: j.startedAt,
          tools: j.tools || [],
          domains: null,
          domain_statuses: null,
        };
      }

      const meta = getBatchMeta(j.batchId) || null;
      if (meta && isFinalBatchStatus(meta.status)) return null;
      const progress = readProgress(j.batchId);
      const domains = domainsForBatch(j.batchId, j, meta);
      const effectiveProgress = progressForBatchStatus(meta, progress, domains, true);
      const resultSet = resultDomainsForBatch(j.batchId, meta);
      const domainStatuses = withDomainProcessTimes(
        j.batchId,
        buildDomainStatusList(domains, effectiveProgress, true, meta, resultSet),
        true,
        meta
      );

      return {
        key: j.key,
        type: j.type,
        domain: null,
        batchId: j.batchId || null,
        jobId: j.jobId,
        pid: j.pid,
        startedAt: j.startedAt,
        status: (meta && meta.status) || "RUNNING",
        queuedAt: meta && meta.enqueuedAt || null,
        tools: j.tools || [],
        domains,
        domain_statuses: domainStatuses,
        domain_count: domains.length || j.domainCount || 0,
        queued_count: domainStatuses.filter((item) => item.status === "queued").length,
        processing_count: domainStatuses.filter((item) => item.status === "processing").length,
        done_count: domainStatuses.filter((item) => item.status === "done").length,
        failed_count: domainStatuses.filter((item) => item.status === "failed").length,
      };
    }).filter(Boolean);

    // Also surface queued / waiting-gap multi batches that have been accepted by
    // /multi-enqueue + /multi-scan-start but have not spawned multi-audit.js yet.
    // Without this, the monitor only shows the currently running single job until
    // the minutes gap clears and the multi batch actually starts.
    for (const { meta, domains } of listVisibleQueuedBatchMetas()) {
      const batchId = meta.batchId;
      const progress = progressForBatchStatus(meta, readProgress(batchId), domains, false);
      const resultSet = resultDomainsForBatch(batchId, meta);
      const domainStatuses = withDomainProcessTimes(
        batchId,
        buildDomainStatusList(domains, progress, false, meta, resultSet),
        false,
        meta
      );

      jobs.push({
        key: `multi:${batchId}`,
        type: "multi",
        domain: null,
        batchId,
        jobId: batchId,
        pid: null,
        startedAt: meta.startedAt || null,
        queuedAt: meta.enqueuedAt || meta.lastChunkAt || null,
        status: meta.status || "QUEUED",
        tools: meta.tools || [],
        domains,
        domain_statuses: domainStatuses,
        domain_count: domains.length || meta.domainCount || meta.totalQueued || 0,
        queued_count: domainStatuses.filter((item) => item.status === "queued").length,
        processing_count: domainStatuses.filter((item) => item.status === "processing").length,
        done_count: domainStatuses.filter((item) => item.status === "done").length,
        failed_count: domainStatuses.filter((item) => item.status === "failed").length,
      });
    }

    return jsonResponse(res, 200, {
      ok: true,
      api_started_at: API_STARTED_AT,
      server_time: nowIso(),
      reserved_domains: currentReservedDomainCount(),
      max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
      jobs,
      pool_stats: poolStats(),
    });
  }

  // ── GET /cleanup-batches ───────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/cleanup-batches") {
    const days = parseInt(url.searchParams.get("days") || "7", 10);

    if (isNaN(days) || days <= 0) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "days parameter must be a positive integer",
      });
    }

    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    const deleted = [];
    const skipped = [];
    const errors = [];

    try {
      const entries = fs.readdirSync(SCAN_ROOT);

      for (const entry of entries) {
        if (!/^\d{4}-\d{2}-\d{2}_/.test(entry)) continue;

        const fullPath = path.join(SCAN_ROOT, entry);

        try {
          const stat = fs.statSync(fullPath);

          if (!stat.isDirectory()) {
            skipped.push({ entry, reason: "not a directory" });
            continue;
          }

          const folderDateMs = extractFolderDateMs(entry);
          if (folderDateMs == null) {
            skipped.push({ entry, reason: "invalid folder date format" });
            continue;
          }

          if (isCurrentDayFolder(entry)) {
            skipped.push({ entry, reason: "current day folder" });
            continue;
          }

          if (folderDateMs < cutoff) {
            fs.rmSync(fullPath, { recursive: true, force: true });
            deleted.push(entry);
            console.log(`[cleanup] Deleted old batch: ${entry}`);
          } else {
            skipped.push({ entry, reason: "newer than cutoff" });
          }
        } catch (e) {
          errors.push({ entry, error: e.message });
        }
      }
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: e.message });
    }

    return jsonResponse(res, 200, {
      ok: true,
      deleted,
      skipped,
      errors,
      days,
      message: `Deleted ${deleted.length} batch folders older than ${days} days`,
    });
  }

  // ── GET /latest ────────────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/latest") {
    const format = url.searchParams.get("format") || "json";
    const latestCsv = path.join(OUTPUT_DIR, "latest.csv");
    const latestJson = path.join(OUTPUT_DIR, "latest.json");

    if (format === "csv") {
      if (fs.existsSync(latestCsv)) {
        const content = fs.readFileSync(latestCsv, "utf8");
        res.writeHead(200, {
          "Content-Type": "text/csv; charset=utf-8",
          "Access-Control-Allow-Origin": "*",
          "Content-Disposition": "attachment; filename=latest.csv"
        });
        return res.end(content);
      } else {
        return jsonResponse(res, 404, { ok: false, error: "latest.csv not found" });
      }
    } else {
      if (fs.existsSync(latestJson)) {
        const content = fs.readFileSync(latestJson, "utf8");
        res.writeHead(200, {
          "Content-Type": "application/json",
          "Access-Control-Allow-Origin": "*"
        });
        return res.end(content);
      } else {
        return jsonResponse(res, 404, { ok: false, error: "latest.json not found" });
      }
    }
  }

  // ── GET /latest/domain ─────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/latest/domain") {
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");
    if (!domain) {
      return jsonResponse(res, 400, { ok: false, error: "domain parameter required" });
    }

    try {
      const { getDomainFromLatest } = require('./utils/latest-results');
      const data = getDomainFromLatest(domain);

      if (data) {
        return jsonResponse(res, 200, { ok: true, data });
      } else {
        return jsonResponse(res, 404, { ok: false, error: `Domain ${domain} not found in latest results` });
      }
    } catch (err) {
      return jsonResponse(res, 500, { ok: false, error: err.message });
    }
  }

  // ── GET /latest/stats ──────────────────────────────────────────────────────

  if (method === "GET" && url.pathname === "/latest/stats") {
    try {
      const { getAllLatestDomains } = require('./utils/latest-results');
      const allDomains = getAllLatestDomains();

      const stats = {
        total_domains: allDomains.length,
        last_updated: allDomains.length > 0 ?
          allDomains.reduce((latest, d) => {
            const date = new Date(d.Run_At);
            return date > latest ? date : latest;
          }, new Date(0)).toISOString() : null,
        ssl_grade_distribution: {},
        pagespeed_performance_distribution: {},
        sucuri_status_distribution: {}
      };

      for (const domain of allDomains) {
        const sslGrade = domain.SSL_Grade || "N/A";
        stats.ssl_grade_distribution[sslGrade] = (stats.ssl_grade_distribution[sslGrade] || 0) + 1;

        const pagespeedPerf = domain.PageSpeed_Performance || "N/A";
        stats.pagespeed_performance_distribution[pagespeedPerf] = (stats.pagespeed_performance_distribution[pagespeedPerf] || 0) + 1;

        const sucuriStatus = domain.Sucuri_Overall || "N/A";
        stats.sucuri_status_distribution[sucuriStatus] = (stats.sucuri_status_distribution[sucuriStatus] || 0) + 1;
      }

      return jsonResponse(res, 200, { ok: true, stats });
    } catch (err) {
      return jsonResponse(res, 500, { ok: false, error: err.message });
    }
  }

  // ── GET /cleanup (backward compat) ────────────────────────────────────────
  // Supports:
  //   ?days=N   – delete batch folders older than N days (uses folder date, skips today)
  //   ?hours=N  – delete batch folders whose mtime is older than N hours (works same-day)

  if (method === "GET" && url.pathname === "/cleanup") {
    const hoursRaw = url.searchParams.get("hours");
    const daysRaw  = url.searchParams.get("days");

    const useHours = hoursRaw != null;
    const hours    = useHours ? parseInt(hoursRaw, 10) : null;
    const days     = useHours ? null : parseInt(daysRaw || "7", 10);

    if (useHours && (isNaN(hours) || hours <= 0)) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "hours parameter must be a positive integer",
      });
    }
    if (!useHours && (isNaN(days) || days <= 0)) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "days parameter must be a positive integer",
      });
    }

    const cutoff = useHours
      ? Date.now() - hours * 60 * 60 * 1000
      : Date.now() - days * 24 * 60 * 60 * 1000;

    const deleted = [];
    const skipped = [];
    const errors = [];

    try {
      const entries = fs.readdirSync(SCAN_ROOT);

      for (const entry of entries) {
        if (!/^\d{4}-\d{2}-\d{2}/.test(entry)) continue;

        const fullPath = path.join(SCAN_ROOT, entry);

        try {
          const stat = fs.statSync(fullPath);

          if (!stat.isDirectory()) {
            skipped.push({ entry, reason: "not a directory" });
            continue;
          }

          if (useHours) {
            // Use actual mtime for sub-day precision; bypass the current-day guard.
            if (stat.mtimeMs < cutoff) {
              fs.rmSync(fullPath, { recursive: true, force: true });
              deleted.push(entry);
              console.log(`[cleanup] Deleted batch folder (hours mode): ${entry}`);
            } else {
              skipped.push({ entry, reason: "newer than cutoff" });
            }
          } else {
            const folderDateMs = extractFolderDateMs(entry);
            if (folderDateMs == null) {
              skipped.push({ entry, reason: "invalid folder date format" });
              continue;
            }
            if (isCurrentDayFolder(entry)) {
              skipped.push({ entry, reason: "current day folder" });
              continue;
            }
            if (folderDateMs < cutoff) {
              fs.rmSync(fullPath, { recursive: true, force: true });
              deleted.push(entry);
              console.log(`[cleanup] Deleted old scan folder: ${entry}`);
            } else {
              skipped.push({ entry, reason: "newer than cutoff" });
            }
          }
        } catch (e) {
          errors.push({ entry, error: e.message });
        }
      }
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: e.message });
    }

    return jsonResponse(res, 200, {
      ok: true,
      deleted,
      skipped,
      errors,
      ...(useHours ? { hours } : { days }),
      message: `Deleted ${deleted.length} batch folders older than ${useHours ? `${hours} hours` : `${days} days`}`,
    });
  }

  // ── GET /images/* ─────────────────────────────────────────────────────────
  // FileMaker fallback URL: /images/{domain}/{file.png}
  // Searches the most recent batch folder for that domain's screenshot.

  if (method === "GET" && url.pathname.startsWith("/images/")) {
    const relPath = decodeURIComponent(url.pathname.slice("/images/".length));
    const parts = relPath.split("/").filter(Boolean);
    if (parts.length < 2) {
      return jsonResponse(res, 400, { ok: false, error: "Expected /images/{domain}/{file}" });
    }
    const domainSegment = parts[0];
    const fileSegment = parts[parts.length - 1];
    if (domainSegment.includes("..") || fileSegment.includes("..")) {
      return jsonResponse(res, 403, { ok: false, error: "Forbidden" });
    }
    const ext = path.extname(fileSegment).toLowerCase();
    const mimeMap = { ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp" };
    const contentType = mimeMap[ext] || "application/octet-stream";
    let absPath = null;
    try {
      const entries = fs.readdirSync(SCAN_ROOT)
        .filter((e) => /^\d{4}-\d{2}-\d{2}/.test(e))
        .sort((a, b) => b.localeCompare(a, undefined, { numeric: true }));
      for (const entry of entries) {
        const candidate = path.join(SCAN_ROOT, entry, domainSegment, "images", fileSegment);
        if (fs.existsSync(candidate)) { absPath = candidate; break; }
      }
    } catch (_) {}
    if (!absPath) return jsonResponse(res, 404, { ok: false, error: "Image not found" });
    try {
      const buf = fs.readFileSync(absPath);
      res.writeHead(200, { "Content-Type": contentType, "Content-Length": buf.length, "Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*" });
      res.end(buf);
    } catch (err) { return jsonResponse(res, 500, { ok: false, error: err.message }); }
    return;
  }

  // ── GET /scan-image/* ─────────────────────────────────────────────────────
  // Explicit batch path: /scan-image/{batchFolder}/{domain}/images/{file.png}

  if (method === "GET" && url.pathname.startsWith("/scan-image/")) {
    const relPath = decodeURIComponent(url.pathname.slice("/scan-image/".length));
    const absPath = path.normalize(path.join(SCAN_ROOT, relPath));
    if (!absPath.startsWith(path.normalize(SCAN_ROOT) + path.sep)) {
      return jsonResponse(res, 403, { ok: false, error: "Forbidden" });
    }
    if (!fs.existsSync(absPath)) return jsonResponse(res, 404, { ok: false, error: "Image not found" });
    const ext = path.extname(absPath).toLowerCase();
    const mimeMap = { ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".webp": "image/webp" };
    const contentType = mimeMap[ext] || "application/octet-stream";
    try {
      const buf = fs.readFileSync(absPath);
      res.writeHead(200, { "Content-Type": contentType, "Content-Length": buf.length, "Cache-Control": "no-cache", "Access-Control-Allow-Origin": "*" });
      res.end(buf);
    } catch (err) { return jsonResponse(res, 500, { ok: false, error: err.message }); }
    return;
  }


  // ── POST /multi-enqueue ────────────────────────────────────────────────────
  // Receives one chunk of domains from FileMaker and appends it to a queued
  // batch domain-list file WITHOUT starting the scan process.  This endpoint
  // must always return JSON; FileMaker treats an empty response as error 1631.

  if (method === "POST" && url.pathname === "/multi-enqueue") {
    try {
      let parsed;
      try {
        parsed = await readJsonBody(req);
      } catch (e) {
        return jsonError(res, 400, e.message, { endpoint: "/multi-enqueue" });
      }

      const incomingTargets = uniqueDomainTargets(parsed.domains || []);
      const incomingDomains = incomingTargets.map((item) => item.domain);
      const existingBatchId = String(parsed.batch_id || "").trim();

      if (existingBatchId && !isSafeBatchId(existingBatchId)) {
        return jsonError(res, 400, "Invalid batch_id", { batch_id: existingBatchId });
      }

      const batchId = existingBatchId || createBatchId();
      const tools = normalizeSelectedTools(parsed.tools);
      const enabledTools = tools.length ? tools : ALL_TOOL_KEYS;
      const scanSettings = parsed.scan_settings || {};
      const serverName = String(parsed.server_name || scanSettings.server_name || "").trim();

      if (!incomingDomains.length) {
        return jsonError(res, 400, "No domains provided", { endpoint: "/multi-enqueue" });
      }

      const domainListFilePath = queuedDomainListPath(batchId);
      const existingMeta = getBatchMeta(batchId) || {};
      const existingQueuedDomains = readDomainsFromFile(domainListFilePath);
      const existingSet = new Set(existingQueuedDomains);
      const newTargetsToAppend = incomingTargets.filter((item) => !existingSet.has(item.domain));
      const newDomainsToAppend = newTargetsToAppend.map((item) => item.domain);
      const newRawTargetsToAppend = newTargetsToAppend.map((item) => item.raw);
      const duplicateCount = incomingDomains.length - newDomainsToAppend.length;

      try {
        ensureDir(API_BATCH_DIR);
        // Append only NEW normalized domains. This makes /multi-enqueue idempotent
        // when FileMaker retries a request or the web viewer refreshes while a queue is being built.
        if (newDomainsToAppend.length) {
          fs.appendFileSync(domainListFilePath, newRawTargetsToAppend.join("\n") + "\n", "utf8");
        } else if (!fs.existsSync(domainListFilePath)) {
          fs.writeFileSync(domainListFilePath, "", "utf8");
        }
      } catch (writeErr) {
        console.error(`[multi-enqueue] write failed batch=${batchId}: ${writeErr.stack || writeErr.message}`);
        return jsonError(res, 500, "Queue file write failed", {
          endpoint: "/multi-enqueue",
          batch_id: batchId,
          file: domainListFilePath,
          detail: writeErr.message,
        });
      }

      const finalQueuedDomains = readDomainsFromFile(domainListFilePath);
      const totalQueued = finalQueuedDomains.length;

      const meta = {
        ...existingMeta,
        batchId,
        status: existingMeta.status && existingMeta.status !== "ERROR" ? existingMeta.status : "QUEUED",
        serverName,
        tools: enabledTools,
        scanSettings,
        domainListFile: domainListFilePath,
        domains: finalQueuedDomains,
        totalQueued,
        domainCount: totalQueued,
        enqueuedAt: existingMeta.enqueuedAt || nowIso(),
        lastChunkAt: nowIso(),
        lastChunkCount: newDomainsToAppend.length,
        duplicateChunkCount: duplicateCount,
      };

      try {
        saveBatchMetaStrict(meta);
      } catch (metaErr) {
        console.error(`[multi-enqueue] meta save failed batch=${batchId}: ${metaErr.stack || metaErr.message}`);
        return jsonError(res, 500, "Queue metadata write failed", {
          endpoint: "/multi-enqueue",
          batch_id: batchId,
          file: batchMetaPath(batchId),
          detail: metaErr.message,
        });
      }

      console.log(`[multi-enqueue] batch_id=${batchId} +${newDomainsToAppend.length} new domains duplicates=${duplicateCount} total=${totalQueued}`);

      return jsonResponse(res, 200, {
        ok: 1,
        batch_id: batchId,
        queued: newDomainsToAppend.length,
        duplicates: duplicateCount,
        total_queued: totalQueued,
        status: "queued",
        domain_list_file: domainListFilePath,
      });
    } catch (err) {
      console.error(`[multi-enqueue] uncaught: ${err.stack || err.message}`);
      return jsonError(res, 500, "Unhandled /multi-enqueue error", {
        endpoint: "/multi-enqueue",
        detail: err.message,
      });
    }
  }

  // ── POST /multi-scan-start ─────────────────────────────────────────────────
  // Starts processing a batch that was previously built via /multi-enqueue.

  if (method === "POST" && url.pathname === "/multi-scan-start") {
    try {
      let parsed;
      try {
        parsed = await readJsonBody(req);
      } catch (e) {
        return jsonError(res, 400, e.message, { endpoint: "/multi-scan-start" });
      }

      const batchId = String(parsed.batch_id || "").trim();
      if (!batchId) return jsonError(res, 400, "Missing batch_id", { endpoint: "/multi-scan-start" });
      if (!isSafeBatchId(batchId)) return jsonError(res, 400, "Invalid batch_id", { batch_id: batchId });

      const meta = getBatchMeta(batchId);
      if (!meta) return jsonError(res, 404, `Batch ${batchId} not found`, { batch_id: batchId });

      if (meta.status === "RUNNING") {
        return jsonError(res, 409, "Batch is already running", { batch_id: batchId });
      }

      const domainListFilePath = meta.domainListFile || queuedDomainListPath(batchId);
      if (!domainListFilePath || !fs.existsSync(domainListFilePath)) {
        return jsonError(res, 400, "Domain list file not found — enqueue domains first", {
          batch_id: batchId,
          file: domainListFilePath || null,
        });
      }

      const actualQueued = countDomainsInFile(domainListFilePath);
      const enabledTools = meta.tools || ALL_TOOL_KEYS;
      const scanSettings = meta.scanSettings || {};
      const minutesGap = parseFloat(scanSettings.minutes_gap || process.env.MINUTES_GAP || "0") || 0;
      const maxConc = parseInt(scanSettings.max_concurrent || process.env.MAX_CONCURRENT || "3", 10) || 3;
      const launchDelay = minutesGap > 0 ? Math.round(minutesGap * 60 * 1000) : 3000;
      const effectiveMax = minutesGap > 0 ? 1 : Math.max(1, maxConc);
      const initialUsage = getUsage();
      const initialGapMinutes = minutesUntilNextAllowed(minutesGap, initialUsage.last_scan_at);
      const batchKey = `multi:${batchId}`;
      const queuedAt = nowIso();

      // Persist the queued/waiting status before responding so /jobs and
      // /multi-progress can show the multi domains immediately in the monitor.
      meta.status = initialGapMinutes > 0 ? "WAITING_GAP" : "QUEUED";
      meta.queuedAt = queuedAt;
      meta.minutesGap = minutesGap;
      meta.minutesRemaining = initialGapMinutes;
      meta.scheduledStartAt = initialGapMinutes > 0
        ? new Date(Date.now() + (initialGapMinutes * 60000)).toISOString()
        : queuedAt;
      meta.totalQueued = actualQueued;
      meta.domainCount = actualQueued;
      meta.domainListFile = domainListFilePath;
      saveBatchMeta(meta);

      // Respond BEFORE spawn/file-heavy work. FileMaker only needs the batch_id.
      // If the server is still inside the minutes-gap window, the batch is accepted
      // and held as WAITING_GAP until it is safe to start.
      jsonResponse(res, 200, {
        ok: 1,
        batch_id: batchId,
        count: actualQueued,
        queued_at: queuedAt,
        status: initialGapMinutes > 0 ? "waiting_gap" : "queued",
        minutes_gap: minutesGap,
        minutes_remaining: initialGapMinutes,
        estimated_wait_minutes: initialGapMinutes,
        scheduled_start_at: meta.scheduledStartAt,
      });

      setImmediate(async () => {
        try {
          // For multi scan, FileMaker can submit the queue now, but Node should
          // not actually launch multi-audit until the minutes gap from the last
          // scan start is clear. Re-check in a loop so another scan that starts
          // while this batch is waiting still resets the gap correctly.
          while (true) {
            const latestUsage = getUsage();
            const waitMinutes = minutesUntilNextAllowed(minutesGap, latestUsage.last_scan_at);

            if (waitMinutes <= 0) break;

            meta.status = "WAITING_GAP";
            meta.waitingReason =
              `Waiting ${waitMinutes} minute(s) for minutes gap (${minutesGap}) before starting.`;
            meta.minutesGap = minutesGap;
            meta.minutesRemaining = waitMinutes;
            meta.scheduledStartAt = new Date(Date.now() + (waitMinutes * 60000)).toISOString();
            meta.totalQueued = actualQueued;
            meta.domainCount = actualQueued;
            meta.domainListFile = domainListFilePath;
            saveBatchMeta(meta);

            console.log(
              `[multi-scan-start] batch=${batchId} WAITING_GAP wait=${waitMinutes}m gap=${minutesGap}m`
            );

            await new Promise((resolve) => setTimeout(resolve, waitMinutes * 60000));
          }

          const startedAt = nowIso();
          meta.status = "RUNNING";
          meta.startedAt = startedAt;
          meta.totalQueued = actualQueued;
          meta.domainCount = actualQueued;
          meta.domainListFile = domainListFilePath;
          saveBatchMeta(meta);

          initJobProgressFile(batchId, "", actualQueued);

          // FIX: Derive the scan batch path the same way multi-audit.js/resolveBatchPath()
          // does — SCAN_ROOT + today's date folder — and store it in meta.scanBatchPath.
          // This lets resolveBatchRootForMultiResult() find the batch root directly from
          // meta instead of relying solely on latest_path_<childJobId>.txt files.
          // If those files are missing (e.g. due to a prior crash), results are still found.
          const todayFolder = (() => {
            const n = new Date();
            const pad = (v) => String(v).padStart(2, "0");
            return `${n.getFullYear()}-${pad(n.getMonth() + 1)}-${pad(n.getDate())}`;
          })();
          const derivedScanBatchPath = path.join(SCAN_ROOT, todayFolder);
          meta.scanBatchPath = derivedScanBatchPath;
          meta.batchRoot = derivedScanBatchPath;
          saveBatchMeta(meta);

          let child;
          try {
            child = spawn("node", ["multi-audit.js", domainListFilePath], {
              cwd: TOOL_DIR,
              env: {
                ...process.env,
                SCAN_BATCH_ID: batchId,
                JOB_ID: batchId,
                ENABLED_TOOLS: JSON.stringify(enabledTools),
                FORCE_RESCAN: "0",
                MINUTES_GAP: String(minutesGap),
                DOMAIN_LAUNCH_DELAY_MS: String(launchDelay),
                MAX_CONCURRENT: String(effectiveMax),
                DOMAIN_LIST_FILE: domainListFilePath,
              },
              detached: false,
              stdio: ["ignore", "pipe", "pipe"],
            });
          } catch (spawnErr) {
            console.error(`[multi-scan-start] spawn failed: ${spawnErr.stack || spawnErr.message}`);
            meta.status = "ERROR";
            meta.error = spawnErr.message;
            meta.finishedAt = nowIso();
            saveBatchMeta(meta);
            writeTextSafe(doneFlagPath(batchId), `done\ncode=-1\nsignal=spawn_failed\nfinishedAt=${nowIso()}\nerror=${spawnErr.message}`);
            return;
          }

          meta.childPid = child.pid;
          saveBatchMeta(meta);

          const logFile = logFilePath(batchId);
          child.stdout.on("data", (c) => appendTextSafe(logFile, c.toString()));
          child.stderr.on("data", (c) => appendTextSafe(logFile, c.toString()));
          child.on("error", (childErr) => {
            appendTextSafe(logFile, `[child-error] ${childErr.stack || childErr.message}\n`);
          });

          activeJobs.set(batchKey, {
            key: batchKey,
            type: "multi",
            batchId,
            domains: domainsForBatch(batchId, null, meta),
            domainCount: actualQueued,
            domainListFile: domainListFilePath,
            tools: enabledTools,
            jobId: batchId,
            pid: child.pid,
            startedAt,
            logFile,
          });
          startBatchTimeWatcher(batchId);

          child.on("exit", (code, signal) => {
            console.log(`[multi-scan-start] batch=${batchId} exit code=${code} signal=${signal}`);
            const jobBeforeDelete = activeJobs.get(batchKey) || null;
            activeJobs.delete(batchKey);
            const saved = getBatchMeta(batchId) || meta;
            saved.status = code === 0 ? "DONE" : "ERROR";
            saved.exitCode = code;
            saved.signal = signal;
            saved.finishedAt = nowIso();
            saveBatchMeta(saved);

            if (code !== 0 || signal) {
              const reason = inferFailureReasonFromLog(logFile, `Multi scan failed. code=${code} signal=${signal || ""}`);
              for (const d of domainsForBatch(batchId, null, saved) || []) {
                appendFailedScanLog({ domain: d, reason, source: "multi", batchId, jobId: batchId });
              }
            }

            finalizeBatchTimeWatcher(batchId, saved);

            // Delete the generated *_domains.txt only after a clean batch exit.
            // If Node/multi-audit crashes, keep the file for resume/debugging.
            if (code === 0) {
              cleanupBatchDomainListFile(batchId, saved, jobBeforeDelete);
            }
          });

          try { recordScan(actualQueued); } catch (_) {}
        } catch (asyncErr) {
          console.error(`[multi-scan-start] async error batch=${batchId}: ${asyncErr.stack || asyncErr.message}`);
          try {
            const saved = getBatchMeta(batchId) || meta;
            saved.status = "ERROR";
            saved.error = asyncErr.message;
            saved.finishedAt = nowIso();
            saveBatchMeta(saved);
            finalizeBatchTimeWatcher(batchId, saved);
            writeTextSafe(doneFlagPath(batchId), `done\ncode=-1\nsignal=async_error\nfinishedAt=${nowIso()}\nerror=${asyncErr.message}`);
          } catch (_) {}
        }
      });

      return;
    } catch (err) {
      console.error(`[multi-scan-start] uncaught: ${err.stack || err.message}`);
      return jsonError(res, 500, "Unhandled /multi-scan-start error", {
        endpoint: "/multi-scan-start",
        detail: err.message,
      });
    }
  }

  // ── GET /queue-status ──────────────────────────────────────────────────────
  // Returns summary of queued/running batches. FileMaker can poll this endpoint.

  if (method === "GET" && url.pathname === "/queue-status") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();

    if (batchId) {
      if (!isSafeBatchId(batchId)) return jsonError(res, 400, "Invalid batch_id", { batch_id: batchId });
      const meta = getBatchMeta(batchId);
      const progress = readProgress(batchId);
      if (!meta) return jsonResponse(res, 404, { ok: false, error: "Batch not found" });

      const job = activeJobs.get(`multi:${batchId}`) || null;
      const domains = domainsForBatch(batchId, job, meta);
      const running = !!job;
      const resultSet = resultDomainsForBatch(batchId, meta);
      const domainStatuses = withDomainProcessTimes(batchId, buildDomainStatusList(domains, progress, running, meta, resultSet), running, meta);

      return jsonResponse(res, 200, {
        ok: 1,
        batch_id: batchId,
        status: meta.status || "UNKNOWN",
        total_queued: domains.length || meta.totalQueued || meta.domainCount || 0,
        completed: domainStatuses.filter((item) => item.status === "done").length,
        failed_count: domainStatuses.filter((item) => item.status === "failed").length,
        queued_count: domainStatuses.filter((item) => item.status === "queued").length,
        processing_count: domainStatuses.filter((item) => item.status === "processing").length,
        running,
        started_at: meta.startedAt || null,
        finished_at: meta.finishedAt || null,
        domains,
        domain_statuses: domainStatuses,
      });
    }

    const runningBatches = [...activeJobs.values()]
      .filter((j) => j.type === "multi")
      .map((j) => ({
        batch_id: j.batchId,
        domain_count: j.domainCount || 0,
        started_at: j.startedAt,
        pid: j.pid,
      }));

    return jsonResponse(res, 200, {
      ok: 1,
      running_batches: runningBatches,
      total_running: runningBatches.length,
      reserved_slots: currentReservedDomainCount(),
      now: nowIso(),
    });
  }

  // ── 404 catch-all ─────────────────────────────────────────────────────────

  return jsonResponse(res, 404, {
    ok: false,
    error: `Unknown endpoint: ${url.pathname}`,
  });
}

// ── Start server ──────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  Promise.resolve(handleRequest(req, res)).catch((err) => {
    console.error(`[api] unhandled request error ${req.method} ${req.url}: ${err.stack || err.message}`);
    if (!res.writableEnded) {
      jsonError(res, 500, "Unhandled API request error", { detail: err.message });
    }
  });
});

server.requestTimeout = parseInt(process.env.API_REQUEST_TIMEOUT_MS || "120000", 10);
server.headersTimeout = parseInt(process.env.API_HEADERS_TIMEOUT_MS || "125000", 10);
server.keepAliveTimeout = parseInt(process.env.API_KEEPALIVE_TIMEOUT_MS || "5000", 10);

process.on("unhandledRejection", (err) => {
  console.error(`[api] unhandledRejection: ${err && err.stack ? err.stack : err}`);
});

process.on("uncaughtException", (err) => {
  console.error(`[api] uncaughtException: ${err && err.stack ? err.stack : err}`);
  // Do not process.exit() here. The request wrapper returns JSON where possible,
  // and systemd/pm2 can still restart the service if the process becomes unhealthy.
});

const startupQueueCleanup = clearVisibleQueuedBatchFiles("startup");
if (startupQueueCleanup.cleared.length || startupQueueCleanup.errors.length) {
  console.log(`[api] startup queued batch cleanup cleared=${startupQueueCleanup.cleared.length} errors=${startupQueueCleanup.errors.length}`);
}

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[api] ssl-checker-tool API server running on port ${PORT}`);
  console.log(`[api] TOOL_DIR=${TOOL_DIR}`);
  console.log(`[api] OUTPUT_DIR=${OUTPUT_DIR}`);
  console.log(`[api] SCAN_ROOT=${SCAN_ROOT}`);
});

server.on("error", (err) => {
  if (err && err.code === "EADDRINUSE") {
    console.error(`[api] EADDRINUSE: port ${PORT} is already in use. Do not start a second PM2 copy of api-server.js.`);
  } else {
    console.error(`[api] server error: ${err && err.stack ? err.stack : err.message}`);
  }
  process.exit(1);
});
// ── Scheduled self-cleanup ────────────────────────────────────────────────────
// Runs every hour automatically. Cleans batch folders, domain subfolders,
// helper files, and api_batches meta older than PERIODIC_CLEANUP_HOURS (default 1h).
// SAFE: active batch folders are always skipped even if older than the cutoff.
const PERIODIC_CLEANUP_HOURS = parseInt(process.env.PERIODIC_CLEANUP_HOURS || "1", 10);
const PERIODIC_CLEANUP_INTERVAL_MS = 60 * 60 * 1000; // every 1 hour

/**
 * Returns a Set of SCAN_ROOT folder basenames currently in use by an active job.
 * Reads latest_path_*.txt files and batch meta JSON to find the real folder paths.
 */
function getActiveBatchFolders() {
  const protected_ = new Set();
  try {
    const activeBatchIds = new Set();
    for (const [key] of activeJobs.entries()) {
      if (key.startsWith("multi:"))  activeBatchIds.add(key.slice(6));
      if (key.startsWith("single:")) activeBatchIds.add(key.slice(7));
    }
    if (activeBatchIds.size === 0) return protected_;

    // Read latest_path_*.txt files to find actual folder paths
    for (const entry of fs.readdirSync(OUTPUT_DIR)) {
      if (!entry.startsWith("latest_path_") || !entry.endsWith(".txt")) continue;
      const matchesActive = [...activeBatchIds].some((id) => entry.includes(id));
      if (!matchesActive) continue;
      try {
        const latestPath = fs.readFileSync(path.join(OUTPUT_DIR, entry), "utf8").trim();
        if (!latestPath) continue;
        let p = latestPath;
        while (p && p !== SCAN_ROOT && path.dirname(p) !== SCAN_ROOT) p = path.dirname(p);
        if (path.dirname(p) === SCAN_ROOT) protected_.add(path.basename(p));
      } catch (_) {}
    }

    // Also check batch meta for scanBatchPath
    for (const batchId of activeBatchIds) {
      try {
        const metaPath = path.join(API_BATCH_DIR, `batch_${batchId}.json`);
        if (!fs.existsSync(metaPath)) continue;
        const meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
        const root = meta.scanBatchPath || meta.batchRoot || meta.batchPath;
        if (root && path.dirname(root) === SCAN_ROOT) protected_.add(path.basename(root));
      } catch (_) {}
    }
  } catch (e) {
    console.error(`[periodic-cleanup] Error building active batch set: ${e.message}`);
  }
  return protected_;
}

function runPeriodicCleanup() {
  const cutoff = Date.now() - PERIODIC_CLEANUP_HOURS * 60 * 60 * 1000;

  // 1. Batch folders under SCAN_ROOT — skip any still used by an active job
  const activeFolders = getActiveBatchFolders();
  if (activeFolders.size > 0) {
    console.log(`[periodic-cleanup] Protecting active batch folders: ${[...activeFolders].join(", ")}`);
  }

  let batchDeleted = 0;
  let batchSkipped = 0;
  let batchErrors  = 0;
  try {
    for (const entry of fs.readdirSync(SCAN_ROOT)) {
      if (!/^\d{4}-\d{2}-\d{2}/.test(entry)) continue;

      if (activeFolders.has(entry)) {
        batchSkipped++;
        console.log(`[periodic-cleanup] Skipping active batch folder: ${entry}`);
        continue;
      }

      const fullPath = path.join(SCAN_ROOT, entry);
      try {
        const stat = fs.statSync(fullPath);
        if (stat.isDirectory() && stat.mtimeMs < cutoff) {
          fs.rmSync(fullPath, { recursive: true, force: true });
          batchDeleted++;
          console.log(`[periodic-cleanup] Deleted batch folder: ${entry}`);
        }
      } catch (e) {
        batchErrors++;
        console.error(`[periodic-cleanup] Error deleting ${entry}: ${e.message}`);
      }
    }
  } catch (e) {
    console.error(`[periodic-cleanup] Could not read SCAN_ROOT: ${e.message}`);
  }

  // 1b. Per-domain subfolders inside ACTIVE batch folders — free disk space
  //     domain-by-domain for large long-running batches.
  let domainDeleted = 0;
  let domainSkipped = 0;
  let domainErrors  = 0;
  for (const activeFolder of activeFolders) {
    const batchPath = path.join(SCAN_ROOT, activeFolder);
    try {
      for (const entry of fs.readdirSync(batchPath)) {
        const domainPath = path.join(batchPath, entry);
        try {
          const stat = fs.statSync(domainPath);
          if (!stat.isDirectory()) continue;
          if (stat.mtimeMs >= cutoff) { domainSkipped++; continue; }

          const hasSummary   = fs.existsSync(path.join(domainPath, "summary.csv"));
          const hasResultCsv = fs.existsSync(path.join(domainPath, `${entry}_results.csv`));
          if (!hasSummary && !hasResultCsv) { domainSkipped++; continue; }

          const imagesDir = path.join(domainPath, "images");
          if (fs.existsSync(imagesDir)) {
            const remaining = fs.readdirSync(imagesDir)
              .filter((f) => [".png",".jpg",".jpeg",".webp"].includes(path.extname(f).toLowerCase()));
            if (remaining.length > 0) {
              domainSkipped++;
              console.log(`[periodic-cleanup] Skipping ${entry} — images not yet synced to ta1`);
              continue;
            }
          }

          fs.rmSync(domainPath, { recursive: true, force: true });
          domainDeleted++;
          console.log(`[periodic-cleanup] Deleted completed domain folder: ${activeFolder}/${entry}`);
        } catch (e) {
          domainErrors++;
          console.error(`[periodic-cleanup] Error processing domain ${entry}: ${e.message}`);
        }
      }
    } catch (e) {
      console.error(`[periodic-cleanup] Could not read active batch ${activeFolder}: ${e.message}`);
    }
  }

  // 2. Helper files under OUTPUT_DIR.
  // Covers every per-job file type the API and multi-audit produce.
  // Explicitly excludes failed_scans.csv (the global log that should persist).
  const HELPER_FILE_RE = /^(progress_.+\.(txt|log)|done_.+\.flag|latest_path_.+\.txt|checkpoint_.+\.json|failed_.{6,}\.csv|lock_.+\.pid)$/;

  let helperDeleted = 0;
  let helperErrors = 0;
  try {
    for (const entry of fs.readdirSync(OUTPUT_DIR)) {
      if (entry === "failed_scans.csv") continue; // keep global log
      if (!HELPER_FILE_RE.test(entry)) continue;
      const fullPath = path.join(OUTPUT_DIR, entry);
      try {
        const stat = fs.statSync(fullPath);
        if (stat.isFile() && stat.mtimeMs < cutoff) {
          fs.unlinkSync(fullPath);
          helperDeleted++;
          console.log(`[periodic-cleanup] Deleted helper file: ${entry}`);
        }
      } catch (e) {
        helperErrors++;
        console.error(`[periodic-cleanup] Error deleting ${entry}: ${e.message}`);
      }
    }
  } catch (e) {
    console.error(`[periodic-cleanup] Could not read OUTPUT_DIR: ${e.message}`);
  }

  // 3. api_batches/ meta JSON files (one per batch, named batch_*.json)
  let metaDeleted = 0;
  let metaErrors = 0;
  try {
    if (fs.existsSync(API_BATCH_DIR)) {
      for (const entry of fs.readdirSync(API_BATCH_DIR)) {
        if (!/^batch_.+\.(json|txt|log)$/.test(entry)) continue;
        const fullPath = path.join(API_BATCH_DIR, entry);
        try {
          const stat = fs.statSync(fullPath);
          if (stat.isFile() && stat.mtimeMs < cutoff) {
            fs.unlinkSync(fullPath);
            metaDeleted++;
            console.log(`[periodic-cleanup] Deleted batch meta: ${entry}`);
          }
        } catch (e) {
          metaErrors++;
          console.error(`[periodic-cleanup] Error deleting meta ${entry}: ${e.message}`);
        }
      }
    }
  } catch (e) {
    console.error(`[periodic-cleanup] Could not read API_BATCH_DIR: ${e.message}`);
  }

  // 5. Old Chrome BINARY cache cleanup — removes stale previous-version
  // Chrome downloads (250-260MB each) left behind after Puppeteer
  // auto-updates. Safe on a schedule since it skips any binary an active
  // Chrome process is still running from.
  let chromeBinDeleted = 0;
  let chromeBinFreedMB = 0;
  let chromeBinErrors  = 0;
  try {
    const binCleanup = cleanupOldChromeBinaries();
    chromeBinDeleted = binCleanup.removed.length;
    chromeBinFreedMB = binCleanup.freedApproxMB;
    chromeBinErrors  = binCleanup.errors.length;
    if (chromeBinDeleted > 0 || chromeBinErrors > 0) {
      console.log(
        `[periodic-cleanup] Old Chrome binaries: ${chromeBinDeleted} removed ` +
        `(~${chromeBinFreedMB}MB freed), ${chromeBinErrors} errors`
      );
    }
  } catch (e) {
    console.error(`[periodic-cleanup] Chrome binary cleanup failed: ${e.message}`);
  }

  console.log(
    `[periodic-cleanup] Done — ` +
    `scan folders: ${batchDeleted} deleted (${batchErrors} err) | ` +
    `helper files: ${helperDeleted} deleted (${helperErrors} err) | ` +
    `batch meta: ${metaDeleted} deleted (${metaErrors} err) | ` +
    `chrome binaries: ${chromeBinDeleted} deleted (~${chromeBinFreedMB}MB, ${chromeBinErrors} err) | ` +
    `cutoff: ${PERIODIC_CLEANUP_HOURS}h`
  );
}

// Run once immediately at startup to clear any already-stale files,
// then repeat every hour.
runPeriodicCleanup();
setInterval(runPeriodicCleanup, PERIODIC_CLEANUP_INTERVAL_MS);
console.log(`[api] Periodic cleanup scheduled every 1h (cutoff: ${PERIODIC_CLEANUP_HOURS}h via PERIODIC_CLEANUP_HOURS env)`);