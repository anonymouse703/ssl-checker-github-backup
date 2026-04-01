"use strict";

const { loadEnv } = require("./config/env-loader");
loadEnv();

const http = require("http");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const crypto = require("crypto");
const { initPool, poolStats } = require("./utils/browser-pool");

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
const MAX_ACTIVE_RESERVED_DOMAINS = parseInt(
  process.env.MAX_ACTIVE_RESERVED_DOMAINS || "6",
  10
);

const activeJobs = new Map();

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
  process.exit(1);
});

function jsonResponse(res, statusCode, data) {
  const body = JSON.stringify(data);
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS, DELETE",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
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

function nowIso() {
  return new Date().toISOString();
}

function sanitizeDomain(raw) {
  let s = String(raw || "").trim().toLowerCase();

  s = s.replace(/^https?:\/\//i, "");
  s = s.replace(/^www\./i, "");

  s = s.split("/")[0];
  s = s.split("?")[0];
  s = s.split("#")[0];

  s = s.replace(/:\d+$/, "");   // remove :8080 if present
  s = s.replace(/\.+$/, "");    // remove trailing dots

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

function tryAcquireDomainLock(domain, pid) {
  const lockFile = domainLockPath(domain);
  try {
    fs.writeFileSync(lockFile, String(pid), { flag: "wx" });
    console.log(`[lock] Acquired lock for ${domain} (PID: ${pid})`);
    return true;
  } catch (e) {
    if (e.code !== "EEXIST") throw e;
    try {
      const ownerPid = parseInt(readFileOrNull(lockFile) || "0", 10);
      try {
        process.kill(ownerPid, 0);
        console.log(`[lock] Lock for ${domain} held by alive PID ${ownerPid}`);
        return false;
      } catch (_) {
        console.log(`[lock] Stale lock for ${domain} (PID ${ownerPid} dead), taking over`);
        fs.writeFileSync(lockFile, String(pid), { flag: "w" });
        return true;
      }
    } catch (_) {
      return false;
    }
  }
}

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

function currentReservedDomainCount() {
  let count = 0;
  for (const job of activeJobs.values()) {
    if (job.type === "single" && job.domain) count += 1;
    else if (job.type === "multi" && Array.isArray(job.domains)) {
      count += job.domains.length;
    }
  }
  return count;
}

function acquireBatchDomainLocks(domains, pid) {
  const acquired = [];
  const conflicts = [];
  for (const domain of domains) {
    try {
      if (tryAcquireDomainLock(domain, pid)) acquired.push(domain);
      else conflicts.push(domain);
    } catch (_) {
      conflicts.push(domain);
    }
  }
  if (conflicts.length) {
    for (const domain of acquired) releaseDomainLock(domain, pid);
  }
  return { acquired, conflicts };
}

function releaseBatchDomainLocks(domains, pid) {
  for (const domain of domains || []) releaseDomainLock(domain, pid);
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
  } catch (err) {
    console.error(`[api] Error scanning direct result files: ${err.message}`);
  }

  if (bestPath) {
    console.log(`[api] Best direct result file: ${bestPath}`);
  } else {
    console.log(`[api] No direct result file found for domain: ${domain}`);
  }

  return bestPath;
}

function findLatestDomainInBatchSummary(domain) {
  let bestMtime = 0;
  let bestPath = null;
  let bestRow = null;

  console.log(`[api] Looking for domain in batch summary.csv: ${domain}`);

  try {
    const entries = fs.readdirSync(SCAN_ROOT);
    for (const entry of entries) {
      if (!/^\d{4}-\d{2}-\d{2}/.test(entry)) continue;

      const batchPath = path.join(SCAN_ROOT, entry);
      const summaryPath = path.join(batchPath, "summary.csv");
      if (!fs.existsSync(summaryPath)) continue;

      const raw = readFileOrNull(summaryPath);
      if (!raw) continue;

      const rows = parseCSV(raw);
      const row = rows.find((r) => sanitizeDomain(r.Domain || "") === domain);
      if (!row) continue;

      const mtime = fs.statSync(summaryPath).mtimeMs;
      if (mtime > bestMtime) {
        bestMtime = mtime;
        bestPath = summaryPath;
        bestRow = row;
      }
    }
  } catch (err) {
    console.error(`[api] Error scanning batch summary CSVs: ${err.message}`);
  }

  if (bestPath) {
    console.log(`[api] Best batch summary hit: ${bestPath}`);
    return { path: bestPath, row: bestRow };
  }

  console.log(`[api] No batch summary hit found for domain: ${domain}`);
  return null;
}

function findDomainFromLatestPathFiles(domain) {
  let bestMtime = 0;
  let bestPath = null;
  let bestRow = null;

  console.log(`[api] Looking for domain via latest_path_*.txt: ${domain}`);

  try {
    const files = fs.readdirSync(OUTPUT_DIR).filter((f) => /^latest_path_.+\.txt$/.test(f));

    for (const file of files) {
      const fullPath = path.join(OUTPUT_DIR, file);

      let stat;
      try {
        stat = fs.statSync(fullPath);
      } catch (_) {
        continue;
      }

      const pointedPath = (readFileOrNull(fullPath) || "").trim();
      if (!pointedPath) continue;
      if (!fs.existsSync(pointedPath)) continue;

      const raw = readFileOrNull(pointedPath);
      if (!raw) continue;

      const rows = parseCSV(raw);
      const row = rows.find((r) => sanitizeDomain(r.Domain || "") === domain);
      if (!row) continue;

      if (stat.mtimeMs > bestMtime) {
        bestMtime = stat.mtimeMs;
        bestPath = pointedPath;
        bestRow = row;
      }
    }
  } catch (err) {
    console.error(`[api] Error scanning latest_path files: ${err.message}`);
  }

  if (bestPath) {
    console.log(`[api] Found via latest_path file: ${bestPath}`);
    return { path: bestPath, row: bestRow, source: "latest_path_file" };
  }

  console.log(`[api] No latest_path file hit found for domain: ${domain}`);
  return null;
}

function walkDirRecursive(rootDir, visitFile) {
  let entries = [];
  try {
    entries = fs.readdirSync(rootDir, { withFileTypes: true });
  } catch (_) {
    return;
  }

  for (const entry of entries) {
    const fullPath = path.join(rootDir, entry.name);
    if (entry.isDirectory()) {
      walkDirRecursive(fullPath, visitFile);
    } else if (entry.isFile()) {
      visitFile(fullPath);
    }
  }
}

function findDomainByRecursiveSearch(domain) {
  let bestMtime = 0;
  let bestPath = null;
  let bestRow = null;

  console.log(`[api] Looking for domain via recursive search: ${domain}`);

  try {
    walkDirRecursive(SCAN_ROOT, (fullPath) => {
      const base = path.basename(fullPath);
      const isCandidate =
        base === "summary.csv" ||
        base === `${domain}_results.csv`;

      if (!isCandidate) return;

      const raw = readFileOrNull(fullPath);
      if (!raw) return;

      const rows = parseCSV(raw);
      const row = rows.find((r) => sanitizeDomain(r.Domain || "") === domain);
      if (!row) return;

      let mtime = 0;
      try {
        mtime = fs.statSync(fullPath).mtimeMs;
      } catch (_) {
        return;
      }

      if (mtime > bestMtime) {
        bestMtime = mtime;
        bestPath = fullPath;
        bestRow = row;
      }
    });
  } catch (err) {
    console.error(`[api] Error in recursive search: ${err.message}`);
  }

  if (bestPath) {
    console.log(`[api] Found via recursive search: ${bestPath}`);
    return { path: bestPath, row: bestRow, source: "recursive_search" };
  }

  console.log(`[api] No recursive search hit found for domain: ${domain}`);
  return null;
}

function findNewestBatchPathAfter(startedAtIso) {
  const startedAtMs = new Date(startedAtIso).getTime();
  let best = null;
  let bestMtime = 0;

  try {
    const entries = fs.readdirSync(SCAN_ROOT);
    for (const entry of entries) {
      if (!/^\d{4}-\d{2}-\d{2}/.test(entry)) continue;
      const batchPath = path.join(SCAN_ROOT, entry);
      const statsPath = path.join(batchPath, "_batch_stats.json");
      const summaryPath = path.join(batchPath, "summary.csv");
      if (!fs.existsSync(statsPath) && !fs.existsSync(summaryPath)) continue;

      let mtime = 0;
      if (fs.existsSync(statsPath)) {
        mtime = Math.max(mtime, fs.statSync(statsPath).mtimeMs);
      }
      if (fs.existsSync(summaryPath)) {
        mtime = Math.max(mtime, fs.statSync(summaryPath).mtimeMs);
      }

      if (mtime >= startedAtMs && mtime > bestMtime) {
        bestMtime = mtime;
        best = batchPath;
      }
    }
  } catch (_) {}

  return best;
}

function tailFile(filePath, maxLines) {
  return (readFileOrNull(filePath) || "")
    .split("\n")
    .filter((l) => l.trim())
    .slice(-maxLines)
    .join("\n");
}

// ── Cleanup helper files older than X hours ──────────────────────────────────

function cleanupOldApiHelperFiles(maxAgeHours = 24) {
  const cutoff = Date.now() - maxAgeHours * 60 * 60 * 1000;
  const deleted = [];
  const skipped = [];
  const errors = [];

  const activeJobIds = new Set(
    [...activeJobs.values()].map((j) => j.jobId).filter(Boolean)
  );

  function extractJobId(file) {
    let m = file.match(/^done_(.+)\.flag$/);
    if (m) return m[1];
    m = file.match(/^latest_path_(.+)\.txt$/);
    if (m) return m[1];
    m = file.match(/^progress_(.+)\.txt$/);
    if (m) return m[1];
    m = file.match(/^progress_(.+)\.log$/);
    if (m) return m[1];
    return null;
  }

  try {
    const files = fs.readdirSync(OUTPUT_DIR);

    for (const file of files) {
      const jobId = extractJobId(file);
      if (!jobId) continue;

      if (activeJobIds.has(jobId)) {
        skipped.push(file);
        continue;
      }

      const fullPath = path.join(OUTPUT_DIR, file);

      try {
        const stat = fs.statSync(fullPath);
        if (!stat.isFile()) continue;

        if (stat.mtimeMs < cutoff) {
          fs.unlinkSync(fullPath);
          deleted.push(file);
        } else {
          skipped.push(file);
        }
      } catch (e) {
        errors.push({ file, error: e.message });
      }
    }
  } catch (e) {
    return {
      ok: false,
      error: e.message,
      deleted,
      skipped,
      errors,
    };
  }

  return {
    ok: true,
    deleted,
    skipped,
    errors,
    cutoff_iso: new Date(cutoff).toISOString(),
  };
}

// ── Body parser ───────────────────────────────────────────────────────────────

function parseBodyJson(req, callback) {
  let body = "";
  req.on("data", (chunk) => {
    body += chunk;
  });
  req.on("end", () => {
    let parsed;
    try {
      parsed = JSON.parse(body || "{}");
    } catch (_) {
      return callback(new Error("Invalid JSON body"));
    }
    callback(null, parsed);
  });
}

const PUPPETEER_PATH =
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  "/usr/local/ind_leads/ssl-checker-tool/chrome/linux-145.0.7632.67/chrome-linux64/chrome";

// ── Cleanup hooks ─────────────────────────────────────────────────────────────

function cleanupServerOwnedLocks() {
  console.log("[api] Cleaning up domain locks owned by this server...");
  for (const job of activeJobs.values()) {
    if (job.type === "single" && job.domain) {
      releaseDomainLock(job.domain, process.pid);
    } else if (job.type === "multi") {
      releaseBatchDomainLocks(job.domains, process.pid);
    }
  }
}

process.on("exit", cleanupServerOwnedLocks);
process.on("SIGINT", () => {
  cleanupServerOwnedLocks();
  process.exit(130);
});
process.on("SIGTERM", () => {
  cleanupServerOwnedLocks();
  process.exit(143);
});

const STALE_JOB_MAX_AGE_MS =
  parseInt(process.env.STALE_JOB_MAX_AGE_HOURS || "2", 10) *
  60 * 60 * 1000;

function purgeStaleJobs(maxAgeMs) {
  const now = Date.now();
  const killed = [];

  for (const [key, job] of activeJobs.entries()) {
    const age = now - new Date(job.startedAt).getTime();
    if (age < maxAgeMs) continue;

    console.log(
      `[stale] Job ${key} started ${job.startedAt} (${Math.round(age / 60000)} min ago) — killing PID ${job.pid}`
    );

    try {
      process.kill(job.pid, "SIGKILL");
      console.log(`[stale] Killed PID ${job.pid} for job ${key}`);
    } catch (e) {
      console.log(`[stale] PID ${job.pid} already gone (${e.code})`);
    }

    if (job.type === "single" && job.domain) {
      releaseDomainLock(job.domain, job.pid);
    } else if (job.type === "multi" && Array.isArray(job.domains)) {
      releaseBatchDomainLocks(job.domains, job.pid);
    }

    activeJobs.delete(key);

    killed.push({
      key,
      domain: job.domain || null,
      batchId: job.batchId || null,
      jobId: job.jobId,
      pid: job.pid,
      startedAt: job.startedAt,
      ageMinutes: Math.round(age / 60000),
    });
  }

  if (killed.length > 0) {
    console.log(`[stale] Purged ${killed.length} stale job(s)`);
  }

  return { killed };
}

const STALE_CLEANUP_INTERVAL_MS = 30 * 60 * 1000;
setInterval(() => {
  console.log("[stale] Running scheduled stale job cleanup...");
  purgeStaleJobs(STALE_JOB_MAX_AGE_MS);
}, STALE_CLEANUP_INTERVAL_MS).unref();

const API_HELPER_CLEANUP_INTERVAL_MS = 60 * 60 * 1000;
setInterval(() => {
  const result = cleanupOldApiHelperFiles(24);
  if (result.ok && result.deleted.length > 0) {
    console.log(`[cleanup] deleted ${result.deleted.length} old API helper files older than 24h`);
  }
}, API_HELPER_CLEANUP_INTERVAL_MS).unref();

// ── Router ────────────────────────────────────────────────────────────────────

function handleRequest(req, res) {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const method = req.method.toUpperCase();

  if (method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers": "Content-Type",
    });
    return res.end();
  }

  // Health check
  if (method === "GET" && url.pathname === "/health") {
    const jobs = [...activeJobs.values()].map((j) => ({
      key: j.key,
      type: j.type,
      domain: j.domain || null,
      batchId: j.batchId || null,
      pid: j.pid,
      startedAt: j.startedAt,
      jobId: j.jobId || null,
    }));

    return jsonResponse(res, 200, {
      ok: true,
      server: "ssl-checker-tool api",
      uptime: process.uptime(),
      active_jobs: jobs,
      total_active: jobs.length,
      reserved_domains: currentReservedDomainCount(),
      max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
      timestamp: nowIso(),
      pool_stats: poolStats(),
    });
  }

  // Single domain scan
  if (method === "POST" && url.pathname === "/scan") {
    return parseBodyJson(req, (err, parsed) => {
      if (err) return jsonResponse(res, 400, { ok: false, error: err.message });

      const domain = sanitizeDomain(parsed.domain);
      if (!domain) {
        return jsonResponse(res, 400, {
          ok: false,
          error: "domain is required",
        });
      }

      const jobKey = `single:${domain}`;

      if (activeJobs.has(jobKey)) {
        const existing = activeJobs.get(jobKey);
        const alreadyDone = fs.existsSync(existing.doneFlag);

        if (alreadyDone) {
          console.log(`[api] Job for ${domain} is done, clearing from activeJobs to allow re-scan`);
          releaseDomainLock(domain, existing.pid);
          activeJobs.delete(jobKey);
        } else {
          try {
            process.kill(existing.pid, 0);
            return jsonResponse(res, 200, {
              ok: false,
              error: `Scan already running for ${domain}. Poll /status?domain=${domain} for progress.`,
              job_id: existing.jobId,
              started_at: existing.startedAt,
              pid: existing.pid,
            });
          } catch (_) {
            console.log(`[api] Cleaning up crashed job for ${domain} (PID ${existing.pid})`);
            releaseDomainLock(domain, existing.pid);
            activeJobs.delete(jobKey);
          }
        }
      }

      const lockAcquired = tryAcquireDomainLock(domain, process.pid);
      if (!lockAcquired) {
        return jsonResponse(res, 200, {
          ok: false,
          error: `${domain} is locked by another process (possibly from another API server instance).`,
        });
      }

      const jobId = createJobId(domain);
      const startedAt = nowIso();

      initJobProgressFile(jobId, domain, 1);

      const child = spawn("node", ["index.js", domain], {
        cwd: TOOL_DIR,
        detached: false,
        stdio: ["ignore", "pipe", "pipe"],
        env: {
          ...process.env,
          PUPPETEER_EXECUTABLE_PATH: PUPPETEER_PATH,
          JOB_ID: jobId,
          DOMAIN_LOCK_ALREADY_HELD: "1",
          BROWSER_POOL_SKIP: "1",
        },
      });

      try {
        fs.writeFileSync(domainLockPath(domain), String(child.pid), { flag: "w" });
      } catch (_) {}

      const jobEntry = {
        key: jobKey,
        type: "single",
        domain,
        jobId,
        pid: child.pid,
        startedAt,
        progressFile: progressFilePath(jobId),
        logFile: logFilePath(jobId),
        doneFlag: doneFlagPath(jobId),
      };
      activeJobs.set(jobKey, jobEntry);

      console.log(`[api] single scan started: ${domain} (pid ${child.pid}) jobId=${jobId}`);

      const logStream = fs.createWriteStream(logFilePath(jobId), { flags: "a" });
      child.stdout.pipe(logStream);
      child.stderr.on("data", (data) => logStream.write("[stderr] " + String(data)));

      child.on("close", (code) => {
        logStream.end();
        console.log(`[api] single scan finished: ${domain} exit=${code} jobId=${jobId}`);
        writeTextSafe(doneFlagPath(jobId), `exit=${code}\nfinishedAt=${nowIso()}`);
        releaseDomainLock(domain, child.pid);
        activeJobs.delete(jobKey);
      });

      child.on("error", (err) => {
        logStream.end();
        console.error(`[api] single scan error for ${domain}: ${err.message}`);
        writeTextSafe(doneFlagPath(jobId), `exit=error\nerror=${err.message}\nfinishedAt=${nowIso()}`);
        releaseDomainLock(domain, child.pid);
        activeJobs.delete(jobKey);
      });

      return jsonResponse(res, 200, {
        ok: true,
        queued: true,
        mode: "single",
        domain,
        job_id: jobId,
        pid: child.pid,
        started_at: startedAt,
        pool_stats: poolStats(),
      });
    });
  }

  // Single domain status
  if (method === "GET" && url.pathname === "/status") {
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");
    if (!domain) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "domain parameter required",
      });
    }

    const jobKey = `single:${domain}`;
    let jobId = null;
    let progress = null;
    let isDone = false;

    if (activeJobs.has(jobKey)) {
      const job = activeJobs.get(jobKey);
      jobId = job.jobId;
      progress = readProgress(jobId);
      isDone = fs.existsSync(job.doneFlag);
    } else {
      try {
        const safeDomain = domain.replace(/[^a-z0-9._-]/gi, "_");
        const files = fs
          .readdirSync(OUTPUT_DIR)
          .filter((f) => f.startsWith(`progress_${safeDomain}`) && f.endsWith(".txt"))
          .map((f) => ({
            f,
            mtime: fs.statSync(path.join(OUTPUT_DIR, f)).mtimeMs,
          }))
          .sort((a, b) => b.mtime - a.mtime);

        if (files.length > 0) {
          const fname = files[0].f;
          jobId = fname.replace(/^progress_/, "").replace(/\.txt$/, "");
          progress = readProgress(jobId);
          isDone = fs.existsSync(doneFlagPath(jobId));
        }
      } catch (_) {}
    }

    let pct = 0;
    let statusText = "Unknown";
    let isRunning = false;

    if (progress) {
      const completed = parseInt(progress.completed || "0", 10);
      const total = parseInt(progress.total || "1", 10);
      pct = total > 0 ? Math.round((completed / total) * 100) : 0;
      statusText = progress.status || "Running...";

      if (isDone) {
        statusText = "Audit complete";
        isRunning = false;
      } else if (activeJobs.has(jobKey)) {
        isRunning = true;
        try {
          process.kill(activeJobs.get(jobKey).pid, 0);
        } catch (_) {
          console.log(`[api] Cleaning up stale job entry for ${domain}`);
          activeJobs.delete(jobKey);
          isRunning = false;
          statusText = "Scan was interrupted";
        }
      }
    }

    const logTail = jobId ? tailFile(logFilePath(jobId), 50) : "";

    return jsonResponse(res, 200, {
      ok: true,
      mode: "single",
      domain,
      job_id: jobId,
      done: isDone,
      running: isRunning,
      pct,
      status: statusText,
      progress: progress || {},
      log_tail: logTail,
    });
  }

  // Single domain result
  if (method === "GET" && url.pathname === "/result") {
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");
    if (!domain) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "domain parameter required",
      });
    }

    const csvPath = findLatestDomainCSV(domain);
    if (csvPath) {
      const raw = readFileOrNull(csvPath);
      if (raw) {
        const rows = parseCSV(raw);
        const row =
          rows.find((r) => sanitizeDomain(r.Domain || "") === domain) ||
          rows[rows.length - 1];

        if (row) {
          return jsonResponse(res, 200, {
            ok: true,
            mode: "single",
            source: csvPath.endsWith("/summary.csv") ? "domain_summary_csv" : "domain_csv",
            path: csvPath,
            data: row,
          });
        }
      }
    }

    const summaryHit = findLatestDomainInBatchSummary(domain);
    if (summaryHit) {
      return jsonResponse(res, 200, {
        ok: true,
        mode: "single",
        source: "batch_summary_csv",
        path: summaryHit.path,
        data: summaryHit.row,
      });
    }

    const latestPathHit = findDomainFromLatestPathFiles(domain);
    if (latestPathHit) {
      return jsonResponse(res, 200, {
        ok: true,
        mode: "single",
        source: latestPathHit.source,
        path: latestPathHit.path,
        data: latestPathHit.row,
      });
    }

    const recursiveHit = findDomainByRecursiveSearch(domain);
    if (recursiveHit) {
      return jsonResponse(res, 200, {
        ok: true,
        mode: "single",
        source: recursiveHit.source,
        path: recursiveHit.path,
        data: recursiveHit.row,
      });
    }

    return jsonResponse(res, 404, {
      ok: false,
      error: `No result found for domain: ${domain}`,
      checked_path: csvPath || null,
    });
  }

  // Multi-scan
  if (method === "POST" && url.pathname === "/multi-scan") {
    return parseBodyJson(req, (err, parsed) => {
      if (err) return jsonResponse(res, 400, { ok: false, error: err.message });

      const domains = uniqueDomains(parsed.domains || []);
      if (!Array.isArray(parsed.domains) || domains.length === 0) {
        return jsonResponse(res, 400, {
          ok: false,
          error: "domains array is required",
        });
      }

      const conflictingInMemory = domains.filter((d) => activeJobs.has(`single:${d}`));
      if (conflictingInMemory.length > 0) {
        return jsonResponse(res, 200, {
          ok: false,
          error: `These domains are already being scanned: ${conflictingInMemory.join(", ")}.`,
          conflicting_domains: conflictingInMemory,
        });
      }

      const reservedNow = currentReservedDomainCount();
      if (reservedNow + domains.length > MAX_ACTIVE_RESERVED_DOMAINS) {
        return jsonResponse(res, 429, {
          ok: false,
          error: `Server is busy. This batch needs ${domains.length} slots, but only ${Math.max(
            0,
            MAX_ACTIVE_RESERVED_DOMAINS - reservedNow
          )} of ${MAX_ACTIVE_RESERVED_DOMAINS} are available.`,
          requested_domains: domains.length,
          reserved_domains: reservedNow,
          max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
        });
      }

      const { conflicts: lockedConflicts } = acquireBatchDomainLocks(domains, process.pid);
      if (lockedConflicts.length > 0) {
        return jsonResponse(res, 200, {
          ok: false,
          error: `These domains are locked by another process: ${lockedConflicts.join(", ")}.`,
          conflicting_domains: lockedConflicts,
        });
      }

      const batchId = createBatchId();
      const jobId = batchId;
      const startedAt = nowIso();
      const batchLogPath = path.join(API_BATCH_DIR, `${batchId}.log`);

      initJobProgressFile(jobId, domains[0], domains.length);
      writeTextSafe(batchLogPath, "");

      const meta = {
        batchId,
        jobId,
        mode: "multi",
        status: "RUNNING",
        startedAt,
        finishedAt: "",
        domains,
        total: domains.length,
        batchFolder: "",
        batchPath: "",
        summaryCsvPath: "",
        statsJsonPath: "",
        logPath: batchLogPath,
        pid: null,
      };
      saveBatchMeta(meta);

      const child = spawn("node", ["multi-audit.js", ...domains], {
        cwd: TOOL_DIR,
        detached: false,
        stdio: ["ignore", "pipe", "pipe"],
        env: {
          ...process.env,
          PUPPETEER_EXECUTABLE_PATH: PUPPETEER_PATH,
          JOB_ID: jobId,
          DOMAIN_LOCK_ALREADY_HELD: "1",
        },
      });

      const jobKey = `multi:${batchId}`;
      const jobEntry = {
        key: jobKey,
        type: "multi",
        batchId,
        jobId,
        pid: child.pid,
        startedAt,
        domains,
        activeDomains: [],
        batchFolder: "",
        batchPath: "",
        logPath: batchLogPath,
        progressFile: progressFilePath(jobId),
        doneFlag: doneFlagPath(jobId),
      };
      activeJobs.set(jobKey, jobEntry);

      meta.pid = child.pid;
      saveBatchMeta(meta);

      function processBatchOutput(chunk, isErr) {
        const text = String(chunk || "");
        appendTextSafe(batchLogPath, isErr ? "[stderr] " + text : text);

        for (const line of text.split("\n")) {
          const t = line.trim();
          if (!t) continue;

          let m = t.match(/^__BATCH_FOLDER__:(.+)$/);
          if (m && activeJobs.has(jobKey)) {
            const entry = activeJobs.get(jobKey);
            entry.batchFolder = m[1].trim();
            entry.batchPath = path.join(SCAN_ROOT, entry.batchFolder);
          }

          m = t.match(/^__DOMAIN_START__:(.+?):(\d+):(.*)$/);
          if (m && activeJobs.has(jobKey)) {
            const entry = activeJobs.get(jobKey);
            const domain = m[1].trim();
            if (!entry.activeDomains.includes(domain)) {
              entry.activeDomains.push(domain);
            }
          }

          m = t.match(/^__DOMAIN_DONE__:(.+?):(\d+)$/);
          if (m && activeJobs.has(jobKey)) {
            const entry = activeJobs.get(jobKey);
            entry.activeDomains = entry.activeDomains.filter((d) => d !== m[1].trim());
          }
        }
      }

      child.stdout.on("data", (data) => processBatchOutput(data, false));
      child.stderr.on("data", (data) => processBatchOutput(data, true));

      child.on("close", (code) => {
        const entry = activeJobs.get(jobKey);
        const currentMeta = getBatchMeta(batchId) || meta;
        const guessedBatchPath =
          (entry && entry.batchPath) ||
          currentMeta.batchPath ||
          findNewestBatchPathAfter(startedAt) ||
          "";

        const guessedBatchFolder = guessedBatchPath
          ? path.basename(guessedBatchPath)
          : currentMeta.batchFolder || "";

        const summaryCsvPath = guessedBatchPath
          ? path.join(guessedBatchPath, "summary.csv")
          : "";
        const statsJsonPath = guessedBatchPath
          ? path.join(guessedBatchPath, "_batch_stats.json")
          : "";

        saveBatchMeta({
          ...currentMeta,
          status: code === 0 ? "DONE" : "FAILED",
          finishedAt: nowIso(),
          batchFolder: guessedBatchFolder,
          batchPath: guessedBatchPath,
          summaryCsvPath,
          statsJsonPath,
          pid: child.pid,
        });

        releaseBatchDomainLocks(domains, process.pid);
        activeJobs.delete(jobKey);
      });

      child.on("error", () => {
        releaseBatchDomainLocks(domains, process.pid);
        activeJobs.delete(jobKey);
      });

      return jsonResponse(res, 200, {
        ok: true,
        queued: true,
        mode: "multi",
        batch_id: batchId,
        job_id: jobId,
        total: domains.length,
        started_at: startedAt,
        pid: child.pid,
      });
    });
  }

  // Multi-scan status
  if (method === "GET" && url.pathname === "/multi-status") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "batch_id parameter required",
      });
    }

    const meta = getBatchMeta(batchId);
    if (!meta) {
      return jsonResponse(res, 404, { ok: false, error: "Unknown batch_id" });
    }

    const jobKey = `multi:${batchId}`;
    const jobId = meta.jobId || batchId;
    const progress = readProgress(jobId);
    const logTail = tailFile(meta.logPath || path.join(API_BATCH_DIR, `${batchId}.log`), 60);

    if (activeJobs.has(jobKey)) {
      const entry = activeJobs.get(jobKey);
      const completed = parseInt(progress?.completed || "0", 10);
      const total = parseInt(progress?.total || String(meta.total || 0), 10);
      const pct = total > 0 ? Math.round((completed / total) * 100) : 0;

      return jsonResponse(res, 200, {
        ok: true,
        mode: "multi",
        batch_id: batchId,
        job_id: jobId,
        done: false,
        running: true,
        status: progress?.status || "RUNNING",
        pct,
        completed,
        total,
        batch_folder: entry.batchFolder || meta.batchFolder || "",
        batch_path: entry.batchPath || meta.batchPath || "",
        log_tail: logTail,
      });
    }

    const batchPath = meta.batchPath || findNewestBatchPathAfter(meta.startedAt) || "";
    const statsPath = meta.statsJsonPath || (batchPath ? path.join(batchPath, "_batch_stats.json") : "");
    const summaryPath = meta.summaryCsvPath || (batchPath ? path.join(batchPath, "summary.csv") : "");
    const stats = statsPath ? readJsonOrNull(statsPath) : null;

    let total = meta.total || 0;
    let completed = total;
    let failed = 0;
    let pct = total > 0 ? 100 : 0;

    if (stats) {
      total = stats.total || total;
      completed = (stats.successful || 0) + (stats.failed || 0);
      failed = stats.failed || 0;
      pct = total > 0 ? Math.round((completed / total) * 100) : 100;
    }

    return jsonResponse(res, 200, {
      ok: true,
      mode: "multi",
      batch_id: batchId,
      job_id: jobId,
      done: true,
      running: false,
      status: meta.status || "DONE",
      pct,
      total,
      completed,
      failed,
      batch_folder: batchPath ? path.basename(batchPath) : meta.batchFolder || "",
      batch_path: batchPath,
      summary_csv: summaryPath || "",
      stats_json: statsPath || "",
      log_tail: logTail,
    });
  }

  // Multi-scan result
  if (method === "GET" && url.pathname === "/multi-result") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "batch_id parameter required",
      });
    }

    const meta = getBatchMeta(batchId);
    if (!meta) {
      return jsonResponse(res, 404, { ok: false, error: "Unknown batch_id" });
    }

    const batchPath = meta.batchPath || findNewestBatchPathAfter(meta.startedAt) || "";
    const summaryPath = meta.summaryCsvPath || (batchPath ? path.join(batchPath, "summary.csv") : "");
    const statsPath = meta.statsJsonPath || (batchPath ? path.join(batchPath, "_batch_stats.json") : "");

    if (!summaryPath || !fs.existsSync(summaryPath)) {
      return jsonResponse(res, 404, {
        ok: false,
        error: "Batch summary.csv not found yet",
        batch_id: batchId,
      });
    }

    const raw = readFileOrNull(summaryPath);
    if (!raw) {
      return jsonResponse(res, 404, {
        ok: false,
        error: "Batch summary.csv is empty or unreadable",
        batch_id: batchId,
      });
    }

    const rows = parseCSV(raw);
    const stats = statsPath ? readJsonOrNull(statsPath) : null;

    return jsonResponse(res, 200, {
      ok: true,
      mode: "multi",
      batch_id: batchId,
      batch_path: batchPath,
      summary_csv: summaryPath,
      total_results: rows.length,
      stats: stats || null,
      results: rows,
    });
  }

  // Multi-scan log
  if (method === "GET" && url.pathname === "/multi-log") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "batch_id parameter required",
      });
    }

    const meta = getBatchMeta(batchId);
    if (!meta) {
      return jsonResponse(res, 404, { ok: false, error: "Unknown batch_id" });
    }

    const log = readFileOrNull(meta.logPath) || "(no batch log yet)";
    res.writeHead(200, {
      "Content-Type": "text/plain; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    });
    return res.end(log);
  }

  // Single log
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

  // Serve tester HTML files
  if (method === "GET" && url.pathname === "/tester") {
    try {
      const html = fs.readFileSync(path.join(TOOL_DIR, "api-tester.html"), "utf8");
      res.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8",
        "Access-Control-Allow-Origin": "*",
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
      });
      return res.end(html);
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: "Could not load api-tester-multi.html: " + e.message });
    }
  }

  // Cleanup API helper files
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

  // Cleanup stale jobs
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

  // List all jobs
  if (method === "GET" && url.pathname === "/jobs") {
    return jsonResponse(res, 200, {
      ok: true,
      reserved_domains: currentReservedDomainCount(),
      max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
      jobs: [...activeJobs.values()].map((j) => ({
        key: j.key,
        type: j.type,
        domain: j.domain || null,
        batchId: j.batchId || null,
        jobId: j.jobId,
        pid: j.pid,
        startedAt: j.startedAt,
      })),
      pool_stats: poolStats(),
    });
  }

  // Cleanup old batch folders (keep latest X days)
  if (method === "GET" && url.pathname === "/cleanup-batches") {
    const days = parseInt(url.searchParams.get("days") || "7", 10);
    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    const deleted = [];
    const errors = [];

    try {
      const entries = fs.readdirSync(SCAN_ROOT);
      for (const entry of entries) {
        if (!/^\d{4}-\d{2}-\d{2}_/.test(entry)) continue;
        
        const fullPath = path.join(SCAN_ROOT, entry);
        try {
          const stat = fs.statSync(fullPath);
          if (stat.isDirectory() && stat.mtimeMs < cutoff) {
            const folderDate = entry.split('_')[0];
            const now = new Date();
            const isCurrentDay = folderDate === `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
            
            if (!isCurrentDay) {
              fs.rmSync(fullPath, { recursive: true, force: true });
              deleted.push(entry);
              console.log(`[cleanup] Deleted old batch: ${entry}`);
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
      errors, 
      days,
      message: `Deleted ${deleted.length} batch folders older than ${days} days`
    });
  }

  // Latest results - CSV or JSON
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

  // Get specific domain from latest
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

  // Get stats from latest
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

  // Cleanup old scan folders (backward compatibility)
  if (method === "GET" && url.pathname === "/cleanup") {
    const days = parseInt(url.searchParams.get("days") || "7", 10);
    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    const deleted = [];
    const errors = [];

    try {
      const entries = fs.readdirSync(SCAN_ROOT);
      for (const entry of entries) {
        if (!/^\d{4}-\d{2}-\d{2}/.test(entry)) continue;
        const fullPath = path.join(SCAN_ROOT, entry);
        try {
          const stat = fs.statSync(fullPath);
          if (stat.isDirectory() && stat.mtimeMs < cutoff) {
            fs.rmSync(fullPath, { recursive: true, force: true });
            deleted.push(entry);
          }
        } catch (e) {
          errors.push({ entry, error: e.message });
        }
      }
    } catch (e) {
      return jsonResponse(res, 500, { ok: false, error: e.message });
    }

    return jsonResponse(res, 200, { ok: true, deleted, errors, days });
  }

  return jsonResponse(res, 404, {
    ok: false,
    error: `Unknown endpoint: ${url.pathname}`,
  });
}

// ── Start server ──────────────────────────────────────────────────────────────

const server = http.createServer(handleRequest);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[api] ssl-checker-tool API server running on port ${PORT}`);
  console.log(`[api] TOOL_DIR=${TOOL_DIR}`);
  console.log(`[api] OUTPUT_DIR=${OUTPUT_DIR}`);
  console.log(`[api] SCAN_ROOT=${SCAN_ROOT}`);
});

server.on("error", (err) => {
  console.error(`[api] server error: ${err.message}`);
  process.exit(1);
});