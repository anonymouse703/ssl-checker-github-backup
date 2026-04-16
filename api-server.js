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

// ── Domain lock helpers (check-only) ─────────────────────────────────────────

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
    else if (job.type === "multi" && Array.isArray(job.domains)) {
      count += job.domains.length;
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

function findSingleResult(domain) {
  const csvPath = findLatestDomainCSV(domain);
  if (!csvPath || !fs.existsSync(csvPath)) return null;

  const rows = parseCSV(fs.readFileSync(csvPath, "utf8"));
  if (!rows.length) return null;

  return {
    csv_path: csvPath,
    data: rows[rows.length - 1],
  };
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
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 2 * 1024 * 1024) {
        reject(new Error("Request body too large"));
        req.destroy();
      }
    });
    req.on("end", () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch (e) {
        reject(new Error("Invalid JSON body"));
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

  // Health
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

  // Single scan

  if (method === "POST" && url.pathname === "/scan") {
    let parsed;
    try {
      parsed = await readJsonBody(req);
    } catch (e) {
      return jsonResponse(res, 400, { ok: false, error: e.message });
    }

    const domain = sanitizeDomain(parsed.domain);
    const forceRescan = !!parsed.force_rescan;
    const selectedTools = normalizeSelectedTools(parsed.tools);
    const enabledTools = selectedTools.length ? selectedTools : ALL_TOOL_KEYS;

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

    // Always clear any stale lock file before spawning the child.
    // At this point activeJobs confirmed no scan is running for this domain,
    // so any leftover lock file is from a previous crashed/finished child and
    // must be removed — otherwise the new child's acquireDomainLock() will fail.
    try {
      const lockFile = domainLockPath(domain);
      if (fs.existsSync(lockFile)) {
        fs.unlinkSync(lockFile);
        console.log(`[api] Cleared stale lock file for ${domain} before spawn`);
      }
    } catch (_) {}

    const child = spawn("node", ["index.js", domain], {
      cwd: TOOL_DIR,
      env: {
        ...process.env,
        PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH,
        JOB_ID: jobId,
        BROWSER_POOL_SKIP: "1",
        ENABLED_TOOLS: JSON.stringify(enabledTools),
        FORCE_RESCAN: forceRescan ? "1" : "0",
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

      // Release the domain lock so the same domain can be re-scanned immediately.
      releaseDomainLock(domain, child.pid);

      activeJobs.delete(jobKey);
    });

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

  // Batch scan
  if (method === "POST" && url.pathname === "/multi-scan") {
    let parsed;
    try {
      parsed = await readJsonBody(req);
    } catch (e) {
      return jsonResponse(res, 400, { ok: false, error: e.message });
    }

    const domains = uniqueDomains(parsed.domains || []);
    const forceRescan = !!parsed.force_rescan;
    const selectedTools = normalizeSelectedTools(parsed.tools);
    const enabledTools = selectedTools.length ? selectedTools : ALL_TOOL_KEYS;

    if (!domains.length) {
      return jsonResponse(res, 400, { ok: false, error: "No domains provided" });
    }

    const batchId = createBatchId();
    const startedAt = nowIso();
    const batchKey = `multi:${batchId}`;

    if (currentReservedDomainCount() + domains.length > MAX_ACTIVE_RESERVED_DOMAINS) {
      return jsonResponse(res, 429, {
        ok: false,
        error: "Max active reserved domains would be exceeded by this batch.",
        requested_domains: domains.length,
        reserved_domains: currentReservedDomainCount(),
        max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
      });
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

    const meta = {
      batchId,
      startedAt,
      status: "QUEUED",
      domains,
      tools: enabledTools,
      childPid: null,
      progressFile: progressFilePath(batchId),
      logFile: logFilePath(batchId),
    };
    saveBatchMeta(meta);
    initJobProgressFile(batchId, "", domains.length);

    console.log(`[api] Starting multi-scan batch_id=${batchId} domains=${domains.length}`);

    const child = spawn("node", ["multi-audit.js", ...domains], {
      cwd: TOOL_DIR,
      env: {
        ...process.env,
        PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH,
        SCAN_BATCH_ID: batchId,
        JOB_ID: batchId,
        ENABLED_TOOLS: JSON.stringify(enabledTools),
        FORCE_RESCAN: forceRescan ? "1" : "0",
      },
      detached: false,
      stdio: ["ignore", "pipe", "pipe"],
    });

    meta.childPid = child.pid;
    meta.status = "RUNNING";
    saveBatchMeta(meta);

    const logFile = logFilePath(batchId);
    child.stdout.on("data", (chunk) => appendTextSafe(logFile, chunk.toString()));
    child.stderr.on("data", (chunk) => appendTextSafe(logFile, chunk.toString()));

    activeJobs.set(batchKey, {
      key: batchKey,
      type: "multi",
      batchId,
      domains,
      tools: enabledTools,
      jobId: batchId,
      pid: child.pid,
      startedAt,
      logFile,
      forceRescan,
    });

    child.on("exit", (code, signal) => {
      console.log(`[api] Multi-scan exited batch_id=${batchId}. code=${code} signal=${signal}`);
      activeJobs.delete(batchKey);

      const saved = getBatchMeta(batchId) || meta;
      saved.status = code === 0 ? "DONE" : "ERROR";
      saved.exitCode = code;
      saved.signal = signal;
      saved.finishedAt = nowIso();
      saveBatchMeta(saved);
    });

    return jsonResponse(res, 200, {
      ok: true,
      queued: true,
      mode: "multi",
      batch_id: batchId,
      domains,
      tools: enabledTools,
      force_rescan: forceRescan,
      count: domains.length,
      pid: child.pid,
      started_at: startedAt,
      reserved_domains: currentReservedDomainCount(),
      max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
      pool_stats: poolStats(),
    });
  }

  // Single progress
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

  // Batch progress
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

    return jsonResponse(res, 200, {
      ok: true,
      batch_id: batchId,
      meta,
      progress,
    });
  }

  // Single result
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

    const found = findSingleResult(resolvedDomain);
    if (!found) {
      return jsonResponse(res, 404, {
        ok: false,
        error: `No result found yet for ${resolvedDomain}`,
      });
    }

    return jsonResponse(res, 200, {
      ok: true,
      domain: resolvedDomain,
      csv_path: found.csv_path,
      data: found.data,
    });
  }

  // Batch result
  if (method === "GET" && url.pathname === "/multi-result") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing batch_id" });
    }

    const meta = getBatchMeta(batchId);
    if (!meta) {
      return jsonResponse(res, 404, { ok: false, error: "Batch not found" });
    }

    let batchRoot = null;
    try {
      const latestPath = readFileOrNull(latestPathFile(batchId));
      if (latestPath) {
        batchRoot = path.dirname(path.dirname(latestPath.trim()));
      }
    } catch (_) {}

    if (!batchRoot) {
      try {
        const entries = fs.readdirSync(SCAN_ROOT);
        let bestMtime = 0;
        for (const entry of entries) {
          if (!/^\d{4}-\d{2}-\d{2}/.test(entry)) continue;
          const full = path.join(SCAN_ROOT, entry);
          const stat = fs.statSync(full);
          if (stat.isDirectory() && stat.mtimeMs > bestMtime) {
            bestMtime = stat.mtimeMs;
            batchRoot = full;
          }
        }
      } catch (_) {}
    }

    const csvPath = findBatchCSV(batchRoot);
    if (!csvPath || !fs.existsSync(csvPath)) {
      return jsonResponse(res, 404, {
        ok: false,
        error: "No batch CSV found yet",
        batch_root: batchRoot,
      });
    }

    const rows = parseCSV(fs.readFileSync(csvPath, "utf8"));
    return jsonResponse(res, 200, {
      ok: true,
      batch_id: batchId,
      batch_root: batchRoot,
      csv_path: csvPath,
      count: rows.length,
      data: rows,
      tools: meta.tools || [],
    });
  }

  // Single stop
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

  // Batch stop
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

  // Single status
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

  // Batch status
  if (method === "GET" && url.pathname === "/multi-status") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing batch_id" });
    }

    const key = `multi:${batchId}`;
    const job = activeJobs.get(key) || null;
    const meta = getBatchMeta(batchId) || null;

    return jsonResponse(res, 200, {
      ok: true,
      running: !!job,
      batch_id: batchId,
      job: job
        ? {
            pid: job.pid,
            started_at: job.startedAt,
            domains: job.domains,
            tools: job.tools || [],
          }
        : null,
      meta,
    });
  }

  // Batch log
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

  // Done check — fastest way for the HTML to know results are ready.
  if (method === "GET" && url.pathname === "/done") {
    const jobId = (url.searchParams.get("job_id") || "").trim();
    if (!jobId) {
      return jsonResponse(res, 400, { ok: false, error: "Missing job_id" });
    }
    const flagPath = doneFlagPath(jobId);
    const isDone = fs.existsSync(flagPath);
    return jsonResponse(res, 200, { ok: true, done: isDone });
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