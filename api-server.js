"use strict";

/**
 * api-server.js
 *
 * Changes vs original:
 *
 *  1. CONCURRENT JOBS — replaced the single activeScan / activeBatch mutex
 *     with an activeJobs Map keyed by "single:<domain>" or "multi:<batchId>".
 *     15 users scanning 15 different domains now all run in parallel.
 *     Only the same domain at the same time is rejected.
 *
 *  2. PER-JOB PROGRESS FILES — each spawned process gets a unique JOB_ID
 *     env var.  progress.txt is now progress_<JOB_ID>.txt so concurrent scans
 *     never overwrite each other's status.  /status and /multi-status read
 *     the right file via the job entry stored in activeJobs.
 *
 *  3. DOMAIN LOCK FILES — before spawning, the server creates
 *     lock_<domain>.pid in OUTPUT_DIR.  This prevents two concurrent single
 *     scans (or a single + multi scan) from running the same domain at the
 *     same time even if the in-memory check is bypassed on restart.
 *
 *  4. DONE FLAG / LATEST PATH — keyed by JOB_ID so they don't collide.
 *
 *  5. /tester-multi endpoint moved inside handleRequest() (was outside in
 *     original, causing a ReferenceError because url/method were out of scope).
 *
 *  6. FIXED: Added proper cleanup of domain locks when scans complete
 *  7. FIXED: Better error handling for multi-batch domain lock acquisition
 *  8. FIXED: /status endpoint now properly checks done flag before marking as running
 */

const http = require("http");
const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");
const crypto = require("crypto");
const { initPool, poolStats } = require("./utils/browser-pool");

// ── Configuration ─────────────────────────────────────────────────────────────
const PORT = 3000;
const TOOL_DIR = "/usr/local/ind_leads/ssl-checker-tool";
const OUTPUT_DIR = "/home/ind/ind_leads_inputs";
const SCAN_ROOT = "/home/ind";

const API_BATCH_DIR = path.join(OUTPUT_DIR, "api_batches");
const MAX_ACTIVE_RESERVED_DOMAINS = parseInt(
  process.env.MAX_ACTIVE_RESERVED_DOMAINS || "6",
  10,
);

// ── Active runtime state ──────────────────────────────────────────────────────
// Map key: "single:<domain>"  or  "multi:<batchId>"
// Value:   job descriptor object (see createJobEntry helpers below)
//
// Multiple single scans for DIFFERENT domains run concurrently.
// The same domain cannot be scanned twice at the same time.
// Multiple multi-batches can also run concurrently (different batch IDs).
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
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
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
  return String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/+$/, "");
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

/**
 * Create a unique JOB_ID for a single-domain scan.
 * Format: <domain>_<timestamp>_<rand4>
 * The domain part makes progress files easy to identify when browsing the dir.
 */
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

// ── Domain lock file helpers ──────────────────────────────────────────────────
// Cross-process mutex for the same domain.  The lock file contains the PID
// of the process that owns it.  On process exit (or explicit release) the
// file is removed.  Stale locks (PID no longer alive) are automatically
// broken so a server restart doesn't permanently block a domain.

function tryAcquireDomainLock(domain, pid) {
  const lockFile = domainLockPath(domain);
  try {
    fs.writeFileSync(lockFile, String(pid), { flag: "wx" });
    console.log(`[lock] Acquired lock for ${domain} (PID: ${pid})`);
    return true; // we got it
  } catch (e) {
    if (e.code !== "EEXIST") throw e;
    // Lock exists — check if the owning PID is still alive
    try {
      const ownerPid = parseInt(readFileOrNull(lockFile) || "0", 10);
      try {
        process.kill(ownerPid, 0);
        console.log(`[lock] Lock for ${domain} held by alive PID ${ownerPid}`);
        return false; // still alive — cannot take the lock
      } catch (_) {
        // Dead PID — stale lock, take it over
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
    else if (job.type === "multi" && Array.isArray(job.domains))
      count += job.domains.length;
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

// ── Progress file helpers ─────────────────────────────────────────────────────

function initJobProgressFile(jobId, domain, total) {
  writeTextSafe(
    progressFilePath(jobId),
    `completed=0\ntotal=${total}\nlast_domain=${domain}\nlast_finish=\nstatus=STARTING\njob_id=${jobId}\ndomain=${domain}`,
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
          } else inQuotes = false;
        } else cur += ch;
      } else {
        if (ch === '"') inQuotes = true;
        else if (ch === ",") {
          cols.push(cur);
          cur = "";
        } else cur += ch;
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

// ── Domain CSV lookup ─────────────────────────────────────────────────────────

function findLatestDomainCSV(domain) {
  let bestMtime = 0;
  let bestPath = null;

  try {
    const entries = fs.readdirSync(SCAN_ROOT);
    for (const entry of entries) {
      if (!/^\d{4}-\d{2}-\d{2}/.test(entry)) continue;
      const csvPath = path.join(
        SCAN_ROOT,
        entry,
        domain,
        `${domain}_results.csv`,
      );
      if (fs.existsSync(csvPath)) {
        const mtime = fs.statSync(csvPath).mtimeMs;
        if (mtime > bestMtime) {
          bestMtime = mtime;
          bestPath = csvPath;
        }
      }
    }
  } catch (_) {}

  return bestPath;
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
      if (fs.existsSync(statsPath))
        mtime = Math.max(mtime, fs.statSync(statsPath).mtimeMs);
      if (fs.existsSync(summaryPath))
        mtime = Math.max(mtime, fs.statSync(summaryPath).mtimeMs);

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

// ── PUPPETEER PATH ────────────────────────────────────────────────────────────

const PUPPETEER_PATH =
  "/usr/local/ind_leads/ssl-checker-tool/chrome/linux-145.0.7632.67/chrome-linux64/chrome";

// ── Request router ────────────────────────────────────────────────────────────

function cleanupServerOwnedLocks() {
  console.log("[api] Cleaning up domain locks owned by this server...");
  for (const job of activeJobs.values()) {
    if (job.type === "single" && job.domain)
      releaseDomainLock(job.domain, process.pid);
    else if (job.type === "multi")
      releaseBatchDomainLocks(job.domains, process.pid);
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

// ── Stale job cleanup ─────────────────────────────────────────────────────────
// Kills and removes jobs that have been running longer than maxAgeMs.
// Called automatically every 30 minutes and also via DELETE /jobs/stale.

const STALE_JOB_MAX_AGE_MS = parseInt(
  process.env.STALE_JOB_MAX_AGE_HOURS || "2",
  10,
) * 60 * 60 * 1000;

function purgeStaleJobs(maxAgeMs) {
  const now = Date.now();
  const killed = [];
  const skipped = [];

  for (const [key, job] of activeJobs.entries()) {
    const age = now - new Date(job.startedAt).getTime();
    if (age < maxAgeMs) continue;

    console.log(
      `[stale] Job ${key} started ${job.startedAt} (${Math.round(age / 60000)} min ago) — killing PID ${job.pid}`,
    );

    // Kill the child process
    try {
      process.kill(job.pid, "SIGKILL");
      console.log(`[stale] Killed PID ${job.pid} for job ${key}`);
    } catch (e) {
      // Already dead — that's fine, still clean up
      console.log(`[stale] PID ${job.pid} already gone (${e.code})`);
    }

    // Release domain lock
    if (job.type === "single" && job.domain) {
      releaseDomainLock(job.domain, job.pid);
    } else if (job.type === "multi" && Array.isArray(job.domains)) {
      releaseBatchDomainLocks(job.domains, job.pid);
    }

    // Remove from active jobs
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

  return { killed, skipped };
}

// Auto-cleanup every 30 minutes
const STALE_CLEANUP_INTERVAL_MS = 30 * 60 * 1000;
setInterval(() => {
  console.log("[stale] Running scheduled stale job cleanup...");
  purgeStaleJobs(STALE_JOB_MAX_AGE_MS);
}, STALE_CLEANUP_INTERVAL_MS).unref(); // .unref() so this timer doesn't keep the process alive alone

function handleRequest(req, res) {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const method = req.method.toUpperCase();

  // CORS preflight
  if (method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type",
    });
    return res.end();
  }

  // ── GET /health ─────────────────────────────────────────────────────────────
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

  // ── POST /scan ──────────────────────────────────────────────────────────────
  if (method === "POST" && url.pathname === "/scan") {
    return parseBodyJson(req, (err, parsed) => {
      if (err) return jsonResponse(res, 400, { ok: false, error: err.message });

      const domain = sanitizeDomain(parsed.domain);
      if (!domain)
        return jsonResponse(res, 400, {
          ok: false,
          error: "domain is required",
        });

      const jobKey = `single:${domain}`;

      // Check if already running in memory
      if (activeJobs.has(jobKey)) {
        const existing = activeJobs.get(jobKey);

        // ✅ ALWAYS check done flag FIRST — if the scan finished, clean up and allow re-scan
        // regardless of whether the PID is still alive (PIDs can be reused by the OS)
        const alreadyDone = fs.existsSync(existing.doneFlag);
        if (alreadyDone) {
          console.log(`[api] Job for ${domain} is done (done flag exists), clearing from activeJobs to allow re-scan`);
          releaseDomainLock(domain, existing.pid);
          activeJobs.delete(jobKey);
        } else {
          // Done flag not present — check if the process is actually still alive
          try {
            process.kill(existing.pid, 0);
            // Process is alive AND not done — genuinely still running, reject
            return jsonResponse(res, 200, {
              ok: false,
              error: `Scan already running for ${domain}. Poll /status?domain=${domain} for progress.`,
              job_id: existing.jobId,
              started_at: existing.startedAt,
              pid: existing.pid,
            });
          } catch (_) {
            // Process is dead and no done flag — crashed, clean up and allow re-scan
            console.log(`[api] Cleaning up crashed job for ${domain} (PID ${existing.pid})`);
            releaseDomainLock(domain, existing.pid);
            activeJobs.delete(jobKey);
          }
        }
      }

      // ✅ MODIFIED: Allow immediate re-scans - only clean up old flags, don't block
      const possibleJobId = createJobId(domain);
      const possibleDoneFlag = doneFlagPath(possibleJobId);
      if (fs.existsSync(possibleDoneFlag)) {
        // Clean up old done flags to prevent accumulation (optional)
        const doneTime = fs.statSync(possibleDoneFlag).mtimeMs;
        if (Date.now() - doneTime > 60 * 60 * 1000) { // Older than 1 hour
          try {
            fs.unlinkSync(possibleDoneFlag);
            console.log(`[api] Cleaned up old done flag for ${domain}`);
          } catch (_) {}
        }
        // ✅ Allow re-scan immediately - no blocking
        console.log(`[api] Allowing re-scan for ${domain} (last scan at ${new Date(doneTime).toISOString()})`);
      }

      // Cross-process domain lock guard
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

      // Update lock file with actual child PID
      try {
        fs.writeFileSync(domainLockPath(domain), String(child.pid), {
          flag: "w",
        });
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

      console.log(
        `[api] single scan started: ${domain} (pid ${child.pid}) jobId=${jobId}`,
      );

      // ✅ Use a write stream instead of appendFileSync on every data event.
      // appendTextSafe calls fs.appendFileSync synchronously per chunk — with a
      // long-running scan that can be hundreds of blocking writes.  A single
      // createWriteStream buffers internally and flushes in larger batches,
      // reducing both CPU and memory churn.
      const logStream = fs.createWriteStream(logFilePath(jobId), { flags: "a" });
      child.stdout.pipe(logStream);
      child.stderr.on("data", (data) =>
        logStream.write("[stderr] " + String(data)),
      );

      child.on("close", (code) => {
        logStream.end();
        console.log(
          `[api] single scan finished: ${domain} exit=${code} jobId=${jobId}`,
        );
        // ✅ Always write done flag on close so the re-scan check can detect completion
        // even if the child script didn't write it (e.g. crashed or exited early)
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

  // ── GET /status?domain=example.com ─────────────────────────────────────────
  if (method === "GET" && url.pathname === "/status") {
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");
    if (!domain) {
      return jsonResponse(res, 400, { ok: false, error: "domain parameter required" });
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
      // Server may have restarted — look for the newest done/progress file for this domain
      try {
        const safeDomain = domain.replace(/[^a-z0-9._-]/gi, "_");
        const files = fs
          .readdirSync(OUTPUT_DIR)
          .filter(
            (f) =>
              f.startsWith(`progress_${safeDomain}`) && f.endsWith(".txt"),
          )
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
      
      // Check if it's actually running vs completed
      if (isDone) {
        statusText = "Audit complete";
        isRunning = false;
      } else if (activeJobs.has(jobKey)) {
        isRunning = true;
        // Check if process is actually alive
        try {
          process.kill(activeJobs.get(jobKey).pid, 0);
        } catch (_) {
          // Process is dead but job still in activeJobs - cleanup
          console.log(`[api] Cleaning up stale job entry for ${domain}`);
          activeJobs.delete(jobKey);
          isRunning = false;
          statusText = "Scan was interrupted";
        }
      } else {
        isRunning = false;
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

  // ── GET /result?domain=example.com ─────────────────────────────────────────
  if (method === "GET" && url.pathname === "/result") {
    const domain = sanitizeDomain(url.searchParams.get("domain") || "");
    if (!domain)
      return jsonResponse(res, 400, {
        ok: false,
        error: "domain parameter required",
      });

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
            source: "domain_csv",
            path: csvPath,
            data: row,
          });
        }
      }
    }

    return jsonResponse(res, 404, {
      ok: false,
      error: `No result found for domain: ${domain}`,
      checked_path: csvPath || null,
    });
  }

  // ── POST /multi-scan ────────────────────────────────────────────────────────
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

      const conflictingInMemory = domains.filter((d) =>
        activeJobs.has(`single:${d}`),
      );
      if (conflictingInMemory.length > 0) {
        return jsonResponse(res, 200, {
          ok: false,
          error: `These domains are already being scanned: ${conflictingInMemory.join(", ")}. Wait for them to finish or exclude them.`,
          conflicting_domains: conflictingInMemory,
        });
      }

      const reservedNow = currentReservedDomainCount();
      if (reservedNow + domains.length > MAX_ACTIVE_RESERVED_DOMAINS) {
        return jsonResponse(res, 429, {
          ok: false,
          error: `Server is busy. This batch needs ${domains.length} slots, but only ${Math.max(0, MAX_ACTIVE_RESERVED_DOMAINS - reservedNow)} of ${MAX_ACTIVE_RESERVED_DOMAINS} are available.`,
          requested_domains: domains.length,
          reserved_domains: reservedNow,
          max_reserved_domains: MAX_ACTIVE_RESERVED_DOMAINS,
        });
      }

      const { conflicts: lockedConflicts } = acquireBatchDomainLocks(
        domains,
        process.pid,
      );
      if (lockedConflicts.length > 0) {
        return jsonResponse(res, 200, {
          ok: false,
          error: `These domains are locked by another process: ${lockedConflicts.join(", ")}. Wait for them to finish or exclude them.`,
          conflicting_domains: lockedConflicts,
        });
      }

      const batchId = createBatchId();
      const jobId = batchId; // parent batch progress job
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

      console.log(
        `[api] multi batch started: ${batchId} (${domains.length} domains, pid ${child.pid})`,
      );

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
            if (!entry.activeDomains.includes(domain))
              entry.activeDomains.push(domain);
          }
          m = t.match(/^__DOMAIN_DONE__:(.+?):(\d+)$/);
          if (m && activeJobs.has(jobKey)) {
            const entry = activeJobs.get(jobKey);
            entry.activeDomains = entry.activeDomains.filter(
              (d) => d !== m[1].trim(),
            );
          }
          m = t.match(/Batch folder\s*:\s*([^\s/]+)\/?$/i);
          if (m && activeJobs.has(jobKey)) {
            const entry = activeJobs.get(jobKey);
            entry.batchFolder = m[1];
            entry.batchPath = path.join(SCAN_ROOT, m[1]);
          }
          m = t.match(/\/home\/ind\/([^/]+)\/?$/i);
          if (m && activeJobs.has(jobKey)) {
            const entry = activeJobs.get(jobKey);
            if (!entry.batchFolder) {
              entry.batchFolder = m[1];
              entry.batchPath = path.join(SCAN_ROOT, m[1]);
            }
          }
        }
      }

      child.stdout.on("data", (data) => processBatchOutput(data, false));
      child.stderr.on("data", (data) => processBatchOutput(data, true));

      child.on("close", (code) => {
        console.log(`[api] multi batch finished: ${batchId} exit=${code}`);

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

      child.on("error", (err) => {
        console.error(`[api] multi batch error for ${batchId}: ${err.message}`);
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

  // ── GET /multi-status?batch_id=... ──────────────────────────────────────────
  if (method === "GET" && url.pathname === "/multi-status") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId)
      return jsonResponse(res, 400, {
        ok: false,
        error: "batch_id parameter required",
      });

    const meta = getBatchMeta(batchId);
    if (!meta)
      return jsonResponse(res, 404, { ok: false, error: "Unknown batch_id" });

    const jobKey = `multi:${batchId}`;
    const jobId = meta.jobId || batchId;
    const progress = readProgress(jobId);
    const logTail = tailFile(
      meta.logPath || path.join(API_BATCH_DIR, `${batchId}.log`),
      60,
    );

    // Running batch
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

    // Finished batch
    const batchPath =
      meta.batchPath || findNewestBatchPathAfter(meta.startedAt) || "";
    const statsPath =
      meta.statsJsonPath ||
      (batchPath ? path.join(batchPath, "_batch_stats.json") : "");
    const summaryPath =
      meta.summaryCsvPath ||
      (batchPath ? path.join(batchPath, "summary.csv") : "");
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
      batch_folder: batchPath
        ? path.basename(batchPath)
        : meta.batchFolder || "",
      batch_path: batchPath,
      summary_csv: summaryPath || "",
      stats_json: statsPath || "",
      log_tail: logTail,
    });
  }

  // ── GET /multi-result?batch_id=... ──────────────────────────────────────────
  if (method === "GET" && url.pathname === "/multi-result") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId)
      return jsonResponse(res, 400, {
        ok: false,
        error: "batch_id parameter required",
      });

    const meta = getBatchMeta(batchId);
    if (!meta)
      return jsonResponse(res, 404, { ok: false, error: "Unknown batch_id" });

    const batchPath =
      meta.batchPath || findNewestBatchPathAfter(meta.startedAt) || "";
    const summaryPath =
      meta.summaryCsvPath ||
      (batchPath ? path.join(batchPath, "summary.csv") : "");
    const statsPath =
      meta.statsJsonPath ||
      (batchPath ? path.join(batchPath, "_batch_stats.json") : "");

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

  // ── GET /multi-log?batch_id=... ─────────────────────────────────────────────
  if (method === "GET" && url.pathname === "/multi-log") {
    const batchId = (url.searchParams.get("batch_id") || "").trim();
    if (!batchId)
      return jsonResponse(res, 400, {
        ok: false,
        error: "batch_id parameter required",
      });

    const meta = getBatchMeta(batchId);
    if (!meta)
      return jsonResponse(res, 404, { ok: false, error: "Unknown batch_id" });

    const log = readFileOrNull(meta.logPath) || "(no batch log yet)";
    res.writeHead(200, {
      "Content-Type": "text/plain; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    });
    return res.end(log);
  }

  // ── GET /log?job_id=... (or ?domain=...) ────────────────────────────────────
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

  // ── DELETE /jobs/stale?hours=2 — kill jobs older than X hours ───────────────
  // Also accepts GET for convenience (e.g. curl without -X DELETE)
  if (
    (method === "DELETE" || method === "GET") &&
    url.pathname === "/jobs/stale"
  ) {
    const hours = parseFloat(url.searchParams.get("hours") || "2");
    if (isNaN(hours) || hours <= 0) {
      return jsonResponse(res, 400, {
        ok: false,
        error: "hours parameter must be a positive number (e.g. ?hours=2)",
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

  // ── GET /jobs — list all active jobs ────────────────────────────────────────
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

  // ── GET /tester ─────────────────────────────────────────────────────────────
  if (method === "GET" && url.pathname === "/tester") {
    const htmlPath = path.join(TOOL_DIR, "api-tester.html");
    const html = readFileOrNull(htmlPath);
    if (!html)
      return jsonResponse(res, 404, {
        ok: false,
        error: "api-tester.html not found in " + TOOL_DIR,
      });
    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    });
    return res.end(html);
  }

  // ── GET /tester-multi ───────────────────────────────────────────────────────
  if (method === "GET" && url.pathname === "/tester-multi") {
    const htmlPath = path.join(TOOL_DIR, "api-tester-multi.html");
    const html = readFileOrNull(htmlPath);
    if (!html)
      return jsonResponse(res, 404, {
        ok: false,
        error: "api-tester-multi.html not found in " + TOOL_DIR,
      });
    res.writeHead(200, {
      "Content-Type": "text/html; charset=utf-8",
      "Access-Control-Allow-Origin": "*",
    });
    return res.end(html);
  }

  // ── GET /cleanup?days=7 ──────────────────────────────────────────────────────
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

  // 404 fallback
  return jsonResponse(res, 404, {
    ok: false,
    error: `Unknown endpoint: ${url.pathname}`,
  });
}

// ── Start server ──────────────────────────────────────────────────────────────
const server = http.createServer(handleRequest);

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[api] ssl-checker-tool API server running on port ${PORT}`);
  console.log(`[api] endpoints:`);
  console.log(`[api]   POST http://0.0.0.0:${PORT}/scan`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/status?domain=example.com`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/result?domain=example.com`);
  console.log(`[api]   POST http://0.0.0.0:${PORT}/multi-scan`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/multi-status?batch_id=...`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/multi-result?batch_id=...`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/multi-log?batch_id=...`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/jobs`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/jobs/stale?hours=2  (kill stale jobs)`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/health`);
  console.log(`[api]   GET  http://0.0.0.0:${PORT}/log?job_id=...`);
});

server.on("error", (err) => {
  console.error(`[api] server error: ${err.message}`);
  process.exit(1);
});