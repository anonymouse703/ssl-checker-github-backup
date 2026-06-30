/**
 * utils/recovery.js
 *
 * Handles emergency restart recovery for the domain scanner.
 *
 * What this module does:
 *   1. markInterruptedJobsOnStartup()  — call once at api-server.js startup.
 *      Scans all progress_*.txt files. Any that say RUNNING/STARTING are
 *      changed to INTERRUPTED so FileMaker / the dashboard can find them.
 *
 *   2. findInterruptedJobs()  — returns every job that can be resumed.
 *      Reads progress files, checkpoint files, batch meta, and domain lists
 *      to give you a complete picture of what was left unfinished.
 *
 *   3. markJobStopped(jobId)  — user-initiated "don't resume this".
 *      Sets status=STOPPED, removes the checkpoint file.
 *
 *   4. buildResumeEnv(jobId)  — builds the environment variables needed
 *      to re-spawn multi-audit.js so it picks up the checkpoint and skips
 *      already-completed domains.
 *
 * Usage in api-server.js:
 *   const { markInterruptedJobsOnStartup, findInterruptedJobs,
 *           markJobStopped, buildResumeEnv } = require('./utils/recovery');
 *
 *   // Near the top of api-server.js, after constants are defined:
 *   markInterruptedJobsOnStartup();
 */

'use strict';

const fs   = require('fs');
const path = require('path');

const OUTPUT_DIR    = process.env.OUTPUT_DIR  || '/home/ind/ind_leads_inputs';
const API_BATCH_DIR = path.join(OUTPUT_DIR, 'api_batches');
const TOOL_DIR      = process.env.TOOL_DIR    || '/usr/local/ind_leads/ssl-checker-tool';

// ── File helpers ──────────────────────────────────────────────────────────────

function readTxtProgress(filePath) {
  try {
    const obj = {};
    fs.readFileSync(filePath, 'utf8').split('\n').forEach(line => {
      const eq = line.indexOf('=');
      if (eq > -1) obj[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
    });
    return obj;
  } catch (_) {
    return null;
  }
}

function writeTxtProgress(filePath, obj) {
  const lines = Object.entries(obj).map(([k, v]) => `${k}=${v}`);
  fs.writeFileSync(filePath, lines.join('\n') + '\n', 'utf8');
}

function readJson(filePath) {
  try { return JSON.parse(fs.readFileSync(filePath, 'utf8')); } catch (_) { return null; }
}

function writeJson(filePath, obj) {
  try { fs.writeFileSync(filePath, JSON.stringify(obj, null, 2), 'utf8'); } catch (_) {}
}

function fileAgeMinutes(filePath) {
  try {
    return (Date.now() - fs.statSync(filePath).mtimeMs) / 60000;
  } catch (_) {
    return Infinity;
  }
}

function readDomainFile(filePath) {
  try {
    return fs.readFileSync(filePath, 'utf8')
      .split('\n')
      .map(d => d.trim())
      .filter(d => d && !d.startsWith('#'));
  } catch (_) {
    return [];
  }
}

// ── Job file paths ────────────────────────────────────────────────────────────

function progressPath(jobId)    { return path.join(OUTPUT_DIR,    `progress_${jobId}.txt`); }
function checkpointPath(jobId)  { return path.join(OUTPUT_DIR,    `checkpoint_${jobId}.json`); }
function doneFlagPath(jobId)    { return path.join(OUTPUT_DIR,    `done_${jobId}.flag`); }
function batchMetaPath(batchId) { return path.join(API_BATCH_DIR, `${batchId}.json`); }
function domainListPath(id)     { return path.join(API_BATCH_DIR, `${id}_domains.txt`); }

// ── Active statuses that mean "was running when server died" ──────────────────

const ACTIVE_STATUSES = new Set(['RUNNING', 'STARTING', 'PROCESSING', 'SCANNING', 'ACTIVE']);

// ─────────────────────────────────────────────────────────────────────────────
// 1.  markInterruptedJobsOnStartup
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Call once at server startup (before listening).
 * Marks every progress file that says RUNNING → INTERRUPTED.
 * Also updates the matching batch meta JSON.
 *
 * @returns {Array<{jobId, completed, total}>}
 */
function markInterruptedJobsOnStartup() {
  const interrupted = [];
  const nowIso = new Date().toISOString();

  // ── Scan progress_*.txt ───────────────────────────────────────────────────
  let entries = [];
  try { entries = fs.readdirSync(OUTPUT_DIR); } catch (_) {}

  for (const file of entries) {
    if (!file.startsWith('progress_') || !file.endsWith('.txt')) continue;

    const filePath = path.join(OUTPUT_DIR, file);
    try {
      const obj = readTxtProgress(filePath);
      if (!obj) continue;

      const status = String(obj.status || '').trim().toUpperCase();
      if (!ACTIVE_STATUSES.has(status)) continue;

      // Don't re-mark a file that was modified in the last 60 s
      // (another process may have just started it)
      if (fileAgeMinutes(filePath) < 1) continue;

      obj.status           = 'INTERRUPTED';
      obj.interrupted_at   = nowIso;
      obj.previous_status  = status;
      writeTxtProgress(filePath, obj);

      const jobId    = obj.job_id || file.replace(/^progress_/, '').replace(/\.txt$/, '');
      const completed = parseInt(obj.completed || '0', 10);
      const total     = parseInt(obj.total     || '0', 10);

      interrupted.push({ jobId, completed, total });
      console.log(`[recovery] ⚠️  Marked interrupted: ${jobId}  (${completed}/${total} done)`);
    } catch (e) {
      console.error(`[recovery] Could not update ${file}: ${e.message}`);
    }
  }

  // ── Scan api_batches/*.json ───────────────────────────────────────────────
  let metaFiles = [];
  try {
    if (fs.existsSync(API_BATCH_DIR)) {
      metaFiles = fs.readdirSync(API_BATCH_DIR).filter(f => f.endsWith('.json'));
    }
  } catch (_) {}

  for (const file of metaFiles) {
    const filePath = path.join(API_BATCH_DIR, file);
    try {
      const meta   = readJson(filePath);
      if (!meta || !meta.batchId) continue;

      const status = String(meta.status || '').trim().toUpperCase();
      if (!ACTIVE_STATUSES.has(status) && status !== 'QUEUED') continue;
      if (fileAgeMinutes(filePath) < 1) continue;

      meta.status          = 'INTERRUPTED';
      meta.interruptedAt   = nowIso;
      meta.previousStatus  = status;
      writeJson(filePath, meta);
    } catch (e) {
      console.error(`[recovery] Could not update meta ${file}: ${e.message}`);
    }
  }

  if (interrupted.length > 0) {
    console.log(`[recovery] ✅ Startup complete — ${interrupted.length} interrupted job(s) found`);
  } else {
    console.log('[recovery] ✅ Startup complete — no interrupted jobs found');
  }

  return interrupted;
}


// ─────────────────────────────────────────────────────────────────────────────
// 2.  findInterruptedJobs
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Returns an array of all jobs that are INTERRUPTED or orphaned RUNNING.
 * Each entry contains enough data to display in the dashboard and to
 * build a resume command.
 */
function findInterruptedJobs() {
  const jobs  = [];
  const seen  = new Set();
  let entries = [];

  try { entries = fs.readdirSync(OUTPUT_DIR); } catch (_) {}

  for (const file of entries) {
    if (!file.startsWith('progress_') || !file.endsWith('.txt')) continue;

    const filePath = path.join(OUTPUT_DIR, file);
    try {
      const obj = readTxtProgress(filePath);
      if (!obj) continue;

      const status = String(obj.status || '').trim().toUpperCase();
      if (status !== 'INTERRUPTED' && !ACTIVE_STATUSES.has(status)) continue;

      // Skip obviously current jobs (updated < 2 min ago AND still RUNNING)
      if (ACTIVE_STATUSES.has(status) && fileAgeMinutes(filePath) < 2) continue;

      const jobId = obj.job_id || file.replace(/^progress_/, '').replace(/\.txt$/, '');
      if (seen.has(jobId)) continue;
      seen.add(jobId);

      // Is there a done flag? Then it actually finished cleanly
      if (fs.existsSync(doneFlagPath(jobId))) continue;

      const completed      = parseInt(obj.completed || '0', 10);
      const total          = parseInt(obj.total     || '0', 10);
      const lastDomain     = obj.last_domain     || obj.domain || '';
      const interruptedAt  = obj.interrupted_at  || null;
      const failedDomains  = (obj.failed_domains || '').split(',').filter(Boolean);
      const doneDomains    = (obj.completed_domains || '').split(',').filter(Boolean);

      // ── Checkpoint ───────────────────────────────────────────────────────
      const cp = readJson(checkpointPath(jobId));
      const checkpointDone = cp ? (cp.done || []) : [];

      // ── Batch meta ───────────────────────────────────────────────────────
      // api-server stores meta as api_batches/<batchId>.json where batchId = jobId for multi-audit
      const meta = readJson(batchMetaPath(jobId));

      // ── Domain list ──────────────────────────────────────────────────────
      let domainListFile = null;
      let domainList     = [];

      const candidates = [
        meta && meta.domainListFile,
        domainListPath(jobId),
      ].filter(Boolean);

      for (const c of candidates) {
        if (fs.existsSync(c)) {
          const domains = readDomainFile(c);
          if (domains.length) {
            domainList     = domains;
            domainListFile = c;
            break;
          }
        }
      }

      // Fallback: use domains snapshot saved in meta
      if (!domainList.length && meta && Array.isArray(meta.domains)) {
        domainList = meta.domains;
      }

      // Domains not yet in checkpoint = still need scanning
      const checkpointSet  = new Set(checkpointDone);
      const pendingDomains = domainList.filter(d => !checkpointSet.has(d));

      const mtime = fs.statSync(filePath).mtimeMs;

      jobs.push({
        jobId,
        batchId      : (meta && meta.batchId) || jobId,
        type         : domainList.length > 1 ? 'batch' : 'single',
        status       : status === 'INTERRUPTED' ? 'INTERRUPTED' : 'ORPHANED_RUNNING',
        completed,
        total        : total || domainList.length,
        lastDomain,
        interruptedAt,
        lastModified : new Date(mtime).toISOString(),
        // Checkpoint data
        checkpointCount  : checkpointDone.length,
        checkpointDomains: checkpointDone,
        // Domain list
        domainListFile,
        domainList,
        domainCount  : domainList.length,
        // What still needs to run
        pendingDomains,
        pendingCount : pendingDomains.length,
        // Failures from this run
        failedDomains,
        doneDomains,
        // Resume capability
        canResume    : domainList.length > 0 && domainListFile !== null,
        hasCheckpoint: checkpointDone.length > 0,
        // Server that was running it
        serverName   : (meta && meta.serverName) || null,
        // Extra meta from batch
        enabledTools : (meta && meta.enabledTools) || null,
        maxConcurrent: (meta && meta.maxConcurrent) || null,
      });
    } catch (e) {
      console.error(`[recovery] Error reading ${file}: ${e.message}`);
    }
  }

  // Newest first
  return jobs.sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified));
}


// ─────────────────────────────────────────────────────────────────────────────
// 3.  markJobStopped
// ─────────────────────────────────────────────────────────────────────────────

/**
 * User clicked "Mark as Stopped" — don't resume this job.
 * Updates the progress file, updates batch meta, removes checkpoint.
 *
 * @param {string} jobId
 * @returns {{ ok: boolean, jobId: string, error?: string }}
 */
function markJobStopped(jobId) {
  try {
    const nowIso = new Date().toISOString();

    // Progress file
    const pFile = progressPath(jobId);
    if (fs.existsSync(pFile)) {
      const obj    = readTxtProgress(pFile) || {};
      obj.status   = 'STOPPED';
      obj.stopped_at = nowIso;
      writeTxtProgress(pFile, obj);
    }

    // Batch meta
    const mFile = batchMetaPath(jobId);
    if (fs.existsSync(mFile)) {
      const meta     = readJson(mFile) || {};
      meta.status    = 'STOPPED';
      meta.stoppedAt = nowIso;
      writeJson(mFile, meta);
    }

    // Remove checkpoint so this job cannot accidentally resume
    const cpFile = checkpointPath(jobId);
    try { if (fs.existsSync(cpFile)) fs.unlinkSync(cpFile); } catch (_) {}

    console.log(`[recovery] Job ${jobId} marked STOPPED by user`);
    return { ok: true, jobId };
  } catch (e) {
    console.error(`[recovery] markJobStopped(${jobId}) failed: ${e.message}`);
    return { ok: false, jobId, error: e.message };
  }
}


// ─────────────────────────────────────────────────────────────────────────────
// 4.  buildResumeEnv
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Build the environment variables for spawning multi-audit.js to resume
 * an interrupted job.  The JOB_ID is intentionally kept the same so
 * multi-audit.js picks up the existing checkpoint_<JOB_ID>.json and
 * skips already-completed domains automatically.
 *
 * @param {object} job  — entry returned by findInterruptedJobs()
 * @returns {object}    — env vars to spread into process.env for spawn()
 */
function buildResumeEnv(job) {
  const env = {
    ...process.env,
    JOB_ID         : job.jobId,
    OUTPUT_DIR,
  };

  if (job.domainListFile) {
    env.DOMAIN_LIST_FILE = job.domainListFile;
  }

  if (job.enabledTools) {
    env.ENABLED_TOOLS = Array.isArray(job.enabledTools)
      ? JSON.stringify(job.enabledTools)
      : String(job.enabledTools);
  }

  if (job.maxConcurrent) {
    env.MAX_CONCURRENT = String(job.maxConcurrent);
  }

  return env;
}


module.exports = {
  markInterruptedJobsOnStartup,
  findInterruptedJobs,
  markJobStopped,
  buildResumeEnv,
};