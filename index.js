const fs = require('fs');
const path = require('path');
const http = require('http');
const https = require('https');
const dns = require('dns');
const { syncDomainImages } = require('./sync-images');
const { acquireBrowser, closePool } = require('./utils/browser-pool');
const { checkSiteReachable } = require('./utils/site-checker');

process.setMaxListeners(20);

// Enable garbage collection if available
if (global.gc) {
  const gcMonitor = setInterval(() => {
    const usage = process.memoryUsage();
    if (usage.rss > 400 * 1024 * 1024) {
      console.log(
        `[memory] High memory: RSS ${Math.round(usage.rss / 1024 / 1024)}MB, forcing GC`
      );
      global.gc();
    }
  }, 30000);

  if (typeof gcMonitor.unref === 'function') {
    gcMonitor.unref();
  }
}

const originalTimeout = setTimeout;
global.setTimeout = function (callback, ms) {
  return originalTimeout(callback, Math.max(1, ms || 0));
};

// Internal modules
const { loadEnv } = require('./config/env-loader');
const { resolveBatchPath, domainPaths, ensureDomainDirs } = require('./audit-paths');
const { createNewPage } = require('./utils/browser');
const { wait } = require('./utils/screenshot');
const { writeDomainCSV, writeSummaryCSV } = require('./utils/csv-writer');
const { getErrorCode } = require('./utils/error-codes');
const { updateLatestResults } = require('./utils/latest-results');

// Load .env before service modules are required.
// Some services read env values at module load time, so this must happen first.
loadEnv();

function setDefaultEnv(key, value) {
  if (process.env[key] === undefined || process.env[key] === '') {
    process.env[key] = String(value);
  }
}

// Index-only safety defaults. They can still be overridden from PM2/.env.
// Pingdom should stay on tools.pingdom.com, and index.js must not kill the
// browser while pingdom.js is still waiting for visible metrics and the final
// screenshot. Keep the outer wrapper to one long attempt and let pingdom.js
// handle its own retry loop.
setDefaultEnv('PINGDOM_BASE_URLS', 'https://tools.pingdom.com/');
setDefaultEnv('PINGDOM_TOOLS_URLS', 'https://tools.pingdom.com/');
setDefaultEnv('PINGDOM_FORCE_TOOLS_HOST', '1');
setDefaultEnv('PINGDOM_USE_DEDICATED_BROWSER', 'true');
setDefaultEnv('PINGDOM_TIMEOUT_MS', '1980000'); // ~33 min: covers Pingdom internal worst-case budget
setDefaultEnv('PINGDOM_ATTEMPTS', '1');

// DNS_TIMEOUT_MS wraps runWhatsMyDNSWithIndexFallback (real browser scrape +
// optional index-level public-resolver fallback). Keep this higher than
// WHATSMYDNS_BROWSER_TIMEOUT_MS so the real WhatsMyDNS page is not killed early.
setDefaultEnv('DNS_TIMEOUT_MS', '360000'); // 6 min — allow real WhatsMyDNS browser capture before fallback
setDefaultEnv('DNS_ATTEMPTS', '1');

// WHATSMYDNS_MAX_RETRIES governs dns.js's OWN internal retry loop (runWhatsMyDNS),
// which runs entirely inside the single WHATSMYDNS_BROWSER_TIMEOUT_MS window below.
// Leave at 1 unless WHATSMYDNS_BROWSER_TIMEOUT_MS is also raised enough to fit
// multiple full attempts (each can take 90s+ just for dns.js's own
// waitThroughSecurityVerification, well over 2-4 minutes total per attempt).
setDefaultEnv('WHATSMYDNS_MAX_RETRIES', '1');

// dns.js's own waitThroughSecurityVerification() can wait up to 90000ms before
// it even continues to the spinner/results waits. If this wrapper timeout is too
// small, the real WhatsMyDNS capture is killed and index.js generates the local
// fallback screenshot instead. Keep this around 300000ms unless you intentionally
// want a fast fallback.
setDefaultEnv('WHATSMYDNS_BROWSER_TIMEOUT_MS', '300000'); // 5 min — allow dns.js to finish real WhatsMyDNS capture
setDefaultEnv('WHATSMYDNS_SECURITY_TIMEOUT_MS', '90000'); // match dns.js security-verification wait budget
setDefaultEnv('DNS_RESOLVER_FALLBACK_TIMEOUT_MS', '8000');

// SSL Labs: outer wrapper must be larger than the sum of its internal polling windows.
// SSL_LABS_FIRST_GRADE_MAX_WAIT_MS + SSL_LABS_ALL_DONE_MAX_WAIT_MS + screenshot + buffer.
// 720000 + 1500000 + 300000 + 480000 = 3000000 (50 min).
// These setDefaultEnv calls only apply when the value is NOT already in .env / PM2 config.
setDefaultEnv('SSL_TIMEOUT_MS', '3000000');
setDefaultEnv('SSL_LABS_BROWSER_MAX_WAIT_MS', '900000');
setDefaultEnv('SSL_LABS_FIRST_GRADE_MAX_WAIT_MS', '720000');
setDefaultEnv('SSL_LABS_ALL_DONE_MAX_WAIT_MS', '1500000');

// INDepthDNS can run phase 1 + phase 2, so the outer wrapper needs to be longer
// than the combined internal phase waits.
setDefaultEnv('INDEPTHDNS_PHASE1_TIMEOUT_MS', '120000');
setDefaultEnv('INDEPTHDNS_PHASE2_TIMEOUT_MS', '180000');
setDefaultEnv('INDEPTHDNS_TIMEOUT_MS', '360000');
setDefaultEnv('INDEPTHDNS_ATTEMPTS', '1');

// Services
const { runSSLLabs } = require('./services/ssl-labs');
const { runSucuri } = require('./services/sucuri');
const { runPageSpeed } = require('./services/pagespeed');
const { runPingdom } = require('./services/pingdom');
const dnsService = require('./services/dns');
const runWhatsMyDNS =
  typeof dnsService === 'function'
    ? dnsService
    : typeof dnsService.runWhatsMyDNS === 'function'
      ? dnsService.runWhatsMyDNS
      : typeof dnsService.default === 'function'
        ? dnsService.default
        : null;

if (!runWhatsMyDNS) {
  throw new Error('runWhatsMyDNS export missing from ./services/dns');
}
const { runPageRank } = require('./services/pagerank');
const { runServerChecks } = require('./services/server-checks');
const { runIntoDNS } = require('./services/intodns');

let runInDepthDNS = async (domainName) => ({
  status: 'FAILED',
  error: 'INDepthDNS service module not loaded',
  errorCode: 'INDEPTHDNS_MODULE_MISSING',
  data: {},
  url: 'https://tool.indepthdns.com/',
  screenshot: '',
  tool: 'indepthdns',
});
try {
  ({ runInDepthDNS } = require('./services/indepthdns'));
} catch (err) {
  console.warn(`[startup] INDepthDNS service disabled: ${err.message}`);
}

let detectWordPress = async (inputUrl) => ({
  inputUrl,
  checkedUrl: inputUrl,
  finalUrl: inputUrl,
  status: null,
  verdict: 'unknown',
  confidence: 'none',
  score: 0,
  markerCount: 0,
  evidence: [],
  error: 'WordPress detector module not loaded',
});
try {
  ({ detectWordPress } = require('./utils/detect-wordpress'));
} catch (err) {
  console.warn(`[startup] WordPress detector disabled: ${err.message}`);
}

function sanitizeDomain(raw) {
  let s = String(raw || '').trim().toLowerCase();

  s = s.replace(/^https?:\/\//i, '');
  s = s.replace(/^www\./i, '');
  s = s.split('/')[0];
  s = s.split('?')[0];
  s = s.split('#')[0];
  s = s.replace(/:\d+$/, '');
  s = s.replace(/\.+$/, '');

  return s;
}

const domain = sanitizeDomain(process.argv[2]);
const DEBUG = process.argv.includes('--debug');
const FRESH = process.argv.includes('--fresh');

if (!domain) {
  console.log(JSON.stringify({ error: 'No domain provided' }));
  process.exit(1);
}

const sslDelay = parseInt(process.env.SSL_QUEUE_DELAY_MS || '0', 10);
const batchRoot = resolveBatchPath(process.env.SCAN_BATCH_PATH);
const paths = domainPaths(batchRoot, domain);
let reachabilitySnapshot = null;

console.log(`\n🔍 Starting audit for: ${domain}`);
console.log(`   📁 Batch root: ${batchRoot}`);
console.log(`   📂 Domain dir: ${paths.domainDir}`);
console.log(`   📄 CSV path: ${paths.csvPath}`);
console.log(`   📊 Domain summary path: ${paths.domainSummaryPath}`);
console.log(`   📊 Batch summary path : ${paths.batchSummaryPath}`);
console.log(`   🖼️  Images dir: ${paths.imagesDir}`);

ensureDomainDirs(paths);

const OUTPUT_DIR = process.env.OUTPUT_DIR || '/home/ind/ind_leads_inputs';

// ── Per-job progress file ─────────────────────────────────────────────────────
const JOB_ID = process.env.JOB_ID || domain.replace(/[^a-z0-9._-]/gi, '_');
const PROGRESS_FILE = path.join(OUTPUT_DIR, `progress_${JOB_ID}.txt`);
const LOG_FILE = path.join(OUTPUT_DIR, `progress_${JOB_ID}.log`);

// ── Domain lock file ──────────────────────────────────────────────────────────
const DOMAIN_LOCK_FILE = path.join(OUTPUT_DIR, `lock_${domain}.pid`);
const LOCK_STALE_MS = 30 * 60 * 1000; // 30 minutes

function acquireDomainLock() {
  try {
    fs.writeFileSync(DOMAIN_LOCK_FILE, String(process.pid), { flag: 'wx' });
    return true;
  } catch (e) {
    if (e.code === 'EEXIST') {
      try {
        const stats = fs.statSync(DOMAIN_LOCK_FILE);
        const now = Date.now();
        if (now - stats.mtimeMs > LOCK_STALE_MS) {
          // stale by age – take over
          fs.writeFileSync(DOMAIN_LOCK_FILE, String(process.pid), { flag: 'w' });
          return true;
        }
        const ownerPid = parseInt(fs.readFileSync(DOMAIN_LOCK_FILE, 'utf8'), 10);
        try {
          process.kill(ownerPid, 0);
          return false; // still alive
        } catch (_) {
          // dead – take over
          fs.writeFileSync(DOMAIN_LOCK_FILE, String(process.pid), { flag: 'w' });
          return true;
        }
      } catch (_) {
        return false;
      }
    }
    throw e;
  }
}

function releaseDomainLock() {
  try {
    if (fs.existsSync(DOMAIN_LOCK_FILE)) {
      const owner = fs.readFileSync(DOMAIN_LOCK_FILE, 'utf8').trim();
      if (owner === String(process.pid)) {
        fs.unlinkSync(DOMAIN_LOCK_FILE);
        console.log(`[lock] Released lock for ${domain}`);
      }
    }
  } catch (_) {}
}

process.on('exit', releaseDomainLock);
process.on('SIGINT', () => {
  releaseDomainLock();
  process.exit(130);
});
process.on('SIGTERM', () => {
  releaseDomainLock();
  process.exit(143);
});

// If lock is already held by us (e.g., from parent), skip acquisition
if (!acquireDomainLock()) {
  console.error(`❌ Another process is already scanning ${domain}. Exiting.`);
  process.exit(1);
}
console.log(`[lock] Acquired lock for ${domain} (PID: ${process.pid})`);

function progressLog(message) {
  const timestamp = new Date().toLocaleTimeString();
  const line = `[${timestamp}] ${message}`;
  process.stdout.write(line + '\n');
  try {
    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    fs.appendFileSync(LOG_FILE, line + '\n', 'utf8');
  } catch (_) {}
}

function writeProgress(completed, total, lastDomain, finishTime, status) {
  try {
    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    const lines = [
      `completed=${completed}`,
      `total=${total}`,
      `last_domain=${lastDomain}`,
      `last_finish=${finishTime}`,
      `status=${status}`,
      `job_id=${JOB_ID}`,
      `domain=${domain}`,
    ];
    fs.writeFileSync(PROGRESS_FILE, lines.join('\n'), 'utf8');
  } catch (_) {}
}


function safeWpFileKey(raw) {
  return String(raw || '').replace(/[^a-z0-9._-]/gi, '_');
}

function writeWordPressEarlyResult(wordpressCheck = {}) {
  try {
    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

    const wpCSV = normalizeWordPressDetectionForCSV(wordpressCheck || {});
    const payload = {
      domain,
      job_id: JOB_ID,
      IsWordPress: wpCSV.isWordPress,
      WordPress_Verdict: wpCSV.verdict || '',
      WordPress_Confidence: wpCSV.confidence || '',
      WordPress_Score: wpCSV.score || 0,
      WordPress_MarkerCount: wpCSV.markerCount || 0,
      WordPress_Final_URL: wpCSV.finalUrl || '',
      WordPress_Error: wpCSV.error || '',
      WordPress_Evidence: wpCSV.evidenceText || '',
      ready: true,
      written_at: new Date().toISOString(),
    };

    const files = [
      path.join(OUTPUT_DIR, `wp_${safeWpFileKey(JOB_ID)}.json`),
      path.join(OUTPUT_DIR, `wp_${safeWpFileKey(domain)}.json`),
    ];

    for (const file of [...new Set(files)]) {
      fs.writeFileSync(file, JSON.stringify(payload), 'utf8');
    }

    progressLog(`✅ WordPress early result written for ${domain}: ${payload.WordPress_Verdict}`);
  } catch (wpFileErr) {
    progressLog(`⚠️ Could not write WordPress early result: ${wpFileErr.message}`);
  }
}

if (FRESH && fs.existsSync(paths.csvPath)) {
  try {
    fs.unlinkSync(paths.csvPath);
    console.log(`🗑️  --fresh: deleted ${path.basename(paths.csvPath)}`);
  } catch (err) {
    if (err.code === 'EBUSY' || err.code === 'EPERM') {
      console.warn('⚠️  Cannot delete domain CSV — it is open in another program.');
    } else {
      throw err;
    }
  }
}

const ALL_TOOLS = [
  { key: 'ssl', label: '🔒 SSL Lab' },
  { key: 'sucuri', label: '🛡️  Sucuri' },
  { key: 'pagespeed', label: '📈 PageSpeed' },
  { key: 'pingdom', label: '📊 Pingdom' },
  { key: 'dns', label: '🌐 WhatsMyDNS' },
  { key: 'pagerank', label: '🔍 OnePageRank' },
  { key: 'server', label: '🖥️  Server' },
  { key: 'intodns', label: '🌐 IntoDNS' },
  { key: 'indepthdns', label: '🧭 INDepthDNS' },
];

// Keep the legacy/default FileMaker scan set stable. INDepthDNS is available
// only when FileMaker explicitly sends it in ENABLED_TOOLS. This prevents the
// new tool from changing old scan timing or browser load unexpectedly.
const DEFAULT_TOOL_KEYS = ALL_TOOLS
  .map((t) => t.key)
  .filter((key) => key !== 'indepthdns');

function readEnabledTools() {
  try {
    const raw = process.env.ENABLED_TOOLS || '[]';
    const parsed = JSON.parse(raw);

    if (!Array.isArray(parsed) || !parsed.length) {
      return DEFAULT_TOOL_KEYS;
    }

    const allowed = new Set(ALL_TOOLS.map((t) => t.key));
    return parsed
      .map((v) => String(v || '').trim().toLowerCase())
      .filter((v) => allowed.has(v));
  } catch (_) {
    return DEFAULT_TOOL_KEYS;
  }
}

const ENABLED_TOOL_KEYS = readEnabledTools();
const TOOLS = ALL_TOOLS.filter((t) => ENABLED_TOOL_KEYS.includes(t.key));
const TOTAL_TOOLS = TOOLS.length;
const toolStatus = {};

function isEnabled(key) {
  return ENABLED_TOOL_KEYS.includes(key);
}

writeProgress(0, TOTAL_TOOLS || 1, domain, '', 'RUNNING — ' + domain);

console.log(`   🔑 Job ID : ${JOB_ID}`);
console.log(`   ⏱️  SSL delay : ${sslDelay}ms`);

function renderProgress() {
  const done = Object.values(toolStatus).filter((t) => t.done).length;
  const lines = [`\n▶  Checking: ${domain}`, `   ${'─'.repeat(50)}`];
  TOOLS.forEach((t, i) => {
    const s = toolStatus[t.key];
    if (!s) {
      lines.push(`   ${i + 1}  ${t.label.padEnd(16)} ⏳ processing...`);
    } else {
      lines.push(`   ${i + 1}  ${t.label.padEnd(16)} ✅ done  (${s.timeStr})  ${s.result}`);
    }
  });
  lines.push(`   ${'─'.repeat(50)}`);
  lines.push(`   ${done} / ${TOTAL_TOOLS} complete\n`);
  console.clear();
  console.log(lines.join('\n'));
}

function markDone(key, resultStr, startMs) {
  toolStatus[key] = {
    done: true,
    result: resultStr,
    timeStr: formatDuration(Date.now() - startMs),
  };
  renderProgress();
}

function skippedResult(key) {
  return {
    status: 'SKIPPED',
    error: null,
    errorCode: null,
    url: '',
    screenshot: '',
    data: {},
    skipped: true,
    tool: key,
  };
}

function deleteLocalImages(imagesDir) {
  try {
    const imageExts = ['.png', '.jpg', '.jpeg', '.webp'];
    const files = fs.readdirSync(imagesDir);
    for (const file of files) {
      if (imageExts.includes(path.extname(file).toLowerCase())) {
        fs.unlinkSync(path.join(imagesDir, file));
      }
    }
  } catch (_) {}
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(promise, ms, label) {
  let timer = null;
  return Promise.race([
    Promise.resolve().then(() => promise),
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${Math.round(ms / 1000)}s`)), ms);
    }),
  ]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

function toolTimeoutMs(key) {
  const normalizedKey = String(key || '').toLowerCase();
  const perToolKey = `${normalizedKey.toUpperCase()}_TIMEOUT_MS`;
  const defaultByTool = {
    // SSL outer wrapper must exceed the combined internal polling budget:
    // SSL_LABS_FIRST_GRADE_MAX_WAIT_MS (12 min) + SSL_LABS_ALL_DONE_MAX_WAIT_MS (25 min)
    // + screenshotViaFreshTab overhead (~5 min) + buffer (~5 min) = ~47 min.
    // Set SSL_TIMEOUT_MS in .env to override. Default: 50 min.
    ssl: '3000000',
    pingdom: '1980000', // Pingdom's own internal retry loop can need ~31.75 min worst case; give it room.
    dns: '330000',       // Includes browser scrape (up to 300000ms) plus index resolver fallback.
  };
  return parseInt(process.env[perToolKey] || process.env.TOOL_TIMEOUT_MS || defaultByTool[normalizedKey] || '180000', 10);
}

function toolAttempts(key) {
  const normalizedKey = String(key || '').toLowerCase();
  const perToolKey = `${normalizedKey.toUpperCase()}_ATTEMPTS`;
  const defaultByTool = {
    pingdom: '1', // Pingdom service has its own retry loop.
    dns: '1',     // DNS wrapper has its own index fallback.
  };
  return Math.max(1, parseInt(process.env[perToolKey] || process.env.TOOL_ATTEMPTS || defaultByTool[normalizedKey] || '3', 10));
}

function toolRetryDelayMs(key) {
  const perToolKey = `${String(key || '').toUpperCase()}_RETRY_DELAY_MS`;
  return Math.max(0, parseInt(process.env[perToolKey] || process.env.TOOL_RETRY_DELAY_MS || '5000', 10));
}

// --- Pingdom timeout/retry budget sanity check -----------------------------
// track('pingdom', ...) wraps ONE full call to runPingdomWithRequiredData()
// (which itself runs pingdom.js's own internal PINGDOM_MAX_RETRIES loop) inside
// a single outer timeout taken from PINGDOM_TIMEOUT_MS. If that outer timeout is
// smaller than pingdom.js's own worst-case internal budget, index.js can abandon
// a Pingdom run mid-attempt — discarding the result and leaving no screenshot —
// even though pingdom.js was still actively working and might have succeeded
// seconds later. This check estimates that worst-case internal time and warns
// loudly at startup if PINGDOM_TIMEOUT_MS doesn't leave enough room for it, so a
// future retune of these values doesn't silently break image generation again.
function pingdomWorstCaseBudgetMs() {
  const resultWaitMs = Math.max(0, parseInt(process.env.PINGDOM_RESULT_MAX_WAIT_MS || '420000', 10));
  const stableWaitMs = Math.max(0, parseInt(process.env.PINGDOM_STABLE_METRICS_WAIT_MS || '150000', 10));
  // Covers navigation, legal/cookie-overlay handling, font/network-idle waits,
  // and screenshot capture retries that run on top of the two waits above.
  const overheadMs = 90000;
  const perAttemptMs = resultWaitMs + stableWaitMs + overheadMs;

  const internalRetries = Math.max(1, parseInt(process.env.PINGDOM_MAX_RETRIES || '3', 10));
  let internalDelaysMs = 0;
  for (let i = 1; i < internalRetries; i++) {
    internalDelaysMs += (i === 1 ? 45000 : 60000); // matches runPingdom()'s own retryDelay logic
  }
  const oneRunPingdomCallMs = (internalRetries * perAttemptMs) + internalDelaysMs;

  const outerRetryEnabled = String(process.env.PINGDOM_ENABLE_OUTER_REQUIRED_RETRY || '0').trim() === '1';
  const outerAttempts = outerRetryEnabled
    ? Math.max(1, parseInt(process.env.PINGDOM_REQUIRED_DATA_ATTEMPTS || '1', 10))
    : 1;
  const outerDelayMs = Math.max(0, parseInt(process.env.PINGDOM_REQUIRED_DATA_DELAY_MS || '5000', 10));

  const totalMs = (outerAttempts * oneRunPingdomCallMs) + (Math.max(0, outerAttempts - 1) * outerDelayMs);

  return { totalMs, oneRunPingdomCallMs, internalRetries, outerAttempts, resultWaitMs, stableWaitMs, overheadMs };
}

function validatePingdomTimeoutBudget() {
  try {
    const budget = pingdomWorstCaseBudgetMs();
    const outerTimeoutMs = toolTimeoutMs('pingdom');

    if (outerTimeoutMs < budget.totalMs) {
      console.warn(
        `[startup] ⚠️  PINGDOM_TIMEOUT_MS=${outerTimeoutMs}ms (~${Math.round(outerTimeoutMs / 60000)} min) is LOWER than the ` +
        `estimated worst-case time Pingdom may need for one full attempt cycle (~${Math.ceil(budget.totalMs / 60000)} min), ` +
        `given PINGDOM_MAX_RETRIES=${budget.internalRetries}, PINGDOM_RESULT_MAX_WAIT_MS=${budget.resultWaitMs}, ` +
        `PINGDOM_STABLE_METRICS_WAIT_MS=${budget.stableWaitMs}. On slow domains this can make index.js abandon Pingdom ` +
        `mid-run with NO screenshot, even though pingdom.js was still working. Raise PINGDOM_TIMEOUT_MS to at least ` +
        `${budget.totalMs}ms (~${Math.ceil(budget.totalMs / 60000)} min) in your .env, or reduce PINGDOM_MAX_RETRIES / ` +
        `PINGDOM_RESULT_MAX_WAIT_MS / PINGDOM_STABLE_METRICS_WAIT_MS so they fit inside the current timeout.`
      );
    } else {
      console.log(
        `[startup] Pingdom timeout budget OK — PINGDOM_TIMEOUT_MS=${outerTimeoutMs}ms covers estimated worst case ~${budget.totalMs}ms.`
      );
    }
  } catch (err) {
    console.warn(`[startup] Pingdom timeout budget check failed: ${err.message}`);
  }
}
// -----------------------------------------------------------------------------

if (isEnabled('pingdom')) {
  validatePingdomTimeoutBudget();
}

function toolResultLooksFailed(result) {
  const status = String(result?.status || '').trim().toUpperCase();
  return (
    status === 'FAILED' ||
    status === 'FAIL' ||
    status === 'ERROR' ||
    status === 'TIMEOUT' ||
    status.includes('FAILED') ||
    status.includes('ERROR') ||
    status.includes('TIMEOUT')
  );
}

function isNAValue(value) {
  const s = String(value ?? '').trim().toLowerCase();
  return s === '' || s === 'n/a' || s === 'na' || s === 'null' || s === 'none' || s === '?' || s === 'undefined' || s === 'skipped';
}


function firstRealValue(...values) {
  for (const value of values) {
    if (!isNAValue(value)) return value;
  }
  return '';
}

function getAliasedValue(obj, aliases) {
  const source = obj && typeof obj === 'object' ? obj : {};
  const normalized = new Map();

  for (const [key, value] of Object.entries(source)) {
    const nk = String(key || '')
      .toLowerCase()
      .replace(/[^a-z0-9]/g, '');
    normalized.set(nk, value);
  }

  for (const alias of aliases || []) {
    const exact = source[alias];
    if (!isNAValue(exact)) return exact;

    const nk = String(alias || '')
      .toLowerCase()
      .replace(/[^a-z0-9]/g, '');
    if (normalized.has(nk) && !isNAValue(normalized.get(nk))) {
      return normalized.get(nk);
    }
  }

  return '';
}

function normalizePingdomDataFields(data = {}) {
  const d = data && typeof data === 'object' ? data : {};

  return {
    ...d,
    performanceGrade: firstRealValue(
      d.performanceGrade,
      d.grade,
      d.gradeText,
      d.performance_grade,
      getAliasedValue(d, ['Performance grade', 'Performance Grade', 'performance grade', 'Grade'])
    ),
    gradeLetter: firstRealValue(
      d.gradeLetter,
      d.letter,
      d.performanceGradeLetter,
      d.grade_letter,
      getAliasedValue(d, ['Grade Letter', 'grade letter'])
    ),
    gradeNumber: firstRealValue(
      d.gradeNumber,
      d.score,
      d.performanceScore,
      d.performance_score,
      d.grade_number,
      getAliasedValue(d, ['Grade Number', 'Performance score', 'Performance Score', 'Score'])
    ),
    loadTime: firstRealValue(
      d.loadTime,
      d.load_time,
      d.load,
      d.time,
      getAliasedValue(d, ['Load time', 'Load Time', 'load time'])
    ),
    pageSize: firstRealValue(
      d.pageSize,
      d.page_size,
      d.size,
      getAliasedValue(d, ['Page size', 'Page Size', 'page size'])
    ),
    requests: firstRealValue(
      d.requests,
      d.requestCount,
      d.request_count,
      getAliasedValue(d, ['Requests', 'Request count', 'Request Count'])
    ),
  };
}

function textLooksUnreachable(...parts) {
  const s = parts.map((v) => String(v ?? '')).join(' ').toLowerCase();
  return (
    s.includes('dead') ||
    s.includes('unreachable') ||
    s.includes('not reachable') ||
    s.includes('site unreachable') ||
    s.includes('dns lookup failed') ||
    s.includes('dns_probe_finished_nxdomain') ||
    s.includes('nxdomain') ||
    s.includes('enotfound') ||
    s.includes('err_name_not_resolved') ||
    s.includes('err_connection_refused') ||
    s.includes('err_connection_timed_out') ||
    s.includes('http error 444') ||
    s.includes(' error 444') ||
    s.includes('cloudflare error 520') ||
    s.includes('cloudflare error 521') ||
    s.includes('cloudflare error 522') ||
    s.includes('cloudflare error 523') ||
    s.includes('cloudflare error 524') ||
    s.includes('host error') ||
    s.includes('origin is unreachable') ||
    s.includes('net::err')
  );
}

function pingdomResultHasRequiredData(result) {
  if (!result) return false;

  if (textLooksUnreachable(result.status, result.error, result.errorCode)) {
    return true;
  }

  const d = normalizePingdomDataFields(result.data || {});

  const hasGrade =
    !isNAValue(d.performanceGrade) ||
    !isNAValue(d.gradeNumber) ||
    !isNAValue(d.gradeLetter);

  const hasLoadTime = !isNAValue(d.loadTime);
  const hasPageSize = !isNAValue(d.pageSize);
  const hasRequests = !isNAValue(d.requests);

  // Screenshot is useful, but it must not be required before the Pingdom values
  // are accepted. The service now parses and validates metrics first, then tries
  // to capture pingdom.png. If the image step fails, save the metrics instead
  // of rerunning/failing the whole Pingdom tool.
  return hasGrade && hasLoadTime && hasPageSize && hasRequests;
}

async function runPingdomWithRequiredData(domain, context) {
  // IMPORTANT:
  // runPingdom() already has its own retry loop controlled by PINGDOM_MAX_RETRIES.
  // Keeping a second outer retry loop here can multiply runtime by 3x or 9x.
  // For FileMaker queue scans, keep this outer loop OFF unless explicitly enabled.
  const outerRetryEnabled = String(process.env.PINGDOM_ENABLE_OUTER_REQUIRED_RETRY || '0').trim() === '1';
  const attempts = outerRetryEnabled
    ? Math.max(1, parseInt(process.env.PINGDOM_REQUIRED_DATA_ATTEMPTS || '1', 10))
    : 1;
  const delayMs = Math.max(0, parseInt(process.env.PINGDOM_REQUIRED_DATA_DELAY_MS || '5000', 10));

  let lastResult = null;

  for (let attempt = 1; attempt <= attempts; attempt++) {
    lastResult = await runPingdom(domain, context);

    if (pingdomResultHasRequiredData(lastResult)) {
      if (attempt > 1) {
        progressLog(`✅ Pingdom values became ready for ${domain} after retry ${attempt}/${attempts}`);
      }
      return lastResult;
    }

    const d = normalizePingdomDataFields(lastResult?.data || {});
    const hasAnyPingdomValue =
      !isNAValue(d.performanceGrade) ||
      !isNAValue(d.gradeLetter) ||
      !isNAValue(d.gradeNumber) ||
      !isNAValue(d.loadTime) ||
      !isNAValue(d.pageSize) ||
      !isNAValue(d.requests);

    if (hasAnyPingdomValue) {
      progressLog(
        `⚠️ Pingdom returned partial values for ${domain}; saving partial values instead of rerunning. ` +
        `grade=${d.performanceGrade || d.gradeLetter || d.gradeNumber || 'N/A'}, ` +
        `load=${d.loadTime || 'N/A'}, pageSize=${d.pageSize || 'N/A'}, requests=${d.requests || 'N/A'}.`
      );
      return {
        ...(lastResult || {}),
        status: lastResult?.status && !toolResultLooksFailed(lastResult) ? lastResult.status : 'SUCCESS_PARTIAL',
        error: lastResult?.error || null,
        errorCode: lastResult?.errorCode || null,
        data: {
          ...(lastResult?.data || {}),
          performanceGrade: d.performanceGrade || 'N/A',
          gradeLetter: d.gradeLetter || 'N/A',
          gradeNumber: d.gradeNumber || null,
          loadTime: d.loadTime || 'N/A',
          pageSize: d.pageSize || 'N/A',
          requests: d.requests || 'N/A',
        },
        url: lastResult?.url || `https://tools.pingdom.com/#${domain}`,
        screenshot: lastResult?.screenshot,
      };
    }

    if (attempt < attempts) {
      progressLog(
        `⏳ Pingdom values are empty for ${domain}; waiting before retry ${attempt}/${attempts}`
      );
      if (delayMs > 0) await sleep(delayMs);
    }
  }

  const d = normalizePingdomDataFields(lastResult?.data || {});
  progressLog(
    `⚠️ Pingdom values are incomplete/empty for ${domain} after ${attempts} attempt(s). ` +
      `Saving Pingdom fallback values: ` +
      `grade=${d.performanceGrade ?? d.gradeLetter ?? d.gradeNumber ?? 'N/A'}, ` +
      `load=${d.loadTime ?? 'N/A'}, pageSize=${d.pageSize ?? 'N/A'}, requests=${d.requests ?? 'N/A'}.`
  );

  return {
    ...(lastResult || {}),
    status: 'FAILED',
    error: lastResult?.error || `Pingdom values incomplete after ${attempts} attempt(s)`,
    errorCode: lastResult?.errorCode || 'PINGDOM_INCOMPLETE_AFTER_RETRY',
    data: {
      ...(lastResult?.data || {}),
      performanceGrade: d.performanceGrade || 'N/A',
      gradeLetter: d.gradeLetter || 'N/A',
      gradeNumber: d.gradeNumber || null,
      loadTime: d.loadTime || 'N/A',
      pageSize: d.pageSize || 'N/A',
      requests: d.requests || 'N/A',
    },
    url: lastResult?.url || `https://tools.pingdom.com/#${domain}`,
    screenshot: lastResult?.screenshot,
  };
}

const DNS_FALLBACK_RESOLVERS = [
  { name: 'Google DNS 1', location: 'Global', server: '8.8.8.8' },
  { name: 'Google DNS 2', location: 'Global', server: '8.8.4.4' },
  { name: 'Cloudflare 1', location: 'Global', server: '1.1.1.1' },
  { name: 'Cloudflare 2', location: 'Global', server: '1.0.0.1' },
  { name: 'Quad9', location: 'Global', server: '9.9.9.9' },
  { name: 'OpenDNS 1', location: 'Global', server: '208.67.222.222' },
  { name: 'OpenDNS 2', location: 'Global', server: '208.67.220.220' },
  { name: 'CleanBrowsing', location: 'Global', server: '185.228.168.9' },
  { name: 'Level3', location: 'Global', server: '4.2.2.1' },
  { name: 'AdGuard', location: 'Global', server: '94.140.14.14' },
  { name: 'Verisign', location: 'Global', server: '64.6.64.6' },
  { name: 'Neustar', location: 'Global', server: '156.154.70.1' },
];

function dnsResultHasUsableRows(result) {
  const totalServers = Number(result?.data?.totalServers || 0);
  return !!result && !toolResultLooksFailed(result) && totalServers > 0;
}

function dnsBrowserFailureReason(result, caughtError) {
  return compactReason(
    caughtError?.message,
    result?.error,
    result?.errorCode,
    result?.status,
    Number(result?.data?.totalServers || 0) <= 0 ? 'no resolver rows parsed' : ''
  ) || 'WhatsMyDNS browser result was incomplete';
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

async function resolveAFromPublicResolver(domainName, resolverInfo, timeoutMs) {
  const started = Date.now();
  const resolver = new dns.promises.Resolver();
  resolver.setServers([resolverInfo.server]);

  try {
    const records = await withTimeout(
      resolver.resolve4(domainName),
      timeoutMs,
      `${resolverInfo.name} DNS resolver`
    );

    return {
      ...resolverInfo,
      propagated: Array.isArray(records) && records.length > 0,
      records: Array.isArray(records) ? records : [],
      error: '',
      durationMs: Date.now() - started,
    };
  } catch (err) {
    return {
      ...resolverInfo,
      propagated: false,
      records: [],
      error: err?.code || err?.message || 'DNS lookup failed',
      durationMs: Date.now() - started,
    };
  }
}

async function createDnsFallbackScreenshot(domainName, resolverRows, context, reason) {
  if (!context?.newTab || !context?.paths?.imagesDir) return null;

  // When WhatsMyDNS is blocked by Cloudflare, create our own stable DNS screenshot.
  // Use a separate filename so it is obvious when the image is a generated fallback,
  // not the real whatsmydns.net page.
  const screenshotFile = 'dns-fallback.png';
  const screenshotPath = path.join(context.paths.imagesDir, screenshotFile);
  let tab = null;

  try {
    tab = await context.newTab();
    await tab.setViewport({ width: 1080, height: 935, deviceScaleFactor: 1 });

    const generatedAt = new Date().toISOString().replace('T', ' ').replace(/\.\d+Z$/, ' UTC');
    const totalServers = resolverRows.length;
    const propagated = resolverRows.filter((row) => row.propagated).length;
    const failed = Math.max(0, totalServers - propagated);
    const rate = totalServers ? Math.round((propagated / totalServers) * 100) : 0;
    const liveUrl = `https://www.whatsmydns.net/#A/${domainName}`;

    const rowsHtml = resolverRows.map((row, idx) => {
      const status = row.propagated ? '✓' : '×';
      const answer = row.records && row.records.length ? row.records.join(', ') : (row.error || 'No A record');
      const city = row.name.replace(/ DNS \d?$/i, '').replace(/ \d$/i, '');
      const flagClass = row.propagated ? 'flag okflag' : 'flag badflag';
      return `
        <div class="dns-row">
          <div class="loc"><span class="${flagClass}"></span><span>${escapeHtml(city)}</span></div>
          <div class="answer">${escapeHtml(answer)}</div>
          <div class="status ${row.propagated ? 'ok' : 'bad'}">${escapeHtml(status)}</div>
        </div>`;
    }).join('');

    const html = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
  * { box-sizing: border-box; }
  body { margin: 0; background: #f5f7fa; color: #1f2933; font-family: Arial, Helvetica, sans-serif; font-size: 14px; }
  .top { background: #1f5f8f; height: 58px; display: flex; align-items: center; padding: 0 28px; color: #fff; }
  .logo { font-size: 22px; font-weight: 700; letter-spacing: -.3px; }
  .logo small { display: block; font-size: 10px; font-weight: 400; opacity: .85; margin-top: -2px; }
  .searchbar { background: #2b6f9f; padding: 13px 28px; display: flex; gap: 8px; align-items: center; }
  .searchbar input { width: 300px; height: 34px; border: 1px solid #17496d; border-radius: 2px; padding: 0 10px; font-weight: 700; }
  .searchbar select { height: 34px; border: 1px solid #17496d; border-radius: 2px; padding: 0 10px; background: white; }
  .searchbar button { height: 34px; border: 0; border-radius: 2px; padding: 0 16px; background: #f5b400; color: #102a43; font-weight: 700; }
  .wrap { padding: 22px 28px 40px; }
  .grid { display: grid; grid-template-columns: 52% 48%; gap: 24px; align-items: start; }
  .panel { background: #fff; border: 1px solid #d7dee8; border-radius: 3px; box-shadow: 0 1px 2px rgba(0,0,0,.05); }
  .panel-title { background: #f1f5f9; border-bottom: 1px solid #d7dee8; padding: 10px 12px; font-weight: 700; color: #183b56; }
  .summary { display: grid; grid-template-columns: repeat(4, 1fr); gap: 0; border-bottom: 1px solid #e5eaf1; }
  .metric { padding: 12px; border-right: 1px solid #e5eaf1; }
  .metric:last-child { border-right: 0; }
  .metric .label { color: #62748a; font-size: 11px; text-transform: uppercase; }
  .metric .value { font-size: 20px; font-weight: 700; margin-top: 3px; }
  .dns-row { display: grid; grid-template-columns: 42% 48% 10%; align-items: center; min-height: 31px; border-bottom: 1px solid #edf1f5; }
  .dns-row:last-child { border-bottom: 0; }
  .loc { padding: 7px 10px; display: flex; align-items: center; gap: 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .answer { padding: 7px 10px; font-size: 12px; color: #2563eb; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .status { text-align: center; font-size: 18px; font-weight: 700; }
  .ok { color: #16a34a; }
  .bad { color: #dc2626; }
  .flag { width: 14px; height: 10px; display: inline-block; border: 1px solid rgba(0,0,0,.15); }
  .okflag { background: linear-gradient(90deg, #2563eb 0 33%, #fff 33% 66%, #ef4444 66%); }
  .badflag { background: linear-gradient(90deg, #ef4444 0 50%, #fff 50%); }
  .hero { padding: 16px 18px; }
  .hero h1 { margin: 0 0 8px; font-size: 22px; color: #17324d; }
  .hero p { margin: 0 0 14px; color: #4b5563; line-height: 1.45; }
  .map { height: 260px; border: 1px solid #dbe3ee; background: radial-gradient(circle at 25% 45%, #2f6fa3 0 11%, transparent 12%), radial-gradient(circle at 52% 40%, #2f6fa3 0 13%, transparent 14%), radial-gradient(circle at 74% 55%, #2f6fa3 0 10%, transparent 11%), linear-gradient(#edf4fb, #e9f0f7); position: relative; overflow: hidden; }
  .pin { position: absolute; width: 10px; height: 10px; border-radius: 50%; background: #22c55e; border: 2px solid #fff; box-shadow: 0 0 0 1px rgba(0,0,0,.15); }
  .pin.bad { background: #ef4444; }
  .p1 { left: 18%; top: 48%; } .p2 { left: 29%; top: 37%; } .p3 { left: 47%; top: 45%; } .p4 { left: 61%; top: 33%; } .p5 { left: 76%; top: 58%; } .p6 { left: 86%; top: 67%; }
  .live { margin: 12px 0 0; font-size: 13px; }
  .live a { color: #2563eb; text-decoration: underline; }
  .article { margin-top: 22px; background: #fff; border: 1px solid #d7dee8; border-radius: 3px; padding: 18px 20px; line-height: 1.55; color: #374151; }
  .article h2 { margin: 0 0 8px; color: #183b56; font-size: 20px; }
  .footer-note { margin-top: 10px; color: #6b7280; font-size: 12px; }
</style>
</head>
<body>
  <div class="top"><div class="logo">whatsmydns.net<small>DNS propagation checker style report</small></div></div>
  <div class="searchbar">
    <input value="${escapeHtml(domainName)}" readonly>
    <select><option>A</option></select>
    <button>Search</button>
  </div>
  <div class="wrap">
    <div class="grid">
      <div class="panel">
        <div class="panel-title">DNS Propagation Results</div>
        <div class="summary">
          <div class="metric"><div class="label">Propagation</div><div class="value">${propagated}/${totalServers}</div></div>
          <div class="metric"><div class="label">Propagated</div><div class="value ok">${propagated}</div></div>
          <div class="metric"><div class="label">Failed</div><div class="value bad">${failed}</div></div>
          <div class="metric"><div class="label">Rate</div><div class="value">${rate}%</div></div>
        </div>
        ${rowsHtml}
      </div>
      <div class="panel hero">
        <h1>DNS Propagation Checker</h1>
        <p>Perform a DNS lookup for <strong>${escapeHtml(domainName)}</strong> against public recursive resolvers and compare the returned A records.</p>
        <div class="map"><span class="pin p1"></span><span class="pin p2"></span><span class="pin p3"></span><span class="pin p4"></span><span class="pin p5"></span><span class="pin p6 bad"></span></div>
        <div class="live">See Live Results: <a href="${escapeHtml(liveUrl)}">${escapeHtml(liveUrl)}</a></div>
        <div class="footer-note">Generated: ${escapeHtml(generatedAt)}. Source: direct public DNS resolver checks. WhatsMyDNS browser capture was skipped because: ${escapeHtml(reason)}</div>
      </div>
    </div>
    <div class="article">
      <h2>Global DNS Checker</h2>
      <p>This report shows whether public resolvers can currently resolve the selected A record. It is used when the live WhatsMyDNS browser page is unavailable, blocked by Cloudflare verification, or does not expose parseable result rows to automation.</p>
      <p>DNS propagation can vary by resolver cache, TTL, and regional recursive DNS behavior. Re-run the scan after DNS changes to compare the resolver answers again.</p>
    </div>
  </div>
</body>
</html>`;

    await tab.setContent(html, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await wait(500);
    await tab.screenshot({ path: screenshotPath, fullPage: false });
    return screenshotFile;
  } catch (err) {
    progressLog(`⚠️ DNS fallback screenshot failed for ${domainName}: ${err?.message || err}`);
    return null;
  } finally {
    if (tab) await tab.close().catch(() => {});
  }
}

async function runDnsResolverFallback(domainName, context, reason) {
  const timeoutMs = Math.max(1000, parseInt(process.env.DNS_RESOLVER_FALLBACK_TIMEOUT_MS || '6000', 10));
  const resolverRows = await Promise.all(
    DNS_FALLBACK_RESOLVERS.map((resolverInfo) => resolveAFromPublicResolver(domainName, resolverInfo, timeoutMs))
  );

  const totalServers = resolverRows.length;
  const propagated = resolverRows.filter((row) => row.propagated).length;
  const failed = Math.max(0, totalServers - propagated);
  const propagationRate = totalServers ? `${Math.round((propagated / totalServers) * 100)}%` : '0%';
  const screenshot = await createDnsFallbackScreenshot(domainName, resolverRows, context, reason);

  return {
    status: totalServers > 0 ? 'SUCCESS_FALLBACK' : 'ERROR',
    error: totalServers > 0 ? null : reason,
    errorCode: totalServers > 0 ? null : 'DNS_RESOLVER_FALLBACK_FAILED',
    url: `https://www.whatsmydns.net/#A/${domainName}`,
    screenshot,
    data: {
      propagated,
      totalServers,
      failed,
      propagationRate,
      source: 'index_public_dns_resolver_fallback',
      fallbackReason: reason,
      resolvers: resolverRows,
      records: [...new Set(resolverRows.flatMap((row) => row.records || []))],
    },
  };
}

async function runWhatsMyDNSWithIndexFallback(domainName, context) {
  const browserTimeoutMs = Math.max(15000, parseInt(process.env.WHATSMYDNS_BROWSER_TIMEOUT_MS || '75000', 10));
  let primaryResult = null;
  let primaryError = null;

  try {
    primaryResult = await withTimeout(
      runWhatsMyDNS(domainName, context),
      browserTimeoutMs,
      `WhatsMyDNS browser scraper for ${domainName}`
    );
  } catch (err) {
    primaryError = err;
  }

  if (dnsResultHasUsableRows(primaryResult)) {
    return primaryResult;
  }

  const reason = dnsBrowserFailureReason(primaryResult, primaryError);
  progressLog(`⚠️ WhatsMyDNS browser scrape did not return usable DNS rows for ${domainName} — ${reason}. Using index-only DNS resolver fallback.`);

  const fallbackResult = await runDnsResolverFallback(domainName, context, reason);

  // Keep a real WhatsMyDNS screenshot only when the failure was not a
  // Cloudflare/security challenge. If Cloudflare blocked automation, the captured
  // image is the human-verification page, so keep the generated DNS screenshot.
  const blockedBySecurity = /cloudflare|verify you are human|human verification|security verification|challenge/i.test(reason);
  if (primaryResult?.screenshot && !blockedBySecurity) {
    fallbackResult.screenshot = primaryResult.screenshot;
  }

  return fallbackResult;
}


function pingdomCSVRowIsComplete(row) {
  if (!row) return false;

  if (
    textLooksUnreachable(
      row.Pingdom_Status,
      row.Pingdom_Error,
      row.Pingdom_ErrorCode,
      row.Server_HTTP_Status,
      row.Server_HTTP_Code
    )
  ) {
    return true;
  }

  return (
    !isNAValue(row.Pingdom_Grade) &&
    !isNAValue(row.Pingdom_LoadTime) &&
    !isNAValue(row.Pingdom_PageSize) &&
    !isNAValue(row.Pingdom_Requests)
  );
}



function firstStatusCodeFromText(...parts) {
  const text = parts.map((v) => String(v ?? '')).join(' ');
  const m = text.match(/\b(400|401|403|404|410|418|421|422|423|424|425|426|428|429|431|444|451|500|501|502|503|504|508|509|520|521|522|523|524|525|526|527|530)\b/);
  return m ? m[1] : '';
}

function compactReason(...parts) {
  return parts
    .map((v) => String(v ?? '').trim())
    .filter(Boolean)
    .join(' — ')
    .replace(/\s+/g, ' ')
    .slice(0, 1000);
}

function detectParkedOrExpiredText(text) {
  const lowered = String(text || '').toLowerCase().replace(/\s+/g, ' ');

  const parkedPatterns = [
    'the domain has expired',
    'domain has expired',
    'this domain has expired',
    'this domain name has expired',
    'domain expired',
    'expired domain',
    'expired - click here for more information',
    'renew this domain',
    'domain renewal',
    'domain is expired',
    'this domain is parked',
    'domain parking',
    'parked domain',
    'parklogic',
    'namesilo',
    'looking for a domain?',
    'buy this domain',
    'this domain may be for sale',
    'domain for sale',
    'sedo domain parking',
    'afternic',
    'dan.com/buy-domain',
    'hugedomains.com',
  ];

  const matched = parkedPatterns.find((pat) => lowered.includes(pat));
  if (!matched) return null;

  return {
    fatal: true,
    code: 'DOMAIN_EXPIRED_OR_PARKED',
    reason: `Expired/parked domain detected (${matched})`,
  };
}

function readHttpBodySnippet(url, timeoutMs = 10000, redirectLimit = 3) {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (result) => {
      if (!settled) {
        settled = true;
        resolve(result);
      }
    };

    let parsed;
    try {
      parsed = new URL(url);
    } catch (err) {
      finish({ ok: false, url, error: err.message });
      return;
    }

    const client = parsed.protocol === 'https:' ? https : http;
    const req = client.request(
      {
        protocol: parsed.protocol,
        hostname: parsed.hostname,
        port: parsed.port || undefined,
        path: `${parsed.pathname || '/'}${parsed.search || ''}`,
        method: 'GET',
        timeout: timeoutMs,
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Connection': 'close',
        },
      },
      (res) => {
        const statusCode = res.statusCode || 0;
        const location = res.headers.location;

        if ([301, 302, 303, 307, 308].includes(statusCode) && location && redirectLimit > 0) {
          res.resume();
          let nextUrl;
          try {
            nextUrl = new URL(location, url).toString();
          } catch (_) {
            nextUrl = location;
          }
          readHttpBodySnippet(nextUrl, timeoutMs, redirectLimit - 1).then(finish);
          return;
        }

        const chunks = [];
        let total = 0;
        const maxBytes = 256 * 1024;

        res.on('data', (chunk) => {
          if (total < maxBytes) {
            chunks.push(chunk);
            total += chunk.length;
          }
          if (total >= maxBytes) {
            req.destroy();
          }
        });

        res.on('end', () => {
          const body = Buffer.concat(chunks).toString('utf8');
          const titleMatch = body.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
          const title = titleMatch ? titleMatch[1].replace(/\s+/g, ' ').trim() : '';
          const bodyText = body
            .replace(/<script[\s\S]*?<\/script>/gi, ' ')
            .replace(/<style[\s\S]*?<\/style>/gi, ' ')
            .replace(/<[^>]+>/g, ' ')
            .replace(/&nbsp;/gi, ' ')
            .replace(/&amp;/gi, '&')
            .replace(/\s+/g, ' ')
            .trim()
            .slice(0, 4000);

          finish({
            ok: true,
            url,
            statusCode,
            statusText: res.statusMessage || '',
            title,
            bodySnippet: bodyText,
            rawHtml: body,
            parkedFailure: detectParkedOrExpiredText(`${title} ${bodyText}`),
          });
        });
      }
    );

    req.on('timeout', () => {
      req.destroy(new Error('homepage content check timeout'));
    });

    req.on('error', (err) => {
      finish({ ok: false, url, error: err.message });
    });

    req.end();
  });
}

async function fetchTargetHomepageSnippet(domainName, timeoutMs = 10000) {
  const httpsResult = await readHttpBodySnippet(`https://${domainName}/`, timeoutMs, 3);
  if (httpsResult?.ok) return httpsResult;

  const httpResult = await readHttpBodySnippet(`http://${domainName}/`, timeoutMs, 3);
  if (httpResult?.ok) return httpResult;

  return httpsResult || httpResult || null;
}


function classifyFatalDomainFailure(snapshot) {
  if (!snapshot) return null;

  const text = compactReason(
    snapshot.status,
    snapshot.reason,
    snapshot.error,
    snapshot.errorCode,
    snapshot.code,
    snapshot.statusCode,
    snapshot.statusText,
    snapshot.title,
    snapshot.bodySnippet,
    snapshot.finalUrl,
    snapshot.url
  );

  const lowered = text.toLowerCase();
  const codeText = firstStatusCodeFromText(text);
  const rawStatus =
    snapshot.statusCode ||
    snapshot.httpCode ||
    snapshot.status ||
    snapshot.code ||
    snapshot.errorCode ||
    codeText ||
    '';

  const statusNum = parseInt(codeText || snapshot.statusCode || snapshot.httpCode || snapshot.status || '', 10);
  const statusLabel = String(snapshot.status || '').toUpperCase();
  const errorCode = String(snapshot.errorCode || snapshot.code || '').toUpperCase();

  // A real HTTP response means the web server exists. Login walls, cookie gates,
  // age gates, Cloudflare/WAF checks, 403/429/5xx, and scanner timeouts must be
  // reported as reachable/partial, not as a dead domain. This prevents FileMaker
  // from showing "Domain is unreachable" for sites that are actually live.
  const hasHttpResponse = Number.isFinite(statusNum) && statusNum > 0;
  const reachableStatus =
    snapshot.alive === true ||
    snapshot.uncertain === true ||
    snapshot.scannerBlocked === true ||
    statusLabel.startsWith('REACHABLE') ||
    statusLabel === 'SCAN_UNCERTAIN' ||
    statusLabel === 'DNS_UNCERTAIN' ||
    (
      hasHttpResponse &&
      statusNum !== 404 &&
      statusNum !== 410 &&
      statusNum !== 444
    );

  const accessGatePatterns = [
    'are you old enough',
    'legal drinking age',
    'verify your age',
    'age verification',
    'cookie',
    'cookies',
    'accept all',
    'i agree',
    'continue',
    'sign in',
    'login',
    'log in',
    'subscribe',
    'newsletter',
    'captcha',
    'cf-chl',
    'cloudflare',
    'checking your browser',
    'security check',
    'just a moment',
    'access denied',
    'forbidden',
    'too many requests',
    'rate limit',
    'service unavailable',
    'bad gateway',
    'gateway timeout',
    'connection timed out',
    'scan_uncertain',
    'scanner could not prove reachability',
    'waf',
    'bot-style',
  ];

  if (accessGatePatterns.some((pat) => lowered.includes(pat))) {
    return null;
  }

  if (reachableStatus) {
    return null;
  }

  // True DNS failures are still hard failures.
  if (
    statusLabel === 'DNS_FAILED' ||
    errorCode === 'ENOTFOUND' ||
    errorCode === 'ENODATA' ||
    errorCode === 'ERR_NAME_NOT_RESOLVED' ||
    lowered.includes('dns_probe_finished_nxdomain') ||
    lowered.includes('nxdomain') ||
    lowered.includes('enotfound') ||
    lowered.includes('server ip address could not be found')
  ) {
    return {
      fatal: true,
      code: errorCode || 'ERR_NAME_NOT_RESOLVED',
      reason: text || 'DNS not found',
    };
  }

  // Parked/expired domains are still hard failures.
  const parkedFailure = detectParkedOrExpiredText(text);
  if (parkedFailure) return parkedFailure;

  // Keep the hard HTTP list narrow. Do not treat Cloudflare 52x or generic 5xx
  // as dead domains; those are often bot/WAF/origin-blocked but still live.
  if (Number.isFinite(statusNum) && (statusNum === 404 || statusNum === 410 || statusNum === 444)) {
    return {
      fatal: true,
      code: String(statusNum),
      reason: text || `HTTP ${statusNum}`,
    };
  }

  // Only classify refused service as hard when the site-checker has already
  // determined that both HTTPS and HTTP refused the connection.
  if (
    snapshot.fatal === true &&
    (
      lowered.includes('server refused both https and http') ||
      lowered.includes('both https and http') ||
      lowered.includes('econnrefused')
    )
  ) {
    return {
      fatal: true,
      code: errorCode || String(rawStatus || 'ECONNREFUSED'),
      reason: text || 'server refused both HTTPS and HTTP',
    };
  }

  return null;
}

function selectedOrSkippedStatus(key) {
  return isEnabled(key) ? 'FAILED' : 'SKIPPED';
}

function makeFatalDomainResults(domainName, failure) {
  const reason = failure?.reason || 'Domain unreachable';
  const code = failure?.code || firstStatusCodeFromText(reason) || 'ERR_UNREACHABLE';
  const statusFor = (key) => selectedOrSkippedStatus(key);
  const errFor = (key) => isEnabled(key) ? reason : null;
  const codeFor = (key) => isEnabled(key) ? 'DOMAIN_UNREACHABLE' : null;

  return {
    domain: domainName,
    timestamp: new Date().toISOString(),
    ssllabs: {
      status: statusFor('ssl'), error: errFor('ssl'), errorCode: codeFor('ssl'), screenshot: '',
      data: { overallGrade: 'N/A', summary: { totalEndpoints: 0, allGrades: 'N/A' } },
      url: `https://www.ssllabs.com/ssltest/analyze.html?d=${domainName}`,
    },
    sucuri: {
      status: statusFor('sucuri'), error: errFor('sucuri'), errorCode: codeFor('sucuri'), screenshot: '',
      data: { overallStatus: 'N/A', malware: { status: 'N/A' }, blacklist: { status: 'N/A' } },
      url: `https://sitecheck.sucuri.net/results/${domainName}`,
    },
    pagespeed: {
      status: statusFor('pagespeed'), error: errFor('pagespeed'), errorCode: codeFor('pagespeed'), screenshot: '',
      data: {
        scores: { performance: 'N/A', accessibility: 'N/A', bestPractices: 'N/A', seo: 'N/A' },
        metrics: { fcp: 'N/A', lcp: 'N/A', tbt: 'N/A', cls: 'N/A', ttfb: 'N/A' },
      },
      url: `https://pagespeed.web.dev/report?url=https://${domainName}`,
    },
    pingdom: {
      status: statusFor('pingdom'), error: errFor('pingdom'), errorCode: codeFor('pingdom'), screenshot: '',
      data: { performanceGrade: 'N/A', gradeLetter: 'N/A', gradeNumber: null, loadTime: 'N/A', pageSize: 'N/A', requests: 'N/A' },
      url: `https://tools.pingdom.com/#${domainName}`,
    },
    whatsmydns: {
      status: statusFor('dns'), error: errFor('dns'), errorCode: codeFor('dns'), screenshot: '',
      data: { propagated: 'N/A', totalServers: 'N/A', failed: 'N/A', propagationRate: 'N/A%' },
      url: `https://www.whatsmydns.net/#A/${domainName}`,
    },
    pagerank: {
      status: statusFor('pagerank'), error: errFor('pagerank'), errorCode: codeFor('pagerank'),
      data: { page_rank_integer: 'N/A', page_rank_decimal: 'N/A', rank: 'N/A' },
      url: `https://www.domcop.com/openpagerank/${domainName}`,
    },
    server: {
      status: statusFor('server'), error: errFor('server'), errorCode: codeFor('server'),
      data: {
        spf: 'N/A', dmarc: 'N/A', dkim: 'N/A', domain_blacklist: 'N/A', mx: 'N/A', rbl: 'N/A',
        ip: '', broken_links: 'N/A', http: `UNREACHABLE: ${reason}`, http_code: code,
        ssl: 'N/A',
      },
    },
    intodns: {
      status: statusFor('intodns'), error: errFor('intodns'), errorCode: codeFor('intodns'), screenshot: '',
      data: { overallHealth: 'N/A', errorCount: 'N/A', warnCount: 'N/A', mxStatus: 'N/A', nsCount: 'N/A', soaSerial: 'N/A' },
      url: `https://intodns.com/${domainName}`,
    },
    indepthdns: {
      status: statusFor('indepthdns'), error: errFor('indepthdns'), errorCode: codeFor('indepthdns'), screenshot: '',
      data: {
        overallHealth: 'N/A',
        counts: { all: 'N/A', pass: 'N/A', warn: 'N/A', fail: 'N/A', info: 'N/A' },
        timing: { phase1: 'N/A', phase2: 'N/A', total: 'N/A' },
      },
      url: 'https://tool.indepthdns.com/',
    },
  };
}

async function writeResultRowArtifacts(csvRow) {
  console.log(`[index.js] Writing domain CSV...`);
  await writeDomainCSV(paths.csvPath, csvRow);
  console.log(`[index.js] ✅ Domain CSV written`);

  console.log(`[index.js] Writing per-domain summary CSV...`);
  await writeSummaryCSV(paths.domainSummaryPath, csvRow);
  console.log(`[index.js] ✅ Per-domain summary CSV updated`);

  console.log(`[index.js] Writing batch summary CSV...`);
  await writeSummaryCSV(paths.batchSummaryPath, csvRow);
  console.log(`[index.js] ✅ Batch summary CSV updated`);

  try {
    await updateLatestResults(csvRow);
    progressLog(`✅ Latest results updated for ${domain}`);
  } catch (err) {
    progressLog(`⚠️ Failed to update latest results: ${err.message}`);
  }

  const latestPathFile = path.join(OUTPUT_DIR, `latest_path_${JOB_ID}.txt`);
  fs.writeFileSync(latestPathFile, paths.domainSummaryPath);
}

function isRealScreenshotValue(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  const lowered = s.toLowerCase();
  return !['none', 'null', 'n/a', '?', 'skipped'].includes(lowered);
}

function getResultScreenshotFilenames(results) {
  const names = [
    results.ssllabs?.screenshot,
    results.sucuri?.screenshot,
    results.pagespeed?.screenshot,
    results.pingdom?.screenshot,
    results.whatsmydns?.screenshot,
    results.intodns?.screenshot,
    results.indepthdns?.screenshot,
  ]
    .filter(isRealScreenshotValue)
    .map((v) => path.basename(String(v).split('?')[0]));

  return [...new Set(names)].filter(Boolean);
}

function getCsvScreenshotUrls(csvRow) {
  return Object.entries(csvRow || {})
    .filter(([key, value]) => key.endsWith('_Screenshot') && isRealScreenshotValue(value))
    .map(([, value]) => String(value).trim())
    .filter((value) => /^https?:\/\//i.test(value));
}

function getCsvScreenshotChecks(csvRow, imagesDir) {
  return Object.entries(csvRow || {})
    .filter(([key, value]) => key.endsWith('_Screenshot') && isRealScreenshotValue(value))
    .map(([key, value]) => {
      const url = String(value || '').trim();
      // Accept both https:// (remote) and http:// (local Node server)
      if (!/^https?:\/\//i.test(url)) return null;

      let filename = '';
      try {
        filename = path.basename(new URL(url).pathname || '');
      } catch (_) {
        filename = path.basename(url.split('?')[0]);
      }

      const localPath = filename ? path.join(imagesDir, filename) : '';
      let expectedSize = 0;
      try {
        const stat = fs.statSync(localPath);
        expectedSize = stat.isFile() ? stat.size : 0;
      } catch (_) {}

      return { key, url, filename, localPath, expectedSize };
    })
    .filter(Boolean);
}

function clearScreenshotFields(csvRow, reason) {
  const row = { ...(csvRow || {}) };
  for (const key of Object.keys(row)) {
    if (key.endsWith('_Screenshot')) row[key] = 'none';
  }
  if (reason) row.Image_Sync_Warning = reason;
  return row;
}

function clearMissingLocalScreenshotFields(csvRow, missing = []) {
  const row = { ...(csvRow || {}) };
  const missingText = (missing || []).join(' ').toLowerCase();
  const mapping = [
    ['SSL_Screenshot', 'ssl'],
    ['Sucuri_Screenshot', 'sucuri'],
    ['PageSpeed_Screenshot', 'pagespeed'],
    ['Pingdom_Screenshot', 'pingdom'],
    ['DNS_Screenshot', 'dns'],
    ['IntoDNS_Screenshot', 'intodns'],
    ['InDepthDNS_Screenshot', 'indepthdns'],
  ];

  for (const [field, token] of mapping) {
    const current = String(row[field] || '').toLowerCase();
    if (!current || current === 'none') continue;
    if (missingText.includes(`${token}.png`) || missingText.includes(path.basename(current).toLowerCase())) {
      row[field] = 'none';
    }
  }
  return row;
}

async function waitForLocalScreenshots(imagesDir, results, options = {}) {
  const timeoutMs = parseInt(process.env.IMAGE_LOCAL_VERIFY_TIMEOUT_MS || options.timeoutMs || '45000', 10);
  const intervalMs = parseInt(process.env.IMAGE_LOCAL_VERIFY_INTERVAL_MS || options.intervalMs || '750', 10);
  const minBytes = parseInt(process.env.IMAGE_MIN_BYTES || options.minBytes || '1000', 10);
  const filenames = getResultScreenshotFilenames(results);

  if (!filenames.length) {
    return { ok: true, checked: 0, missing: [] };
  }

  const started = Date.now();
  let lastMissing = filenames;

  while (Date.now() - started < timeoutMs) {
    const missing = [];

    for (const filename of filenames) {
      const filePath = path.join(imagesDir, filename);
      try {
        const stat = fs.statSync(filePath);
        if (!stat.isFile() || stat.size < minBytes) {
          missing.push(`${filename} (${stat.size || 0} bytes)`);
        }
      } catch (_) {
        missing.push(filename);
      }
    }

    if (!missing.length) {
      return { ok: true, checked: filenames.length, missing: [] };
    }

    lastMissing = missing;
    await sleep(intervalMs);
  }

  return { ok: false, checked: filenames.length, missing: lastMissing };
}

function requestHeaders(url, method = 'HEAD', timeoutMs = 10000) {
  return new Promise((resolve) => {
    let parsed;
    try {
      parsed = new URL(url);
    } catch (e) {
      resolve({ ok: false, error: e.message, statusCode: 0, headers: {} });
      return;
    }

    const client = parsed.protocol === 'https:' ? https : http;
    const req = client.request(
      parsed,
      {
        method,
        timeout: timeoutMs,
        headers: {
          'Cache-Control': 'no-cache',
          Pragma: 'no-cache',
          'User-Agent': 'indepth-image-verifier/1.0',
        },
      },
      (res) => {
        res.resume();
        resolve({
          ok: res.statusCode >= 200 && res.statusCode < 300,
          statusCode: res.statusCode,
          headers: res.headers || {},
        });
      }
    );

    req.on('timeout', () => {
      req.destroy(new Error('timeout'));
    });

    req.on('error', (e) => {
      resolve({ ok: false, error: e.message, statusCode: 0, headers: {} });
    });

    req.end();
  });
}


function requestImageBytes(url, timeoutMs = 15000, maxBytes = 25 * 1024 * 1024) {
  return new Promise((resolve) => {
    let parsed;
    try {
      parsed = new URL(url);
    } catch (e) {
      resolve({ ok: false, error: e.message, statusCode: 0, headers: {}, bytes: 0 });
      return;
    }

    const client = parsed.protocol === 'https:' ? https : http;
    const req = client.request(
      parsed,
      {
        method: 'GET',
        timeout: timeoutMs,
        headers: {
          'Cache-Control': 'no-cache',
          Pragma: 'no-cache',
          'User-Agent': 'indepth-image-verifier/1.0',
        },
      },
      (res) => {
        let bytes = 0;
        res.on('data', (chunk) => {
          bytes += chunk.length;
          if (bytes > maxBytes) req.destroy(new Error('image too large'));
        });
        res.on('end', () => {
          resolve({
            ok: res.statusCode >= 200 && res.statusCode < 300,
            statusCode: res.statusCode,
            headers: res.headers || {},
            bytes,
          });
        });
      }
    );

    req.on('timeout', () => {
      req.destroy(new Error('timeout'));
    });

    req.on('error', (e) => {
      resolve({ ok: false, error: e.message, statusCode: 0, headers: {}, bytes: 0 });
    });

    req.end();
  });
}

async function isPublicImageReady(input) {
  const item = typeof input === 'string' ? { url: input } : (input || {});
  const url = item.url || '';
  const expectedSize = Number(item.expectedSize || 0);

  if (expectedSize > 0) {
    const result = await requestImageBytes(url);
    const contentType = String(result.headers['content-type'] || '').toLowerCase();
    const looksLikeImageUrl = /\.(png|jpe?g|webp|gif)(\?|$)/i.test(url);
    const sizeMatches = Number(result.bytes || 0) === expectedSize;

    return {
      ok: !!result.ok && (contentType.includes('image/') || looksLikeImageUrl) && sizeMatches,
      statusCode: result.statusCode || 0,
      contentType,
      bytes: result.bytes || 0,
      expectedSize,
      error: result.error || (sizeMatches ? '' : `remote size ${result.bytes || 0} != local size ${expectedSize}`),
    };
  }

  let result = await requestHeaders(url, 'HEAD');

  // Some servers/CDNs do not support HEAD correctly. Fall back to GET headers.
  if (!result.ok && [0, 403, 405, 501].includes(Number(result.statusCode || 0))) {
    result = await requestHeaders(url, 'GET');
  }

  const contentType = String(result.headers['content-type'] || '').toLowerCase();
  const looksLikeImageUrl = /\.(png|jpe?g|webp|gif)(\?|$)/i.test(url);

  return {
    ok: !!result.ok && (contentType.includes('image/') || looksLikeImageUrl),
    statusCode: result.statusCode || 0,
    contentType,
    expectedSize,
    error: result.error || '',
  };
}

async function waitForPublicScreenshotUrls(urls, options = {}) {
  const itemsByUrl = new Map();
  for (const raw of (urls || []).filter(Boolean)) {
    const item = typeof raw === 'string' ? { url: raw } : raw;
    if (item?.url && !itemsByUrl.has(item.url)) itemsByUrl.set(item.url, item);
  }
  const uniqueItems = [...itemsByUrl.values()];
  const timeoutMs = parseInt(process.env.IMAGE_PUBLIC_VERIFY_TIMEOUT_MS || options.timeoutMs || '60000', 10);
  const intervalMs = parseInt(process.env.IMAGE_PUBLIC_VERIFY_INTERVAL_MS || options.intervalMs || '1000', 10);

  if (!uniqueItems.length) {
    return { ok: true, checked: 0, pending: [] };
  }

  const started = Date.now();
  let lastPending = uniqueItems.map((item) => ({ url: item.url, statusCode: 0, contentType: '', expectedSize: item.expectedSize || 0, error: 'not checked yet' }));

  while (Date.now() - started < timeoutMs) {
    const checks = await Promise.all(
      uniqueItems.map(async (item) => ({ url: item.url, key: item.key, filename: item.filename, ...(await isPublicImageReady(item)) }))
    );

    const pending = checks.filter((item) => !item.ok);
    if (!pending.length) {
      return { ok: true, checked: uniqueItems.length, pending: [] };
    }

    lastPending = pending;
    await sleep(intervalMs);
  }

  return { ok: false, checked: uniqueItems.length, pending: lastPending };
}

function formatDuration(ms) {
  const totalSec = Math.round(ms / 1000);
  const mins = Math.floor(totalSec / 60);
  const secs = totalSec % 60;
  return mins === 0 ? `${secs} sec` : `${mins} min ${secs} sec`;
}

function normalizePingdomGradeForCSV(data) {
  const d = data || {};
  const isNA = (v) => v === null || v === undefined || v === '' || String(v).trim().toUpperCase() === 'N/A';
  const gradeLetterFromScore = (scoreValue) => {
    const score = parseInt(scoreValue, 10);
    if (!Number.isFinite(score)) return 'N/A';
    if (score >= 90) return 'A';
    if (score >= 80) return 'B';
    if (score >= 70) return 'C';
    if (score >= 60) return 'D';
    return 'F';
  };
  const normalizeScore = (v) => {
    if (v === null || v === undefined) return null;
    const digits = String(v).replace(/[^0-9]/g, '');
    if (!digits) return null;
    const n = parseInt(digits, 10);
    return Number.isFinite(n) && n >= 0 && n <= 100 ? n : null;
  };

  let letter = !isNA(d.gradeLetter) ? String(d.gradeLetter).trim().toUpperCase() : 'N/A';
  let score = normalizeScore(d.gradeNumber);

  if (score === null && !isNA(d.performanceGrade)) {
    const compact = String(d.performanceGrade).match(/\b([A-F][+-]?)[\s-]*(100|\d{1,2})\b/i);
    if (compact) {
      letter = compact[1].toUpperCase();
      score = normalizeScore(compact[2]);
    } else {
      const scoreOnly = String(d.performanceGrade).match(/\b(100|[1-9]\d)\b/);
      if (scoreOnly) score = normalizeScore(scoreOnly[1]);
    }
  }

  if ((isNA(letter) || letter === 'N/A') && score !== null) {
    letter = gradeLetterFromScore(score);
  }

  return {
    performanceGrade: (!isNA(letter) && score !== null) ? `${letter}${score}` : (!isNA(d.performanceGrade) ? String(d.performanceGrade).replace(/\s+/g, '') : 'N/A'),
    gradeLetter: !isNA(letter) ? letter : 'N/A',
    gradeNumber: score,
  };
}


function sanitizePingdomUrlForCSV(url, domain) {
  const fallback = `https://tools.pingdom.com/#${domain}`;
  const raw = String(url || '').trim();
  if (!raw) return fallback;
  const lowered = raw.toLowerCase();
  if (
    lowered.includes('/legal/') ||
    lowered.includes('software-services-agreement') ||
    lowered.includes('solarwinds.com/legal')
  ) {
    return fallback;
  }
  return raw;
}



function normalizeSSLDataForCSV(sslResult = {}) {
  const d = sslResult?.data && typeof sslResult.data === 'object' ? sslResult.data : {};
  const summary = d.summary && typeof d.summary === 'object' ? d.summary : {};

  const realGradeRe = /^(A\+|A-|A|B|C|D|E|F|T|M)$/;
  const firstGrade = (...values) => {
    for (const value of values) {
      if (isNAValue(value)) continue;
      const m = String(value).trim().match(/\b(A\+|A-|A|B|C|D|E|F|T|M)\b/i);
      if (m && realGradeRe.test(m[1].toUpperCase())) return m[1].toUpperCase();
    }
    return 'N/A';
  };

  const endpoints = Array.isArray(d.endpoints) ? d.endpoints : [];
  const endpointGrades = endpoints
    .map(ep => firstGrade(ep.grade, ep.statusMessage))
    .filter(g => g !== 'N/A');

  const overallGrade = firstGrade(
    d.overallGrade,
    d.grade,
    d.ratingGrade,
    d.pageGrade,
    summary.overallGrade,
    summary.allGrades,
    endpointGrades.join(', ')
  );

  const totalEndpoints = Number(summary.totalEndpoints ?? d.totalEndpoints ?? endpoints.length ?? 0) || endpoints.length || 0;
  const allGrades = firstRealValue(
    summary.allGrades,
    d.allGrades,
    endpointGrades.length ? [...new Set(endpointGrades)].join(', ') : '',
    overallGrade !== 'N/A' ? overallGrade : ''
  ) || 'N/A';

  return { overallGrade, totalEndpoints, allGrades };
}


function normalizeWordPressDetectionForCSV(wordpress = {}) {
  const verdict = String(wordpress.verdict || 'unknown').trim().toLowerCase();
  const confidence = String(wordpress.confidence || 'none').trim().toLowerCase();
  const score = Number(wordpress.score || 0) || 0;
  const markerCount = Number(
    wordpress.markerCount ??
    wordpress.evidenceCount ??
    (Array.isArray(wordpress.evidence) ? wordpress.evidence.length : 0) ??
    0
  ) || 0;

  // FileMaker boolean policy:
  // 1 = confirmed, wordpress, or likely WordPress
  // 0 = not detected or only low-confidence possible marker
  // empty = unknown/error/no HTML/skipped
  let isWordPress = '';
  if (
    verdict === 'confirmed_wordpress' ||
    verdict === 'wordpress' ||
    verdict === 'likely_wordpress'
  ) {
    isWordPress = 1;
  } else if (verdict === 'not_detected' || verdict === 'possible_wordpress') {
    isWordPress = 0;
  }

  let evidenceText = '';
  try {
    evidenceText = Array.isArray(wordpress.evidence)
      ? wordpress.evidence
          .map((item) => `${item.marker || ''}: ${item.detail || ''}`.trim())
          .filter(Boolean)
          .join(' | ')
      : '';
  } catch (_) {
    evidenceText = '';
  }

  return {
    isWordPress,
    verdict,
    confidence,
    score,
    markerCount,
    finalUrl: wordpress.finalUrl || '',
    error: wordpress.error || '',
    evidenceText,
  };
}

function buildCSVRow(results, paths, totalTimeStr) {
  const syncEnabled = String(process.env.IMAGE_SYNC_ENABLED || 'false').toLowerCase() === 'true';
  const syncBaseUrl = (process.env.IMAGE_SYNC_BASE_URL || '').replace(/\/$/, '');

  // Stable cache buster per run so browser does not reuse old remote images.
  const imageVersion = new Date(results.timestamp || Date.now()).getTime();

  // Build the local server base URL for serving images via the /images/ endpoint.
  // When sync is disabled FileMaker still needs an http:// URL it can load —
  // the Node server already serves GET /images/{domain}/{file.png}.
  const localServerHost = process.env.IMAGE_LOCAL_SERVER_HOST || process.env.SERVER_HOST || '127.0.0.1';
  const localServerPort = process.env.IMAGE_LOCAL_SERVER_PORT || process.env.PORT || '3000';
  const localImageBaseUrl = `http://${localServerHost}:${localServerPort}/images`;

  const screenshotPath = (filename) => {
    if (!filename) return 'none';

    if (syncEnabled && syncBaseUrl) {
      // Remote sync mode: return the public CDN/rsync URL
      return `${syncBaseUrl}/${results.domain}/${filename}?v=${imageVersion}`;
    }

    // Local mode: return a proper http:// URL pointing to the Node /images/ endpoint
    // so FileMaker can load it directly. Raw filesystem paths cannot be loaded by FM.
    return `${localImageBaseUrl}/${results.domain}/${filename}?v=${imageVersion}`;
  };

  const pingdomData = normalizePingdomDataFields(results.pingdom?.data || {});
  const pingdomGrade = normalizePingdomGradeForCSV(pingdomData);
  const sslCSV = normalizeSSLDataForCSV(results.ssllabs || {});
  const wpCSV = normalizeWordPressDetectionForCSV(results.wordpress || {});
  const indepthdnsData = results.indepthdns?.data || {};
  const indepthdnsCounts = indepthdnsData.counts || {};
  const indepthdnsTiming = indepthdnsData.timing || {};

  return {
    Domain: results.domain,
    Run_At: results.timestamp,
    Total_Time: totalTimeStr,
    Browser_Alive: reachabilitySnapshot ? (reachabilitySnapshot.alive ? 1 : 0) : '',
    Browser_Status: reachabilitySnapshot ? (reachabilitySnapshot.alive ? 'REACHABLE' : 'UNREACHABLE') : '',
    Browser_Error: reachabilitySnapshot && !reachabilitySnapshot.alive ? (reachabilitySnapshot.reason || reachabilitySnapshot.error || reachabilitySnapshot.errorCode || '') : '',
    Final_URL: reachabilitySnapshot?.finalUrl || reachabilitySnapshot?.url || '',

    SSL_Status: results.ssllabs.status || 'SUCCESS',
    SSL_Error: results.ssllabs.error || null,
    SSL_ErrorCode: results.ssllabs.errorCode || null,
    SSL_Grade: sslCSV.overallGrade || 'N/A',
    SSL_Endpoints: sslCSV.totalEndpoints || 0,
    SSL_AllGrades: sslCSV.allGrades || 'N/A',
    SSL_URL: results.ssllabs.url || `https://www.ssllabs.com/ssltest/analyze.html?d=${results.domain}`,
    SSL_Screenshot: screenshotPath(results.ssllabs.screenshot),

    Sucuri_Status: results.sucuri.status || 'SUCCESS',
    Sucuri_Error: results.sucuri.error || null,
    Sucuri_ErrorCode: results.sucuri.errorCode || null,
    Sucuri_Overall: results.sucuri.data?.overallStatus || 'UNKNOWN',
    Sucuri_Malware: results.sucuri.data?.malware?.status || '⚠️ Scan Failed',
    Sucuri_Blacklist: results.sucuri.data?.blacklist?.status || '⚠️ Scan Failed',
    Sucuri_URL: results.sucuri.url || `https://sitecheck.sucuri.net/results/${results.domain}`,
    Sucuri_Screenshot: screenshotPath(results.sucuri.screenshot),

    PageSpeed_Status: results.pagespeed?.status || 'SKIPPED',
    PageSpeed_Error: results.pagespeed?.error || null,
    PageSpeed_ErrorCode: getErrorCode({ error: results.pagespeed?.error }) || null,
    PageSpeed_Performance: results.pagespeed?.data?.scores?.performance || 'N/A',
    PageSpeed_Accessibility: results.pagespeed?.data?.scores?.accessibility || 'N/A',
    PageSpeed_BestPractices: results.pagespeed?.data?.scores?.bestPractices || 'N/A',
    PageSpeed_SEO: results.pagespeed?.data?.scores?.seo || 'N/A',
    PageSpeed_LCP: results.pagespeed?.data?.metrics?.lcp || 'N/A',
    PageSpeed_CLS: results.pagespeed?.data?.metrics?.cls || 'N/A',
    PageSpeed_TBT: results.pagespeed?.data?.metrics?.tbt || 'N/A',
    PageSpeed_TTFB: results.pagespeed?.data?.metrics?.ttfb || 'N/A',
    PageSpeed_FCP: results.pagespeed?.data?.metrics?.fcp || 'N/A',
    PageSpeed_URL: results.pagespeed?.url || `https://pagespeed.web.dev/report?url=https://${results.domain}`,
    PageSpeed_Screenshot: screenshotPath(results.pagespeed?.screenshot),

    Pingdom_Status: results.pingdom.status || 'SUCCESS',
    Pingdom_Error: results.pingdom.error || null,
    Pingdom_ErrorCode: results.pingdom.errorCode || null,
    Pingdom_Grade: pingdomGrade.performanceGrade || 'N/A',
    Pingdom_GradeLetter: pingdomGrade.gradeLetter || 'N/A',
    Pingdom_GradeNumber: pingdomGrade.gradeNumber || null,
    Pingdom_LoadTime: pingdomData.loadTime || 'N/A',
    Pingdom_PageSize: pingdomData.pageSize || 'N/A',
    Pingdom_Requests: pingdomData.requests || 'N/A',
    Pingdom_URL: sanitizePingdomUrlForCSV(results.pingdom.url, results.domain),
    Pingdom_Screenshot: screenshotPath(results.pingdom.screenshot),

    DNS_Status: results.whatsmydns.status || 'SUCCESS',
    DNS_Error: results.whatsmydns.error || null,
    DNS_ErrorCode: results.whatsmydns.errorCode || null,
    DNS_Propagation: `${results.whatsmydns.data?.propagated || 0}/${results.whatsmydns.data?.totalServers || 0}`,
    DNS_TotalServers: results.whatsmydns.data?.totalServers || 0,
    DNS_Propagated: results.whatsmydns.data?.propagated || 0,
    DNS_Failed: results.whatsmydns.data?.failed || 0,
    DNS_PropagationRate: results.whatsmydns.data?.propagationRate || '0%',
    DNS_URL: results.whatsmydns.url || `https://www.whatsmydns.net/#A/${results.domain}`,
    DNS_Screenshot: screenshotPath(results.whatsmydns.screenshot),

    PageRank_Status: results.pagerank?.status || 'SKIPPED',
    PageRank_Error: results.pagerank?.error || null,
    PageRank_ErrorCode: getErrorCode({ error: results.pagerank?.error }) || null,
    PageRank_Integer: results.pagerank?.data?.page_rank_integer ?? '',
    PageRank_Decimal: results.pagerank?.data?.page_rank_decimal ?? '',
    PageRank_Rank: results.pagerank?.data?.rank ?? '',
    PageRank_URL: results.pagerank?.url || `https://www.domcop.com/openpagerank/${results.domain}`,

    Server_SPF_Status: results.server?.data?.spf || 'Missing',
    Server_SPF_Value: results.server?.data?.spf || '',
    Server_DMARC_Status: results.server?.data?.dmarc || 'Missing',
    Server_DMARC_Value: results.server?.data?.dmarc || '',
    Server_DKIM_Status: results.server?.data?.dkim || 'Missing',
    Server_DKIM_Value: results.server?.data?.dkim || '',
    Server_DomainBlacklist_Status: results.server?.data?.domain_blacklist || 'Missing',
    Server_DomainBlacklist_Value: results.server?.data?.domain_blacklist || '',
    Server_MX_Status: results.server?.data?.mx || 'Missing',
    Server_MX_Value: results.server?.data?.mx || '',
    Server_RBL_Status: results.server?.data?.rbl || 'Missing',
    Server_RBL_Value: results.server?.data?.rbl || '',
    Server_IP_Address: results.server?.data?.ip || '',
    Server_BrokenLinks_Status: results.server?.data?.broken_links || 'Missing',
    Server_BrokenLinks_Value: results.server?.data?.broken_links || '',
    Server_HTTP_Status: results.server?.data?.http || (reachabilitySnapshot && !reachabilitySnapshot.alive ? `UNREACHABLE: ${reachabilitySnapshot.reason || reachabilitySnapshot.error || 'browser could not load domain'}` : 'Missing'),
    Server_HTTP_Code: results.server?.data?.http_code || (reachabilitySnapshot && !reachabilitySnapshot.alive ? (reachabilitySnapshot.statusCode || reachabilitySnapshot.code || reachabilitySnapshot.errorCode || reachabilitySnapshot.reason || 'ERR_UNREACHABLE') : ''),
    Server_SSL_Status: results.server?.data?.ssl || 'Missing',
    Server_SSL_Value: results.server?.data?.ssl || '',

    IntoDNS_OverallHealth: results.intodns.data?.overallHealth || 'N/A',
    IntoDNS_ErrorCount: results.intodns.data?.errorCount || 0,
    IntoDNS_WarnCount: results.intodns.data?.warnCount || 0,
    IntoDNS_MX_Status: results.intodns.data?.mxStatus || 'UNKNOWN',
    IntoDNS_NS_Count: results.intodns.data?.nsCount || 0,
    IntoDNS_SOA_Serial: results.intodns.data?.soaSerial || '',
    IntoDNS_URL: results.intodns.url || `https://intodns.com/${results.domain}`,
    IntoDNS_Screenshot: screenshotPath(results.intodns.screenshot),

    InDepthDNS_Status: results.indepthdns?.status || 'SKIPPED',
    InDepthDNS_Error: results.indepthdns?.error || null,
    InDepthDNS_ErrorCode: results.indepthdns?.errorCode || getErrorCode({ error: results.indepthdns?.error }) || null,
    InDepthDNS_OverallHealth: indepthdnsData.overallHealth || 'N/A',
    InDepthDNS_All: indepthdnsCounts.all ?? 'N/A',
    InDepthDNS_Pass: indepthdnsCounts.pass ?? 'N/A',
    InDepthDNS_Warn: indepthdnsCounts.warn ?? 'N/A',
    InDepthDNS_Fail: indepthdnsCounts.fail ?? 'N/A',
    InDepthDNS_Info: indepthdnsCounts.info ?? 'N/A',
    InDepthDNS_Phase1Time: indepthdnsTiming.phase1 || 'N/A',
    InDepthDNS_Phase2Time: indepthdnsTiming.phase2 || 'N/A',
    InDepthDNS_TotalElapsed: indepthdnsTiming.total || 'N/A',
    InDepthDNS_URL: results.indepthdns?.url || 'https://tool.indepthdns.com/',
    InDepthDNS_Screenshot: screenshotPath(results.indepthdns?.screenshot),

    // New fields appended at the end so existing FileMaker import mappings by
    // column order keep SSL/Sucuri/PageSpeed/Pingdom/DNS fields aligned.
    IsWordPress: wpCSV.isWordPress,
    WordPress_Verdict: wpCSV.verdict,
    WordPress_Confidence: wpCSV.confidence,
    WordPress_Score: wpCSV.score,
    WordPress_MarkerCount: wpCSV.markerCount,
    WordPress_Final_URL: wpCSV.finalUrl,
    WordPress_Error: wpCSV.error,
    WordPress_Evidence: wpCSV.evidenceText,
  };
}

function printSummary(results, paths, csvRow, totalTimeStr) {
  const line = '─'.repeat(55);
  console.log(`\n${'═'.repeat(55)}`);
  console.log(`✅  AUDIT COMPLETE — ${results.domain}`);
  console.log(`⏱️   Total time: ${totalTimeStr}`);
  console.log(line);
  console.log(
    `🔒 SSL Lab        : ${csvRow.SSL_Grade}  (${csvRow.SSL_Endpoints} endpoint${csvRow.SSL_Endpoints !== 1 ? 's' : ''})`
  );
  console.log(`🛡️  Sucuri         : ${csvRow.Sucuri_Overall}`);
  console.log(
    `📈 PageSpeed      : Perf ${csvRow.PageSpeed_Performance}  Access ${csvRow.PageSpeed_Accessibility}  Best ${csvRow.PageSpeed_BestPractices}  SEO ${csvRow.PageSpeed_SEO}`
  );
  console.log(`📊 Pingdom        : ${csvRow.Pingdom_Grade}  |  Load: ${csvRow.Pingdom_LoadTime}`);
  console.log(
    `🌐 WhatsMyDNS     : ${csvRow.DNS_Propagation} servers (${csvRow.DNS_PropagationRate})`
  );
  console.log(`🔍 OnePageRank    : ${csvRow.PageRank_Integer ?? 'N/A'}`);
  console.log(
    `🖥️  Server Check   : IP ${csvRow.Server_IP_Address || 'N/A'}  SPF ${csvRow.Server_SPF_Status}  DMARC ${csvRow.Server_DMARC_Status}  MX ${csvRow.Server_MX_Status}`
  );
  console.log(
    `🌐 IntoDNS        : ${csvRow.IntoDNS_OverallHealth} (Errors: ${csvRow.IntoDNS_ErrorCount}, Warnings: ${csvRow.IntoDNS_WarnCount})`
  );
  console.log(
    `🧭 INDepthDNS     : ${csvRow.InDepthDNS_OverallHealth} (PASS: ${csvRow.InDepthDNS_Pass}, WARN: ${csvRow.InDepthDNS_Warn}, FAIL: ${csvRow.InDepthDNS_Fail}, INFO: ${csvRow.InDepthDNS_Info})`
  );
  console.log(line);
  console.log(`📁 Results saved to:`);
  console.log(`   Domain CSV: ${paths.csvPath}`);
  console.log(`   Domain Summary: ${paths.domainSummaryPath}`);
  console.log(`   Batch Summary : ${paths.batchSummaryPath}`);
  console.log(`   Images: ${paths.imagesDir}`);
  console.log(`${'═'.repeat(55)}\n`);
}

async function main() {
  const totalStart = Date.now();
  let browser = null;
  let release = null;

  try {
    try {
      fs.writeFileSync(LOG_FILE, '', 'utf8');
    } catch (_) {}

    progressLog(`Starting audit for: ${domain}`);

    console.clear();
    console.log(`\n▶  Checking: ${domain}`);
    console.log(`   ${'─'.repeat(50)}`);
    TOOLS.forEach((t, i) => console.log(`   ${i + 1}  ${t.label.padEnd(16)} ⏳ processing...`));
    console.log(`   ${'─'.repeat(50)}`);
    console.log(`   0 / ${TOTAL_TOOLS} complete\n`);

    progressLog(`Checking if ${domain} is reachable...`);
    writeProgress(0, TOTAL_TOOLS || 1, domain, '', `CHECKING — ${domain}`);

    const reachability = await checkSiteReachable(domain, 15000);
    reachabilitySnapshot = reachability;

    let homepageCheck = null;
    const homepageTimeoutMs = Number(process.env.HOMEPAGE_CONTENT_TIMEOUT_MS || 12000);

    if (process.env.SKIP_HOMEPAGE_CONTENT_CHECK === '1') {
      progressLog('Skipping homepage content check because SKIP_HOMEPAGE_CONTENT_CHECK=1');
    } else {
      progressLog(`Starting homepage content check with timeout ${homepageTimeoutMs}ms`);
      try {
        homepageCheck = await Promise.race([
          fetchTargetHomepageSnippet(domain, homepageTimeoutMs),
          new Promise((resolve) => {
            setTimeout(() => {
              resolve({
                ok: false,
                timedOut: true,
                reason: `homepage content check timed out after ${homepageTimeoutMs}ms`,
              });
            }, homepageTimeoutMs + 1000);
          }),
        ]);

        if (homepageCheck?.ok) {
          progressLog(`Homepage content check: HTTP ${homepageCheck.statusCode || 'N/A'} ${homepageCheck.url || ''}`);
        } else if (homepageCheck?.timedOut) {
          progressLog('⚠️ Homepage content check timeout; continuing external tools');
        } else {
          progressLog('⚠️ Homepage content check did not return OK; continuing external tools');
        }
      } catch (homepageErr) {
        progressLog(`⚠️ Homepage content check failed: ${homepageErr.message}; continuing external tools`);
      }
    }

    const mergedReachability = {
      ...(reachability || {}),
      title: homepageCheck?.title || reachability?.title,
      bodySnippet: homepageCheck?.bodySnippet || reachability?.bodySnippet,
      finalUrl: homepageCheck?.url || reachability?.finalUrl || reachability?.url,
      statusCode: homepageCheck?.statusCode || reachability?.statusCode || reachability?.httpCode,
      statusText: homepageCheck?.statusText || reachability?.statusText,
    };

    let wordpressCheck = {
      inputUrl: domain,
      checkedUrl: domain,
      finalUrl: mergedReachability.finalUrl || '',
      status: mergedReachability.statusCode || null,
      verdict: 'unknown',
      confidence: 'none',
      score: 0,
      markerCount: 0,
      evidence: [],
      error: '',
    };

    if (String(process.env.WORDPRESS_DETECT_ENABLED || '1').trim() !== '0') {
      const wpInput = mergedReachability.finalUrl || reachability?.finalUrl || reachability?.url || domain;
      progressLog(`Checking WordPress markers for ${domain}...`);
      try {
        // Prefer the raw HTML already fetched by fetchTargetHomepageSnippet so we
        // avoid a second cold HTTP request. That second request fails on Cloudflare-
        // protected or JS-gated sites and always returns not_detected, even for real
        // WordPress installs. The homepage fetch uses the same User-Agent and follows
        // redirects, so its HTML is the most reliable source of WP markers.
        const cachedHtml = homepageCheck?.rawHtml || '';
        const MIN_HTML_BYTES = 500;
        if (cachedHtml.length >= MIN_HTML_BYTES) {
          const wpDetectorModule = require('./utils/detect-wordpress');
          if (typeof wpDetectorModule.detectWordPressFromHtml === 'function') {
            progressLog(`WordPress check: using cached homepage HTML (${cachedHtml.length} bytes)`);
            const detection = wpDetectorModule.detectWordPressFromHtml(cachedHtml, {
              finalUrl: mergedReachability.finalUrl || homepageCheck?.url || wpInput || domain,
            });
            wordpressCheck = {
              inputUrl: wpInput || domain,
              checkedUrl: wpInput || domain,
              finalUrl: mergedReachability.finalUrl || homepageCheck?.url || '',
              status: mergedReachability.statusCode || homepageCheck?.statusCode || null,
              ...detection,
              error: detection?.error || null,
            };
          } else {
            progressLog('WordPress check: cached HTML parser not exported; falling back to full detector');
            wordpressCheck = await detectWordPress(wpInput || domain);
          }
        } else {
          // Fallback: no cached HTML (SKIP_HOMEPAGE_CONTENT_CHECK=1, timed out, or
          // the response was too short). Make a fresh request via detectWordPress().
          progressLog(`WordPress check: cached HTML not available (${cachedHtml.length} bytes), fetching fresh`);
          wordpressCheck = await detectWordPress(wpInput || domain);
        }
        progressLog(
          `WordPress check: ${wordpressCheck.verdict} ` +
          `(${wordpressCheck.confidence}, score ${wordpressCheck.score || 0}, markers ${wordpressCheck.markerCount || 0})`
        );
      } catch (wpErr) {
        wordpressCheck = {
          ...wordpressCheck,
          verdict: 'unknown',
          confidence: 'none',
          error: wpErr?.message || String(wpErr),
        };
        progressLog(`⚠️ WordPress check failed for ${domain}: ${wordpressCheck.error}`);
      }
    } else {
      wordpressCheck.verdict = 'skipped';
      wordpressCheck.confidence = 'none';
      wordpressCheck.error = 'WORDPRESS_DETECT_ENABLED=0';
      progressLog('WordPress check skipped because WORDPRESS_DETECT_ENABLED=0');
    }

    writeWordPressEarlyResult(wordpressCheck);

    const parkedFailure = homepageCheck?.parkedFailure
      ? {
          ...homepageCheck.parkedFailure,
          reason: `${homepageCheck.parkedFailure.reason} — ${homepageCheck.url || domain}`,
        }
      : null;
    const fatalFailure = parkedFailure || classifyFatalDomainFailure(mergedReachability);

    if (fatalFailure) {
      progressLog(`❌ Hard domain failure detected before tools: ${domain} — ${fatalFailure.reason}`);
      progressLog(`➡️ Skipping external tool scans and writing one real unreachable CSV row.`);

      const totalMs = Date.now() - totalStart;
      const totalTimeStr = formatDuration(totalMs);
      const fatalResults = makeFatalDomainResults(domain, fatalFailure);
      fatalResults.wordpress = wordpressCheck;
      const csvRow = buildCSVRow(fatalResults, paths, totalTimeStr);

      if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
      await writeResultRowArtifacts(csvRow);

      const finishTime = new Date().toLocaleString('en-US', {
        month: '2-digit',
        day: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true,
      });

      writeProgress(
        TOTAL_TOOLS || 1,
        TOTAL_TOOLS || 1,
        domain,
        finishTime,
        `DONE_UNREACHABLE — ${domain} — ${fatalFailure.reason}`
      );

      progressLog(`✅ Unreachable result row written for ${domain}; FileMaker should mark IsActive = 0.`);
      printSummary(fatalResults, paths, csvRow, totalTimeStr);
      return;
    }

    progressLog(`✅ ${domain} is reachable (${reachability.reason || reachability.status || 'OK'}) — starting full scan`);

    const browserToolKeys = ['ssl', 'sucuri', 'pagespeed', 'pingdom', 'dns', 'pagerank', 'intodns', 'indepthdns'];
    const needsBrowser = browserToolKeys.some((key) => isEnabled(key));

    let newTab = null;

    if (needsBrowser) {
      const browserResult = await acquireBrowser();
      browser = browserResult.browser;
      release = browserResult.release;
      newTab = () => createNewPage(browser);
      progressLog('Browser acquired');
    } else {
      progressLog('No browser needed for selected tools');
    }

    const origLog = console.log;
    const origWarn = console.warn;
    if (!DEBUG) {
      console.log = () => {};
      console.warn = () => {};
    }

    const context = {
      domain,
      paths,
      DEBUG,
      wait,
      browser,
      newTab,
      sslDelay,
      env: process.env,
      reachabilitySnapshot,
    };

    async function track(key, resultFn, labelFn) {
      const start = Date.now();
      const timeoutMs = toolTimeoutMs(key);
      const attempts = toolAttempts(key);
      const retryDelayMs = toolRetryDelayMs(key);
      let lastError = null;
      let lastResult = null;

      const restoreConsole = () => {
        if (!DEBUG) {
          console.log = origLog;
          console.warn = origWarn;
        }
      };

      const silenceConsole = () => {
        if (!DEBUG) {
          console.log = () => {};
          console.warn = () => {};
        }
      };

      for (let attempt = 1; attempt <= attempts; attempt++) {
        try {
          if (attempt > 1) {
            restoreConsole();
            progressLog(`🔁 Retrying ${key} for ${domain} — attempt ${attempt}/${attempts}`);
            silenceConsole();
          }

          const r = await withTimeout(resultFn(), timeoutMs, `${key} attempt ${attempt}/${attempts}`);
          lastResult = r;

          if (toolResultLooksFailed(r) && attempt < attempts) {
            restoreConsole();
            progressLog(`⚠️ ${key} returned ${r?.status || 'FAILED'} for ${domain}; retrying only this tool after ${Math.round(retryDelayMs / 1000)}s (${attempt}/${attempts})`);
            silenceConsole();
            if (retryDelayMs > 0) await sleep(retryDelayMs);
            continue;
          }

          restoreConsole();
          const label = labelFn(r);
          markDone(key, label, start);
          if (toolResultLooksFailed(r)) {
            progressLog(`⚠️ ${key} final fallback after ${attempt}/${attempts} attempt(s) — ${label}`);
          } else {
            progressLog(`✅ ${key} done — ${label}`);
          }
          silenceConsole();

          if (global.gc && process.argv.includes('--expose-gc')) {
            setTimeout(() => global.gc(), 100);
          }

          return { status: 'fulfilled', value: r };
        } catch (e) {
          lastError = e;
          restoreConsole();
          progressLog(`❌ ${key} attempt ${attempt}/${attempts} failed — ${e?.message || 'unknown error'}`);
          silenceConsole();

          if (attempt < attempts) {
            if (retryDelayMs > 0) await sleep(retryDelayMs);
            continue;
          }
        }
      }

      restoreConsole();
      markDone(key, '❌ fallback', start);
      progressLog(`❌ ${key} failed after ${attempts} attempt(s). Writing fallback for this tool only — ${lastError?.message || lastResult?.error || 'unknown error'}`);
      silenceConsole();

      return {
        status: 'fulfilled',
        value: {
          ...(lastResult || {}),
          status: 'FAILED',
          error: lastError?.message || lastResult?.error || `${key} failed after ${attempts} attempt(s)`,
          errorCode: `${String(key || 'tool').toUpperCase()}_FAILED_AFTER_RETRY`,
          data: (lastResult && typeof lastResult.data === 'object') ? lastResult.data : {},
          tool: key,
        },
      };
    }

    const toolRunMode = String(process.env.TOOL_RUN_MODE || 'parallel').toLowerCase();
    const runToolTask = async (task) => (
      task.enabled
        ? track(task.key, task.fn, task.label)
        : { status: 'fulfilled', value: skippedResult(task.key) }
    );

    const toolTasks = [
      {
        key: 'ssl',
        enabled: isEnabled('ssl'),
        fn: () => runSSLLabs(domain, context),
        label: (r) => `Grade: ${r?.data?.overallGrade || 'N/A'}`,
      },
      {
        key: 'sucuri',
        enabled: isEnabled('sucuri'),
        fn: () => runSucuri(domain, context),
        label: (r) => r?.data?.overallStatus || 'UNKNOWN',
      },
      {
        key: 'pagespeed',
        enabled: isEnabled('pagespeed'),
        fn: () => runPageSpeed(domain, context),
        label: (r) => `Perf: ${r?.data?.scores?.performance ?? 'N/A'}`,
      },
      {
        key: 'pingdom',
        enabled: isEnabled('pingdom'),
        fn: () => runPingdomWithRequiredData(domain, context),
        label: (r) => `Grade: ${r?.data?.performanceGrade || r?.data?.gradeLetter || 'N/A'} | Load: ${r?.data?.loadTime || 'N/A'}`,
      },
      {
        key: 'dns',
        enabled: isEnabled('dns'),
        fn: () => runWhatsMyDNSWithIndexFallback(domain, context),
        label: (r) => `${r?.data?.propagated ?? '?'}/${r?.data?.totalServers ?? '?'} (${r?.data?.propagationRate || '0%'})`,
      },
      {
        key: 'pagerank',
        enabled: isEnabled('pagerank'),
        fn: () => runPageRank(domain, context),
        label: (r) => `PR: ${r?.data?.page_rank_integer ?? 'N/A'}`,
      },
      {
        key: 'server',
        enabled: isEnabled('server'),
        fn: () => runServerChecks(domain, context),
        label: (r) => `IP: ${r?.data?.ip || 'N/A'}`,
      },
      {
        key: 'intodns',
        enabled: isEnabled('intodns'),
        fn: () => runIntoDNS(domain, context),
        label: (r) => `Health: ${r?.data?.overallHealth || 'N/A'}`,
      },
      {
        key: 'indepthdns',
        enabled: isEnabled('indepthdns'),
        fn: () => runInDepthDNS(domain, context),
        label: (r) => {
          const counts = r?.data?.counts || {};
          return `Health: ${r?.data?.overallHealth || 'N/A'} | PASS:${counts.pass ?? 'N/A'} WARN:${counts.warn ?? 'N/A'} FAIL:${counts.fail ?? 'N/A'}`;
        },
      },
    ];

    let toolResults;
    if (toolRunMode === 'sequential') {
      progressLog(`ℹ️ Tool run mode: sequential`);
      toolResults = [];
      for (const task of toolTasks) {
        toolResults.push(await runToolTask(task));
      }
    } else {
      progressLog(`ℹ️ Tool run mode: parallel`);
      toolResults = await Promise.all(toolTasks.map(runToolTask));
    }

    const [
      sslResult,
      sucuriResult,
      pagespeedResult,
      pingdomResult,
      dnsResult,
      pagerankResult,
      serverResult,
      intodnsResult,
      indepthdnsResult,
    ] = toolResults;

    console.log = origLog;
    console.warn = origWarn;

    const results = {
      domain,
      timestamp: new Date().toISOString(),
      wordpress: wordpressCheck,
      ssllabs:
        sslResult.status === 'fulfilled'
          ? sslResult.value
          : { status: 'FAILED', error: sslResult.reason?.message, data: {} },
      sucuri:
        sucuriResult.status === 'fulfilled'
          ? sucuriResult.value
          : { status: 'FAILED', error: sucuriResult.reason?.message, data: {} },
      pagespeed:
        pagespeedResult.status === 'fulfilled'
          ? pagespeedResult.value
          : { status: 'FAILED', error: pagespeedResult.reason?.message, data: {} },
      pingdom:
        pingdomResult.status === 'fulfilled'
          ? pingdomResult.value
          : { status: 'FAILED', error: pingdomResult.reason?.message, data: {} },
      whatsmydns:
        dnsResult.status === 'fulfilled'
          ? dnsResult.value
          : { status: 'FAILED', error: dnsResult.reason?.message, data: {} },
      pagerank:
        pagerankResult.status === 'fulfilled'
          ? pagerankResult.value
          : { status: 'FAILED', error: pagerankResult.reason?.message, data: {} },
      server:
        serverResult.status === 'fulfilled'
          ? serverResult.value
          : { status: 'FAILED', error: serverResult.reason?.message, data: {} },
      intodns:
        intodnsResult.status === 'fulfilled'
          ? intodnsResult.value
          : { status: 'FAILED', error: intodnsResult.reason?.message, data: {} },
      indepthdns:
        indepthdnsResult.status === 'fulfilled'
          ? indepthdnsResult.value
          : { status: 'FAILED', error: indepthdnsResult.reason?.message, data: {} },
    };

    progressLog('All services done — preparing screenshots and CSV...');

    const totalMs = Date.now() - totalStart;
    const totalTimeStr = formatDuration(totalMs);

    let csvRow = buildCSVRow(results, paths, totalTimeStr);

    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

    if (isEnabled('pingdom') && !pingdomCSVRowIsComplete(csvRow)) {
      progressLog(`⚠️ Pingdom CSV values incomplete for ${domain}; writing Pingdom fallback N/A values and continuing.`);
    }

    const syncEnabled = String(process.env.IMAGE_SYNC_ENABLED || 'false').toLowerCase() === 'true';
    const syncBaseUrl = (process.env.IMAGE_SYNC_BASE_URL || '').replace(/\/$/, '');
    let remoteScreenshotUrls = getCsvScreenshotChecks(csvRow, paths.imagesDir);

    console.log(`[index.js] CSV Row built for: ${domain}`);
    console.log(`[index.js] Domain CSV path: ${paths.csvPath}`);
    console.log(`[index.js] Domain summary path: ${paths.domainSummaryPath}`);
    console.log(`[index.js] Batch summary path: ${paths.batchSummaryPath}`);

    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    const localCheckStart = new Date().toLocaleString('en-US', {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true,
    });

    writeProgress(
      TOTAL_TOOLS || 1,
      TOTAL_TOOLS || 1,
      domain,
      localCheckStart,
      `VERIFYING_LOCAL_IMAGES — ${domain} at ${localCheckStart}`
    );
    progressLog(`⏳ Verifying local screenshots for ${domain}...`);

    const localImageCheck = await waitForLocalScreenshots(paths.imagesDir, results);
    if (!localImageCheck.ok) {
      progressLog(`⚠️ Local screenshot files are not ready: ${localImageCheck.missing.join(', ')} — removing missing screenshot URLs from CSV.`);
      csvRow = clearMissingLocalScreenshotFields(csvRow, localImageCheck.missing);
      remoteScreenshotUrls = getCsvScreenshotChecks(csvRow, paths.imagesDir);
    } else {
      progressLog(`✅ Local screenshots ready (${localImageCheck.checked} checked)`);
    }

    let syncSucceeded = false;

    if (syncEnabled && syncBaseUrl && remoteScreenshotUrls.length) {
      const syncStartTime = new Date().toLocaleString('en-US', {
        month: '2-digit',
        day: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true,
      });

      writeProgress(
        TOTAL_TOOLS || 1,
        TOTAL_TOOLS || 1,
        domain,
        syncStartTime,
        `SYNCING_IMAGES — ${domain} at ${syncStartTime}`
      );
      progressLog(`⏳ CSV not written yet — syncing screenshots for ${domain}...`);

      const syncResult = await syncDomainImages(paths.domainDir, domain);

      if (!syncResult?.success) {
        syncSucceeded = false;
        const reason = `Remote image sync failed or was skipped: ${syncResult?.reason || 'no success flag returned'}`;
        progressLog(`⚠️ ${reason} — keeping local screenshot URLs in CSV (fallback).`);
        // Do NOT clear screenshot fields; keep the local URLs already in csvRow.
        // remoteScreenshotUrls remains as local URLs.
      } else {
        syncSucceeded = true;
        progressLog(
          `✅ Remote image sync complete → ${syncResult.remoteHost}:${syncResult.remoteDomainDir}`
        );

        if (Array.isArray(syncResult.remoteFiles) && syncResult.remoteFiles.length) {
          progressLog(`✅ Remote files: ${syncResult.remoteFiles.join(', ')}`);
        }
      }

      // If sync succeeded, we should verify the remote URLs.
      // If sync failed, we already keep local URLs – skip public verification.
      if (syncSucceeded) {
        // Re‑build remoteScreenshotUrls because the CSV row still has the remote URLs
        remoteScreenshotUrls = getCsvScreenshotChecks(csvRow, paths.imagesDir);

        const publicCheckStart = new Date().toLocaleString('en-US', {
          month: '2-digit',
          day: '2-digit',
          year: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hour12: true,
        });

        writeProgress(
          TOTAL_TOOLS || 1,
          TOTAL_TOOLS || 1,
          domain,
          publicCheckStart,
          `VERIFYING_PUBLIC_IMAGES — ${domain} at ${publicCheckStart}`
        );
        progressLog(`⏳ Verifying public screenshot URLs for ${domain}...`);

        const publicImageCheck = await waitForPublicScreenshotUrls(remoteScreenshotUrls);
        if (!publicImageCheck.ok) {
          const pending = publicImageCheck.pending
            .map((item) => `${item.url} [${item.statusCode || 'no status'} ${item.contentType || item.error || ''}]`)
            .join('; ');
          progressLog(`⚠️ Public screenshot URLs are not ready: ${pending} — keeping local screenshot URLs as fallback.`);
          // Do NOT clear screenshot fields; keep the local URLs.
          // We'll set syncSucceeded to false so we don't delete local images.
          syncSucceeded = false;
        } else {
          progressLog(`✅ Public screenshot URLs ready (${publicImageCheck.checked} checked)`);
        }
      }
    } else if (syncEnabled && syncBaseUrl && !remoteScreenshotUrls.length) {
      progressLog('⚠️ Image sync is enabled, but no remote screenshot URLs were present in the CSV row');
    } else {
      // Local-only mode. Nothing to rsync or verify publicly.
      syncSucceeded = true;
      progressLog('ℹ️ Remote image sync disabled; using local screenshot paths');
    }

    progressLog('Screenshots ready — writing CSV...');

    try {
      console.log(`[index.js] Writing domain CSV...`);
      await writeDomainCSV(paths.csvPath, csvRow);
      console.log(`[index.js] ✅ Domain CSV written`);

      console.log(`[index.js] Writing per-domain summary CSV...`);
      await writeSummaryCSV(paths.domainSummaryPath, csvRow);
      console.log(`[index.js] ✅ Per-domain summary CSV updated`);

      console.log(`[index.js] Writing batch summary CSV...`);
      await writeSummaryCSV(paths.batchSummaryPath, csvRow);
      console.log(`[index.js] ✅ Batch summary CSV updated`);
    } catch (err) {
      console.error(`[index.js] ❌ Failed to write CSV: ${err.message}`);
      console.error(err.stack);
      throw err;
    }

    progressLog(`✅ Domain CSV written: ${paths.csvPath}`);
    progressLog(`✅ Per-domain summary CSV updated: ${paths.domainSummaryPath}`);
    progressLog(`✅ Batch summary CSV updated: ${paths.batchSummaryPath}`);

    try {
      progressLog(`Updating latest results file for ${domain}...`);
      await updateLatestResults(csvRow);
      progressLog(`✅ Latest results updated for ${domain}`);
    } catch (err) {
      progressLog(`⚠️ Failed to update latest results: ${err.message}`);
    }

    const latestPathFile = path.join(OUTPUT_DIR, `latest_path_${JOB_ID}.txt`);
    fs.writeFileSync(latestPathFile, paths.domainSummaryPath);

    // Only delete local images if sync truly succeeded and we are using remote URLs.
    if (syncSucceeded && syncEnabled && syncBaseUrl && remoteScreenshotUrls.length) {
      deleteLocalImages(paths.imagesDir);
      progressLog(`✅ Local images deleted from ${paths.imagesDir}`);
    }

    const finishTime = new Date().toLocaleString('en-US', {
      month: '2-digit',
      day: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: true,
    });

    writeProgress(
      TOTAL_TOOLS || 1,
      TOTAL_TOOLS || 1,
      domain,
      finishTime,
      `DONE — ${domain} finished at ${finishTime}`
    );

    progressLog(`✅ Results ready — ${domain} finished at ${finishTime}`);
    progressLog(`Audit complete — ${domain}`);

    printSummary(results, paths, csvRow, totalTimeStr);
  } catch (error) {
    progressLog(`FATAL ERROR: ${error.message}`);
    console.error(error);
    const errorTime = new Date().toLocaleString();
    writeProgress(
      0,
      TOTAL_TOOLS || 1,
      domain,
      errorTime,
      `FAILED — ${domain} failed at ${errorTime} - ${error.message}`
    );

    // Critical for FileMaker imports:
    // if no CSV result row exists, do not let Node exit as success.
    // multi-audit.js must see a non-zero exit code so it records the domain in
    // failed_domains[], allowing api-server.js to return a synthetic N/A row
    // through /multi-result.
    const hasResultRow =
      fs.existsSync(paths.csvPath) ||
      fs.existsSync(paths.domainSummaryPath);

    process.exitCode = hasResultRow ? 0 : 1;
  } finally {
    if (release) {
      try {
        release();
        progressLog('Browser released successfully');
      } catch (e) {
        progressLog(`Error releasing browser: ${e.message}`);
      }
    }

    try {
      await closePool();
      progressLog('Browser pool closed successfully');
    } catch (e) {
      progressLog(`Error closing browser pool: ${e.message}`);
    }

    releaseDomainLock();
    progressLog(`Domain lock released for ${domain}`);

    try {
      const doneFlagFile = path.join(OUTPUT_DIR, `done_${JOB_ID}.flag`);
      fs.writeFileSync(doneFlagFile, new Date().toISOString());
      progressLog(`✅ Done flag written for job ${JOB_ID}`);
    } catch (e) {
      progressLog(`Error writing done flag: ${e.message}`);
    }
  }

}

main().catch(console.error);