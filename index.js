/**
 * index.js
 * Main entry point for domain audit tool
 * Optimized for memory consumption
 */

const fs = require('fs');
const path = require('path');
const { syncDomainImages } = require('./sync-images');
const { acquireBrowser } = require('./utils/browser-pool');
const { checkSiteReachable } = require('./utils/site-checker');

process.setMaxListeners(20);

// Enable garbage collection if available
if (global.gc) {
  setInterval(() => {
    const usage = process.memoryUsage();
    if (usage.rss > 400 * 1024 * 1024) {
      console.log(
        `[memory] High memory: RSS ${Math.round(usage.rss / 1024 / 1024)}MB, forcing GC`
      );
      global.gc();
    }
  }, 30000);
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

// Services
const { runSSLLabs } = require('./services/ssl-labs');
const { runSucuri } = require('./services/sucuri');
const { runPageSpeed } = require('./services/pagespeed');
const { runPingdom } = require('./services/pingdom');
const { runWhatsMyDNS } = require('./services/dns');
const { runPageRank } = require('./services/pagerank');
const { runServerChecks } = require('./services/server-checks');
const { runIntoDNS } = require('./services/intodns');

loadEnv();

// const domain = process.argv[2];
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

function acquireDomainLock() {
  try {
    fs.writeFileSync(DOMAIN_LOCK_FILE, String(process.pid), { flag: 'wx' });
    return true;
  } catch (e) {
    if (e.code === 'EEXIST') {
      try {
        const ownerPid = parseInt(fs.readFileSync(DOMAIN_LOCK_FILE, 'utf8'), 10);
        try {
          process.kill(ownerPid, 0);
          return false;
        } catch (_) {
          // dead — stale lock, take it over
        }
        fs.writeFileSync(DOMAIN_LOCK_FILE, String(process.pid), { flag: 'w' });
        return true;
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

const DOMAIN_LOCK_ALREADY_HELD = process.env.DOMAIN_LOCK_ALREADY_HELD === '1';

if (!DOMAIN_LOCK_ALREADY_HELD) {
  if (!acquireDomainLock()) {
    console.error(`❌ Another process is already scanning ${domain}. Exiting.`);
    process.exit(1);
  }
  console.log(`[lock] Acquired lock for ${domain} (PID: ${process.pid})`);
}

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

writeProgress(0, 1, domain, '', 'RUNNING — ' + domain);

console.log(`   🔑 Job ID : ${JOB_ID}`);
console.log(`   ⏱️  SSL delay : ${sslDelay}ms`);

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

const TOOLS = [
  { key: 'ssl', label: '🔒 SSL Lab' },
  { key: 'sucuri', label: '🛡️  Sucuri' },
  { key: 'pagespeed', label: '📈 PageSpeed' },
  { key: 'pingdom', label: '📊 Pingdom' },
  { key: 'dns', label: '🌐 WhatsMyDNS' },
  { key: 'pagerank', label: '🔍 OnePageRank' },
  { key: 'server', label: '🖥️  Server' },
  { key: 'intodns', label: '🌐 IntoDNS' },
];
const TOTAL_TOOLS = TOOLS.length;
const toolStatus = {};

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

function formatDuration(ms) {
  const totalSec = Math.round(ms / 1000);
  const mins = Math.floor(totalSec / 60);
  const secs = totalSec % 60;
  return mins === 0 ? `${secs} sec` : `${mins} min ${secs} sec`;
}

function buildCSVRow(results, paths, totalTimeStr) {
  const syncEnabled = String(process.env.IMAGE_SYNC_ENABLED || 'false').toLowerCase() === 'true';
  const syncBaseUrl = (process.env.IMAGE_SYNC_BASE_URL || '').replace(/\/$/, '');

  const screenshotPath = (filename) => {
    if (!filename) return 'none';
    
    // FIXED: No extra 'images/' folder - just the filename
    // URL: https://ta1.in-depth.com/images/domain.com/ssl.png
    if (syncEnabled && syncBaseUrl) {
      return `${syncBaseUrl}/${results.domain}/${filename}`;
    }
    return path.join(paths.imagesDir, filename);
  };

  return {
    Domain: results.domain,
    Run_At: results.timestamp,
    Total_Time: totalTimeStr,

    SSL_Status: results.ssllabs.status || 'SUCCESS',
    SSL_Error: results.ssllabs.error || null,
    SSL_ErrorCode: results.ssllabs.errorCode || null,
    SSL_Grade: results.ssllabs.data?.overallGrade || (results.ssllabs.status === 'FAILED' ? 'ERROR' : 'N/A'),
    SSL_Endpoints: results.ssllabs.data?.summary?.totalEndpoints || 0,
    SSL_AllGrades: results.ssllabs.data?.summary?.allGrades || 'N/A',
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
    Pingdom_Grade: results.pingdom.data?.performanceGrade || 'N/A',
    Pingdom_GradeLetter: results.pingdom.data?.gradeLetter || 'N/A',
    Pingdom_GradeNumber: results.pingdom.data?.gradeNumber || null,
    Pingdom_LoadTime: results.pingdom.data?.loadTime || 'N/A',
    Pingdom_PageSize: results.pingdom.data?.pageSize || 'N/A',
    Pingdom_Requests: results.pingdom.data?.requests || 'N/A',
    Pingdom_URL: results.pingdom.url || `https://tools.pingdom.com/#${results.domain}`,
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
    Server_HTTP_Status: results.server?.data?.http || 'Missing',
    Server_HTTP_Code: results.server?.data?.http_code || '',
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

    // ── Site reachability check ─────────────────────────────────────────────
    // Runs before any browser is acquired. Dead domains finish in < 15 seconds.
    progressLog(`Checking if ${domain} is reachable...`);
    writeProgress(0, 1, domain, '', `CHECKING — ${domain}`);

    const reachability = await checkSiteReachable(domain, 15000);

    if (!reachability.alive) {
      progressLog(`❌ Site unreachable: ${domain} — ${reachability.reason}`);
      writeProgress(0, 1, domain, '', `DEAD — ${domain} is not reachable`);

      const deadRow = {
        Domain: domain,
        Run_At: new Date().toISOString(),
        Total_Time: '0 sec',
        SSL_Status: 'SKIPPED', SSL_Error: reachability.error || 'Unreachable',
        SSL_ErrorCode: reachability.errorCode || 'DEAD',
        SSL_Grade: 'DEAD', SSL_Endpoints: '0', SSL_AllGrades: 'N/A',
        SSL_URL: `https://www.ssllabs.com/ssltest/analyze.html?d=${domain}`,
        SSL_Screenshot: 'none',
        Sucuri_Status: 'SKIPPED', Sucuri_Error: 'Site Unreachable', Sucuri_ErrorCode: 'DEAD',
        Sucuri_Overall: 'DEAD', Sucuri_Malware: 'Site Unreachable',
        Sucuri_Blacklist: 'Site Unreachable',
        Sucuri_URL: `https://sitecheck.sucuri.net/results/${domain}`,
        Sucuri_Screenshot: 'none',
        PageSpeed_Status: 'SKIPPED', PageSpeed_Error: 'Site Unreachable', PageSpeed_ErrorCode: 'DEAD',
        PageSpeed_Performance: 'N/A', PageSpeed_Accessibility: 'N/A',
        PageSpeed_BestPractices: 'N/A', PageSpeed_SEO: 'N/A',
        PageSpeed_LCP: 'N/A', PageSpeed_CLS: 'N/A', PageSpeed_TBT: 'N/A',
        PageSpeed_TTFB: 'N/A', PageSpeed_FCP: 'N/A',
        PageSpeed_URL: `https://pagespeed.web.dev/report?url=https://${domain}`,
        PageSpeed_Screenshot: 'none',
        Pingdom_Status: 'SKIPPED', Pingdom_Error: 'Site Unreachable', Pingdom_ErrorCode: 'DEAD',
        Pingdom_Grade: 'N/A', Pingdom_GradeLetter: 'N/A', Pingdom_GradeNumber: '',
        Pingdom_LoadTime: 'N/A', Pingdom_PageSize: 'N/A', Pingdom_Requests: 'N/A',
        Pingdom_URL: `https://tools.pingdom.com/#${domain}`, Pingdom_Screenshot: 'none',
        DNS_Status: 'SKIPPED', DNS_Error: 'Site Unreachable', DNS_ErrorCode: 'DEAD',
        DNS_Propagation: 'N/A', DNS_TotalServers: '0', DNS_Propagated: '0',
        DNS_Failed: '0', DNS_PropagationRate: 'N/A',
        DNS_URL: `https://www.whatsmydns.net/#A/${domain}`, DNS_Screenshot: 'none',
        PageRank_Status: 'SKIPPED', PageRank_Error: 'Site Unreachable', PageRank_ErrorCode: 'DEAD',
        PageRank_Integer: '', PageRank_Decimal: '', PageRank_Rank: '',
        PageRank_URL: `https://www.domcop.com/openpagerank/${domain}`,
        Server_SPF_Status: 'DEAD', Server_SPF_Value: '',
        Server_DMARC_Status: 'DEAD', Server_DMARC_Value: '',
        Server_DKIM_Status: 'DEAD', Server_DKIM_Value: '',
        Server_DomainBlacklist_Status: 'DEAD', Server_DomainBlacklist_Value: '',
        Server_MX_Status: 'DEAD', Server_MX_Value: '',
        Server_RBL_Status: 'DEAD', Server_RBL_Value: '',
        Server_IP_Address: reachability.ip || '',
        Server_BrokenLinks_Status: 'DEAD', Server_BrokenLinks_Value: '',
        Server_HTTP_Status: 'DEAD',
        Server_HTTP_Code: reachability.errorCode || reachability.error || 'Unreachable',
        Server_SSL_Status: 'DEAD', Server_SSL_Value: '',
        IntoDNS_OverallHealth: 'DEAD', IntoDNS_ErrorCount: '0', IntoDNS_WarnCount: '0',
        IntoDNS_MX_Status: 'UNKNOWN', IntoDNS_NS_Count: '0', IntoDNS_SOA_Serial: '',
        IntoDNS_URL: `https://intodns.com/${domain}`, IntoDNS_Screenshot: 'none',
      };

      try {
        await writeDomainCSV(paths.csvPath, deadRow);
        await writeSummaryCSV(paths.domainSummaryPath, deadRow);
        await writeSummaryCSV(paths.batchSummaryPath, deadRow);
        await updateLatestResults(deadRow);
        progressLog(`✅ Dead domain CSV written for ${domain}`);
      } catch (csvErr) {
        progressLog(`⚠️ Could not write dead domain CSV: ${csvErr.message}`);
      }

      const latestPathFile = path.join(OUTPUT_DIR, `latest_path_${JOB_ID}.txt`);
      fs.writeFileSync(latestPathFile, paths.domainSummaryPath);

      const doneFlagFile = path.join(OUTPUT_DIR, `done_${JOB_ID}.flag`);
      fs.writeFileSync(doneFlagFile, new Date().toISOString());

      const finishTime = new Date().toLocaleString();
      writeProgress(1, 1, domain, finishTime, `DEAD — ${domain}`);
      progressLog(`Dead domain scan complete for ${domain}`);
      return; // ← skip all browser tools
    }

    progressLog(`✅ ${domain} is reachable (${reachability.reason}) — starting full scan`);
    // ── End reachability check ──────────────────────────────────────────────

    const browserResult = await acquireBrowser();
    browser = browserResult.browser;
    release = browserResult.release;
    const newTab = () => createNewPage(browser);

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
    };

    function track(key, resultFn, labelFn) {
      const start = Date.now();
      return resultFn()
        .then((r) => {
          if (!DEBUG) {
            console.log = origLog;
            console.warn = origWarn;
          }

          markDone(key, labelFn(r), start);
          progressLog(`✅ ${key} done — ${labelFn(r)}`);

          if (!DEBUG) {
            console.log = () => {};
            console.warn = () => {};
          }

          if (global.gc && process.argv.includes('--expose-gc')) {
            setTimeout(() => global.gc(), 100);
          }

          return { status: 'fulfilled', value: r };
        })
        .catch((e) => {
          if (!DEBUG) {
            console.log = origLog;
            console.warn = origWarn;
          }

          markDone(key, '❌ failed', start);
          progressLog(`❌ ${key} failed — ${e?.message || 'unknown error'}`);

          if (!DEBUG) {
            console.log = () => {};
            console.warn = () => {};
          }

          return { status: 'rejected', reason: e };
        });
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
    ] = await Promise.all([
      track('ssl', () => runSSLLabs(domain, context), (r) => `Grade: ${r?.data?.overallGrade || 'N/A'}`),
      track('sucuri', () => runSucuri(domain, context), (r) => r?.data?.overallStatus || 'UNKNOWN'),
      track('pagespeed', () => runPageSpeed(domain, context), (r) => `Perf: ${r?.data?.scores?.performance ?? 'N/A'}`),
      track('pingdom', () => runPingdom(domain, context), (r) => `Grade: ${r?.data?.performanceGrade || 'N/A'} | Load: ${r?.data?.loadTime || 'N/A'}`),
      track('dns', () => runWhatsMyDNS(domain, context), (r) => `${r?.data?.propagated ?? '?'}/${r?.data?.totalServers ?? '?'} (${r?.data?.propagationRate || '0%'})`),
      track('pagerank', () => runPageRank(domain, context), (r) => `PR: ${r?.data?.page_rank_integer ?? 'N/A'}`),
      track('server', () => runServerChecks(domain, context), (r) => `IP: ${r?.data?.ip || 'N/A'}`),
      track('intodns', () => runIntoDNS(domain, context), (r) => `Health: ${r?.data?.overallHealth || 'N/A'}`),
    ]);

    console.log = origLog;
    console.warn = origWarn;

    const results = {
      domain,
      timestamp: new Date().toISOString(),
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
    };

    progressLog('All services done — writing CSV...');

    const totalMs = Date.now() - totalStart;
    const totalTimeStr = formatDuration(totalMs);

    const csvRow = buildCSVRow(results, paths, totalTimeStr);

    console.log(`[index.js] CSV Row built for: ${domain}`);
    console.log(`[index.js] Domain CSV path: ${paths.csvPath}`);
    console.log(`[index.js] Domain summary path: ${paths.domainSummaryPath}`);
    console.log(`[index.js] Batch summary path: ${paths.batchSummaryPath}`);

    if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

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

    // Update latest results file
    try {
      progressLog(`Updating latest results file for ${domain}...`);
      await updateLatestResults(csvRow);
      progressLog(`✅ Latest results updated for ${domain}`);
    } catch (err) {
      progressLog(`⚠️ Failed to update latest results: ${err.message}`);
    }

    const latestPathFile = path.join(OUTPUT_DIR, `latest_path_${JOB_ID}.txt`);
    fs.writeFileSync(latestPathFile, paths.domainSummaryPath);

    const doneFlagFile = path.join(OUTPUT_DIR, `done_${JOB_ID}.flag`);
    fs.writeFileSync(doneFlagFile, new Date().toISOString());

    try {
      progressLog(`Syncing screenshots to remote server for ${domain}...`);
      const syncResult = await syncDomainImages(paths.domainDir, domain);
      if (syncResult?.success) {
        progressLog(
          `Remote image sync complete → ${syncResult.remoteHost}:${syncResult.remoteDomainDir}`
        );
        deleteLocalImages(paths.imagesDir);
        progressLog(`Local images deleted from ${paths.imagesDir}`);
      } else if (syncResult?.skipped) {
        progressLog(`Remote image sync skipped → ${syncResult.reason}`);
      }
    } catch (err) {
      progressLog(`Remote image sync failed → ${err.message}`);
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

    writeProgress(1, 1, domain, finishTime, `DONE — ${domain} finished at ${finishTime}`);
    progressLog(`Audit complete — ${domain} finished at ${finishTime}`);

    printSummary(results, paths, csvRow, totalTimeStr);
  } catch (error) {
    progressLog(`FATAL ERROR: ${error.message}`);
    console.error(error);
    const errorTime = new Date().toLocaleString();
    writeProgress(
      0,
      1,
      domain,
      errorTime,
      `FAILED — ${domain} failed at ${errorTime} - ${error.message}`
    );
  } finally {
    if (release) {
      try {
        release();
        progressLog('Browser released successfully');
      } catch (e) {
        progressLog(`Error releasing browser: ${e.message}`);
      }
    }
    releaseDomainLock();
    progressLog(`Domain lock released for ${domain}`);
  }
}

main().catch(console.error);