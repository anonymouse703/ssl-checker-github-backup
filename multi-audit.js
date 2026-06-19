'use strict';

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { resolveBatchPath } = require('./audit-paths');
const { parseCSVRow, writeDomainCSV, writeSummaryCSV } = require('./utils/csv-writer');
const { updateLatestResults } = require('./utils/latest-results');
const { syncDomainImages } = require('./sync-images');

// ── Parse domain list ─────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let initialDomains = [];
let DOMAIN_LIST_FILE = null;   // kept for live-tail

console.log('\n🔍 MULTI-AUDIT:');
console.log(`   args: ${JSON.stringify(args)}`);

if (args.length === 1 && args[0].endsWith('.txt')) {
    // Accept either a basename inside TOOL_DIR or a full absolute queue file path.
    // The large FileMaker queue stores domain-list files under OUTPUT_DIR/api_batches,
    // so using path.join(__dirname, args[0]) unconditionally breaks queued scans.
    DOMAIN_LIST_FILE = path.isAbsolute(args[0]) ? args[0] : path.join(__dirname, args[0]);
    console.log(`   reading from file: ${DOMAIN_LIST_FILE}`);
    if (!fs.existsSync(DOMAIN_LIST_FILE)) {
        console.error(`❌ File not found: ${DOMAIN_LIST_FILE}`);
        process.exit(1);
    }
    initialDomains = readDomainFile(DOMAIN_LIST_FILE);
    console.log(`   parsed ${initialDomains.length} domains from file`);
} else if (process.env.DOMAIN_LIST_FILE && fs.existsSync(process.env.DOMAIN_LIST_FILE)) {
    DOMAIN_LIST_FILE = process.env.DOMAIN_LIST_FILE;
    console.log(`   reading from DOMAIN_LIST_FILE: ${DOMAIN_LIST_FILE}`);
    initialDomains = readDomainFile(DOMAIN_LIST_FILE);
    console.log(`   parsed ${initialDomains.length} domains from file`);
} else {
    initialDomains = args.filter(a => !a.startsWith('--'));
    console.log(`   parsed ${initialDomains.length} domains from args`);
}

if (initialDomains.length === 0) {
    console.log('Usage: node multi-audit.js domains.txt');
    process.exit(1);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function readDomainFile(filePath) {
    try {
        return fs.readFileSync(filePath, 'utf8')
            .split('\n')
            .map(d => d.trim())
            .filter(d => d && !d.startsWith('#'));
    } catch (e) {
        console.error(`[domain-file] read error: ${e.message}`);
        return [];
    }
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

const rawTargetByDomain = new Map();

function uniqueList(arr) {
    const out = [];
    const seen = new Set();
    for (const rawValue of arr || []) {
        const raw = String(rawValue || '').trim();
        const d = sanitizeDomain(raw);
        if (!d || seen.has(d)) continue;
        seen.add(d);
        out.push(d);
        rawTargetByDomain.set(d, raw || d);
    }
    return out;
}

function rawTargetForDomain(domain) {
    return rawTargetByDomain.get(domain) || domain;
}

// ── Checkpoint support ────────────────────────────────────────────────────────
//
// Written after every domain.  On restart, already-done domains are skipped.
// The file is deleted at the end of a clean (zero-failure) run.

const OUTPUT_DIR      = process.env.OUTPUT_DIR || '/home/ind/ind_leads_inputs';
const JOB_ID          = process.env.JOB_ID     || `batch_${Date.now()}`;
const CHECKPOINT_FILE = path.join(OUTPUT_DIR, `checkpoint_${JOB_ID}.json`);
const PROGRESS_FILE   = path.join(OUTPUT_DIR, `progress_${JOB_ID}.txt`);
const FAILED_LOG_FILE = path.join(OUTPUT_DIR, `failed_${JOB_ID}.csv`);
const GLOBAL_FAILED_LOG_FILE = path.join(OUTPUT_DIR, 'failed_scans.csv');

if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

function loadCheckpoint() {
    try {
        if (fs.existsSync(CHECKPOINT_FILE)) {
            const cp = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, 'utf8'));
            console.log(`[checkpoint] Resuming — ${cp.done.length} domain(s) already done`);
            return new Set(cp.done);
        }
    } catch (_) {}
    return new Set();
}

function saveCheckpoint(doneSet) {
    try {
        fs.writeFileSync(
            CHECKPOINT_FILE,
            JSON.stringify({ done: [...doneSet], savedAt: new Date().toISOString() }, null, 2),
            'utf8'
        );
    } catch (e) {
        console.error(`[checkpoint] save failed: ${e.message}`);
    }
}

function deleteCheckpoint() {
    try { if (fs.existsSync(CHECKPOINT_FILE)) fs.unlinkSync(CHECKPOINT_FILE); } catch (_) {}
}

const FAILED_LOG_HEADER = 'time,domain,reason,source,batch_id,job_id';

function csvEscape(value) {
    const s = String(value == null ? '' : value);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function ensureFailedLogHeader(filePath) {
    try {
        if (!fs.existsSync(filePath) || fs.statSync(filePath).size === 0) {
            fs.writeFileSync(filePath, FAILED_LOG_HEADER + '\n', 'utf8');
        }
    } catch (_) {}
}

function appendFailedLog(domain, reason, time, childJobId) {
    const row = [
        time || new Date().toISOString(),
        sanitizeDomain(domain),
        String(reason || 'Unknown failure').replace(/\s+/g, ' ').trim().slice(0, 1000),
        'multi',
        JOB_ID,
        childJobId || '',
    ].map(csvEscape).join(',') + '\n';

    // Keep both per-batch and global logs. The global log lets
    // domains-monitor.html show the download button even after completed jobs
    // disappear from /jobs.
    for (const filePath of [FAILED_LOG_FILE, GLOBAL_FAILED_LOG_FILE]) {
        try {
            ensureFailedLogHeader(filePath);
            fs.appendFileSync(filePath, row, 'utf8');
        } catch (e) {
            console.error(`[failed-log] write failed for ${domain} at ${filePath}: ${e.message}`);
        }
    }
}

function markFailureReason(current, line) {
    const cleaned = String(line || '').replace(/\x1B\[[0-?]*[ -/]*[@-~]/g, '').trim();
    if (!cleaned) return current;
    if (/(failed|failure|error|fatal|exception|timeout|timed out|cannot|unreachable|not reachable|denied|refused|crash|exit code|ERR_|❌|⚠️)/i.test(cleaned)) {
        return cleaned.slice(0, 1000);
    }
    return current;
}

function terminateChildTree(child, signal, label, domain) {
    if (!child || !child.pid) return;

    // Children are spawned as their own process group (detached:true).
    // Killing the negative PID terminates node + any Chrome/Chromium children
    // that would otherwise remain stuck after a hard timeout.
    try {
        process.kill(-child.pid, signal);
        console.error(`${label} ${signal} sent to process group for ${domain} (pgid ${child.pid})`);
        return;
    } catch (groupErr) {
        // Fallback for platforms/processes where process-group kill is unavailable.
        try {
            child.kill(signal);
            console.error(`${label} ${signal} sent to child process for ${domain} (${child.pid})`);
        } catch (_) {}
    }
}

// ── Configuration ─────────────────────────────────────────────────────────────
const MEMORY_LIMIT_MB        = parseInt(process.env.MEMORY_LIMIT_MB        || '1200', 10);
const MAX_CONCURRENT_DEFAULT = parseInt(process.env.MAX_CONCURRENT         || '2',    10);  // 3→2: each Chrome instance uses ~200-400MB; 3 concurrent = 600-1200MB RAM peak
const DOMAIN_LAUNCH_DELAY_MS = parseInt(process.env.DOMAIN_LAUNCH_DELAY_MS || '5000', 10);  // 3s→5s: give previous browser time to init before spawning next
const GROUP_DELAY_MS_ENV     = parseInt(process.env.GROUP_DELAY_MS         || '20000',10);  // 30s→20s: shorter gap since we have better memory management now
const MINUTES_GAP            = parseFloat(process.env.MINUTES_GAP || '0') || 0;
const FORCE_RESCAN           = String(process.env.FORCE_RESCAN || '0') === '1';
const MULTI_AUDIT_POST_SYNC_ENABLED = String(process.env.MULTI_AUDIT_POST_SYNC_ENABLED || 'false').toLowerCase() === 'true';
const DOMAIN_HARD_TIMEOUT_MS = parseInt(process.env.DOMAIN_HARD_TIMEOUT_MS || '5400000', 10); // 90 minutes by default; full-tool scans can legitimately run longer than 15 minutes

// MINUTES_GAP forces one-at-a-time processing; otherwise use configured value.
// No artificial cap based on domain count — let the gap/concurrency setting
// be the sole throttle.
const CONFIG = {
    MAX_CONCURRENT:       MINUTES_GAP > 0 ? 1 : Math.max(1, MAX_CONCURRENT_DEFAULT),
    SSL_STAGGER_SEC:      60,
    GROUP_DELAY_MS:       MINUTES_GAP > 0
                            ? Math.round(MINUTES_GAP * 60 * 1000)
                            : GROUP_DELAY_MS_ENV,
    DOMAIN_LAUNCH_DELAY_MS,
};

console.log(`\n💾 Config: concurrent=${CONFIG.MAX_CONCURRENT}  gap=${CONFIG.GROUP_DELAY_MS / 1000}s  minutes_gap=${MINUTES_GAP}`);

// ── Batch folder ──────────────────────────────────────────────────────────────
const SCAN_BATCH_PATH = resolveBatchPath();
console.log(`📁 Batch folder: ${SCAN_BATCH_PATH}`);
if (!fs.existsSync(SCAN_BATCH_PATH)) fs.mkdirSync(SCAN_BATCH_PATH, { recursive: true });
const batchFolder = path.basename(SCAN_BATCH_PATH);

const summaryPath = path.join(SCAN_BATCH_PATH, 'summary.csv');
const statsPath   = path.join(SCAN_BATCH_PATH, '_batch_stats.json');
const { SUMMARY_FIELDS } = require('./config/constants');
if (!fs.existsSync(summaryPath)) {
    fs.writeFileSync(summaryPath, SUMMARY_FIELDS.join(',') + '\n', 'utf8');
    console.log(`✓ Created summary.csv`);
}

// ── Progress file ─────────────────────────────────────────────────────────────
function writeProgress(completed, total, lastDomain, finishTime, status, completedList, failedList, currentDomain) {
    try {
        const lines = [
            `completed=${completed}`,
            `total=${total}`,
            `last_domain=${lastDomain}`,
            `current_domain=${currentDomain || ''}`,
            `last_finish=${finishTime}`,
            `status=${status}`,
            `job_id=${JOB_ID}`,
            `completed_domains=${(completedList || []).join(',')}`,
            `failed_domains=${(failedList || []).join(',')}`,
        ];
        fs.writeFileSync(PROGRESS_FILE, lines.join('\n'), 'utf8');
        const pct = total > 0 ? Math.round((completed / total) * 100) : 0;
        console.log(`[progress] 📊 ${completed}/${total} (${pct}%) — ${status}`);
    } catch (e) {
        console.error(`[progress] write error: ${e.message}`);
    }
}

// ── Latest-results updater ────────────────────────────────────────────────────
async function updateLatestForDomain(domain) {
    const csvPath = path.join(SCAN_BATCH_PATH, domain, `${domain}_results.csv`);
    if (!fs.existsSync(csvPath)) return false;
    try {
        const content = fs.readFileSync(csvPath, 'utf8');
        const lines = content.split('\n').filter(l => l.trim());
        if (lines.length < 2) return false;
        const headers = parseCSVRow(lines[0]);
        const cols    = parseCSVRow(lines[lines.length - 1]);
        const rowData = {};
        headers.forEach((h, i) => { rowData[h] = cols[i] || ''; });
        await updateLatestResults(rowData);
        console.log(`[multi-audit] ✅ latest updated for ${domain}`);
        return true;
    } catch (err) {
        console.error(`[multi-audit] latest update failed for ${domain}: ${err.message}`);
        return false;
    }
}


function csvFileHasDataRow(filePath) {
    try {
        if (!filePath || !fs.existsSync(filePath)) return false;
        // FIX: was split('\\n') — literal backslash-n — which never splits real CSV
        // files and caused every domain to appear to have no result row, making
        // ensureScannerFallbackResultRow() overwrite real results with N/A fallbacks.
        const lines = fs.readFileSync(filePath, 'utf8').split('\n').filter(l => l.trim());
        if (lines.length < 2) return false;
        const headers = parseCSVRow(lines[0]);
        const cols = parseCSVRow(lines[lines.length - 1]);
        const row = {};
        headers.forEach((h, i) => { row[h] = cols[i] || ''; });
        return sanitizeDomain(row.Domain || row.domain || row.DOMAIN || '') !== '';
    } catch (_) {
        return false;
    }
}

function domainResultPaths(domain) {
    const d = sanitizeDomain(domain);
    const domainDir = path.join(SCAN_BATCH_PATH, d);
    return {
        domainDir,
        domainCsvPath: path.join(domainDir, `${d}_results.csv`),
        domainSummaryPath: path.join(domainDir, 'summary.csv'),
    };
}

function domainHasImportableResultRow(domain) {
    const p = domainResultPaths(domain);
    return csvFileHasDataRow(p.domainCsvPath) || csvFileHasDataRow(p.domainSummaryPath);
}

function writeLatestPathForDomain(domain, childJobId) {
    try {
        if (!childJobId) return;
        const p = domainResultPaths(domain);
        const preferred = csvFileHasDataRow(p.domainSummaryPath) ? p.domainSummaryPath : p.domainCsvPath;
        if (!fs.existsSync(preferred)) return;
        fs.writeFileSync(path.join(OUTPUT_DIR, `latest_path_${childJobId}.txt`), preferred, 'utf8');
        fs.writeFileSync(path.join(OUTPUT_DIR, `latest_path_${JOB_ID}.txt`), preferred, 'utf8');
    } catch (e) {
        console.error(`[multi-audit] latest path write failed for ${domain}: ${e.message}`);
    }
}


function selectedToolSet() {
    try {
        const arr = JSON.parse(process.env.ENABLED_TOOLS || '[]');
        return new Set((Array.isArray(arr) ? arr : []).map(v => String(v || '').toLowerCase()));
    } catch (_) {
        return new Set();
    }
}

function toolWasSelected(key) {
    const set = selectedToolSet();
    return set.has('all') || set.has('all tools') || set.has(String(key || '').toLowerCase());
}

function fallbackStatusForTool(key) {
    return toolWasSelected(key) ? 'FAILED' : 'SKIPPED';
}

function maybeImage(domain, filename) {
    // Scanner fallback rows must not reuse partial/old screenshots.
    // If a tool failed hard, FileMaker should show blank/N/A image fields, not a misleading screenshot.
    return 'none';
}

function makeScannerFallbackRow(domain, reason, elapsedSec) {
    const now = new Date().toISOString();
    const cleanReason = String(reason || 'Tool timeout/failure; scanner fallback row created').replace(/\s+/g, ' ').trim().slice(0, 1000);
    return {
        Domain: domain,
        Run_At: now,
        Total_Time: `${Math.floor((elapsedSec || 0) / 60)} min ${(elapsedSec || 0) % 60} sec`,
        Browser_Alive: 0,
        Browser_Status: 'FAILED',
        Browser_Error: cleanReason,
        Browser_Title: '',
        Final_URL: `https://${domain}`,
        Server_HTTP_Title: '',
        Server_HTTP_BodySnippet: cleanReason,
        Page_Language: '',
        Page_LanguageConfidence: '',
        Page_LanguageReason: cleanReason,
        Page_IsEnglish: '',
        Page_IsNonEnglish: '',
        Domain_IsViable: 0,
        SSL_Status: fallbackStatusForTool('ssl'),
        SSL_Error: toolWasSelected('ssl') ? cleanReason : null,
        SSL_ErrorCode: toolWasSelected('ssl') ? 'SCANNER_FALLBACK' : null,
        SSL_Grade: 'N/A',
        SSL_Endpoints: 0,
        SSL_AllGrades: 'N/A',
        SSL_URL: `https://www.ssllabs.com/ssltest/analyze.html?d=${domain}`,
        SSL_Screenshot: maybeImage(domain, 'ssl.png'),
        Sucuri_Status: fallbackStatusForTool('sucuri'),
        Sucuri_Error: toolWasSelected('sucuri') ? cleanReason : null,
        Sucuri_ErrorCode: toolWasSelected('sucuri') ? 'SCANNER_FALLBACK' : null,
        Sucuri_Overall: 'UNKNOWN',
        Sucuri_Malware: 'Unknown',
        Sucuri_Blacklist: 'N/A',
        Sucuri_URL: `https://sitecheck.sucuri.net/results/${domain}`,
        Sucuri_Screenshot: maybeImage(domain, 'sucuri.png'),
        PageSpeed_Status: fallbackStatusForTool('pagespeed'),
        PageSpeed_Error: toolWasSelected('pagespeed') ? cleanReason : null,
        PageSpeed_ErrorCode: toolWasSelected('pagespeed') ? 'SCANNER_FALLBACK' : null,
        PageSpeed_Performance: 'N/A',
        PageSpeed_Accessibility: 'N/A',
        PageSpeed_BestPractices: 'N/A',
        PageSpeed_SEO: 'N/A',
        PageSpeed_LCP: 'N/A',
        PageSpeed_CLS: 'N/A',
        PageSpeed_TBT: 'N/A',
        PageSpeed_TTFB: 'N/A',
        PageSpeed_FCP: 'N/A',
        PageSpeed_URL: `https://pagespeed.web.dev/report?url=https://${domain}`,
        PageSpeed_Screenshot: maybeImage(domain, 'pagespeed.png'),
        Pingdom_Status: fallbackStatusForTool('pingdom'),
        Pingdom_Error: toolWasSelected('pingdom') ? cleanReason : null,
        Pingdom_ErrorCode: toolWasSelected('pingdom') ? 'SCANNER_FALLBACK' : null,
        Pingdom_Grade: 'N/A',
        Pingdom_GradeLetter: 'N/A',
        Pingdom_GradeNumber: null,
        Pingdom_LoadTime: 'N/A',
        Pingdom_PageSize: 'N/A',
        Pingdom_Requests: 'N/A',
        Pingdom_URL: `https://tools.pingdom.com/#${domain}`,
        Pingdom_Screenshot: maybeImage(domain, 'pingdom.png'),
        DNS_Status: fallbackStatusForTool('dns'),
        DNS_Error: toolWasSelected('dns') ? cleanReason : null,
        DNS_ErrorCode: toolWasSelected('dns') ? 'SCANNER_FALLBACK' : null,
        DNS_Propagation: 'N/A / N/A',
        DNS_TotalServers: 'N/A',
        DNS_Propagated: 'N/A',
        DNS_Failed: 'N/A',
        DNS_PropagationRate: 'N/A%',
        DNS_URL: `https://www.whatsmydns.net/#A/${domain}`,
        DNS_Screenshot: maybeImage(domain, 'dns.png'),
        PageRank_Status: fallbackStatusForTool('pagerank'),
        PageRank_Error: toolWasSelected('pagerank') ? cleanReason : null,
        PageRank_ErrorCode: toolWasSelected('pagerank') ? 'SCANNER_FALLBACK' : null,
        PageRank_Integer: '',
        PageRank_Decimal: '',
        PageRank_Rank: '',
        PageRank_URL: `https://www.domcop.com/openpagerank/${domain}`,
        Server_SPF_Status: 'Missing',
        Server_SPF_Value: '',
        Server_DMARC_Status: 'Missing',
        Server_DMARC_Value: '',
        Server_DKIM_Status: 'Missing',
        Server_DKIM_Value: '',
        Server_DomainBlacklist_Status: 'Missing',
        Server_DomainBlacklist_Value: '',
        Server_MX_Status: 'Missing',
        Server_MX_Value: '',
        Server_RBL_Status: 'Missing',
        Server_RBL_Value: '',
        Server_IP_Address: '',
        Server_BrokenLinks_Status: 'Missing',
        Server_BrokenLinks_Value: '',
        Server_HTTP_Status: toolWasSelected('server') ? `UNREACHABLE: ${cleanReason}` : 'SKIPPED',
        Server_HTTP_Code: toolWasSelected('server') ? (cleanReason.match(/\b(400|401|403|404|410|444|500|501|502|503|504|520|521|522|523|524|525|526|530)\b/)?.[1] || (cleanReason.toLowerCase().includes('nxdomain') || cleanReason.toLowerCase().includes('enotfound') ? 'ERR_NAME_NOT_RESOLVED' : 'ERR_UNREACHABLE')) : '',
        Server_SSL_Status: 'Missing',
        Server_SSL_Value: '',
        IntoDNS_OverallHealth: toolWasSelected('intodns') ? 'UNKNOWN' : 'SKIPPED',
        IntoDNS_ErrorCount: 'N/A',
        IntoDNS_WarnCount: 'N/A',
        IntoDNS_MX_Status: 'N/A',
        IntoDNS_NS_Count: 'N/A',
        IntoDNS_SOA_Serial: 'N/A',
        IntoDNS_URL: `https://intodns.com/${domain}`,
        IntoDNS_Screenshot: maybeImage(domain, 'intodns.png'),
    };
}

async function ensureScannerFallbackResultRow(domain, reason, childJobId, elapsedSec) {
    const domainDir = path.join(SCAN_BATCH_PATH, domain);
    if (!fs.existsSync(domainDir)) fs.mkdirSync(domainDir, { recursive: true });
    const domainCsvPath = path.join(domainDir, `${domain}_results.csv`);
    const domainSummaryPath = path.join(domainDir, 'summary.csv');
    const hasExistingRow = domainHasImportableResultRow(domain);
    if (hasExistingRow) {
        writeLatestPathForDomain(domain, childJobId);
        return true;
    }
    const row = makeScannerFallbackRow(domain, `${reason || 'scanner fallback'} (job ${childJobId || ''})`, elapsedSec || 0);
    await writeDomainCSV(domainCsvPath, row);
    await writeSummaryCSV(domainSummaryPath, row);
    await writeSummaryCSV(summaryPath, row);
    try { await updateLatestResults(row); } catch (_) {}
    writeLatestPathForDomain(domain, childJobId);
    console.log(`[multi-audit] ✅ fallback result row created for ${domain}`);
    return true;
}

// ── State shared between runAudit() calls ─────────────────────────────────────
const doneBefore = loadCheckpoint();
const allInitial = uniqueList(initialDomains);
const remaining  = allInitial.filter(d => !doneBefore.has(d));

const results = {
    doneSet:   new Set([...doneBefore]),
    completed: [],           // completed IN THIS RUN
    failed:    [],           // failed    IN THIS RUN
    failedDetails: {},
    total:     allInitial.length,
    errors:    { sslLabs: 0, sslLabsDetails: {} },
    timing:    { groups: [] },
};

function allDoneForProgress() {
    return [...results.doneSet];
}

if (doneBefore.size > 0) {
    console.log(`\n⏩ Checkpoint: skipping ${doneBefore.size} already-done domain(s)`);
    console.log(`   Remaining: ${remaining.length}\n`);
}

// ── Single-domain runner ──────────────────────────────────────────────────────
function runAudit(domain, groupNum, posInGroup) {
    return new Promise((resolve) => {
        const start      = Date.now();
        const label      = `[G${groupNum}-D${posInGroup + 1}]`.padEnd(12);
        const sslDelayMs = posInGroup * CONFIG.SSL_STAGGER_SEC * 1000;
        const childJobId = `${JOB_ID}_${domain.replace(/[^a-z0-9._-]/gi, '_')}`;
        let failureReason = '';

        const rawTarget = rawTargetForDomain(domain);
        console.log(`${label} 🚀 ${domain}${rawTarget !== domain ? ` (${rawTarget})` : ''}`);
        writeProgress(
            results.doneSet.size,
            results.total,
            '',
            '',
            `CHECKING — ${domain}`,
            allDoneForProgress(),
            results.failed,
            domain
        );

        const child = spawn('node', ['index.js', rawTarget], {
            cwd: __dirname,
            detached: true,
            env: {
                ...process.env,
                SCAN_BATCH_PATH,
                SSL_QUEUE_DELAY_MS:    String(sslDelayMs),
                BATCH_MODE:            'true',
                JOB_ID:                childJobId,
                ENABLED_TOOLS:         process.env.ENABLED_TOOLS || '[]',
                NODE_OPTIONS:          '--max-old-space-size=384',  // 256→384: allow more heap before OOM-crash; Chrome itself is outside this limit
                FORCE_RESCAN:          FORCE_RESCAN ? '1' : '0',
                TARGET_RAW_URL:        rawTarget,
            },
        });

        console.log(`__DOMAIN_START__:${domain}:${child.pid}:${childJobId}`);

        let timedOut = false;
        const hardTimer = setTimeout(() => {
            timedOut = true;
            failureReason = `Domain hard timeout after ${Math.round(DOMAIN_HARD_TIMEOUT_MS / 1000)}s`;
            console.error(`${label} ⏰ ${failureReason}; creating fallback row after child exits.`);
            terminateChildTree(child, 'SIGTERM', label, domain);
            // Escalate to SIGKILL faster (3s not 5s) so memory is freed sooner.
            // This also clears orphan Chrome/Chromium processes.
            setTimeout(() => {
                console.error(`${label} ☠️  SIGKILL escalation for ${domain}`);
                terminateChildTree(child, 'SIGKILL', label, domain);
            }, 3000);
        }, DOMAIN_HARD_TIMEOUT_MS);

        child.on('error', (err) => {
            failureReason = markFailureReason(failureReason, `spawn error: ${err.message}`);
            console.error(`${label} ❌ spawn error for ${domain}: ${err.message}`);
        });

        child.stdout.on('data', (data) => {
            String(data).split('\n').forEach(line => {
                const t = line.trim();
                if (!t) return;
                if (t.includes('SSL ⏳ Capacity')) {
                    results.errors.sslLabs++;
                    results.errors.sslLabsDetails[domain] = (results.errors.sslLabsDetails[domain] || 0) + 1;
                }
                failureReason = markFailureReason(failureReason, t);
                console.log(`${label} ${t}`);
            });
        });
        child.stderr.on('data', (data) => {
            String(data).split('\n').forEach(line => {
                const t = line.trim();
                if (t) {
                    failureReason = markFailureReason(failureReason, t);
                    console.error(`${label} ⚠️  ${t}`);
                }
            });
        });

        child.on('close', async (code) => {
            clearTimeout(hardTimer);
            if (timedOut) {
                terminateChildTree(child, 'SIGKILL', label, domain);
            }

            // FIX: Give index.js a brief moment to flush its CSV writes to disk before
            // we check whether a result row exists. On exit code 0 this is almost
            // always a no-op, but prevents a rare race on slow storage where the file
            // handle is still being closed when we stat() it.
            if (code === 0 && !timedOut) {
                await new Promise(r => setTimeout(r, 500));
            }

            const elapsed = Math.round((Date.now() - start) / 1000);
            const finishTime = new Date().toLocaleString('en-US', {
                month: '2-digit', day: '2-digit', year: 'numeric',
                hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true,
            });

            let hasResultRow = domainHasImportableResultRow(domain);

            // A completed domain must always have one importable CSV row.
            // If index.js exits cleanly but no row exists, create a fallback row instead
            // of marking the domain completed with nothing for /result or /multi-result.
            const scanActuallyFailed = code !== 0 || timedOut;
            let fallbackRowCreated = false;

            if (!hasResultRow) {
                const reason = timedOut
                    ? (failureReason || `Domain hard timeout after ${Math.round(DOMAIN_HARD_TIMEOUT_MS / 1000)}s`)
                    : scanActuallyFailed
                        ? (failureReason || `index.js exited with code ${code}`)
                        : (failureReason || `index.js exited with code 0 but did not write a result row`);

                console.warn(
                    `${label} ⚠️  No result row for ${domain}. ` +
                    `Creating fallback row so FileMaker can import a terminal result. ` +
                    `(code=${code} timedOut=${timedOut})`
                );

                try {
                    fallbackRowCreated = await ensureScannerFallbackResultRow(domain, reason, childJobId, elapsed);
                } catch (fallbackErr) {
                    console.error(`${label} ❌ fallback row creation failed for ${domain}: ${fallbackErr.message}`);
                    fallbackRowCreated = false;
                }

                hasResultRow = domainHasImportableResultRow(domain);
            }

            if (hasResultRow) {
                writeLatestPathForDomain(domain, childJobId);
                console.log(`${label} ✅ importable result row confirmed for ${domain}${fallbackRowCreated ? ' (fallback)' : ''}.`);
            } else {
                const reason = failureReason || `No result row could be created for ${domain}`;

                appendFailedLog(domain, reason, new Date().toISOString(), childJobId);

                if (!results.failed.includes(domain)) {
                    results.failed.push(domain);
                }

                results.failedDetails[domain] = {
                    reason,
                    time: new Date().toISOString(),
                    jobId: childJobId,
                    exitCode: code,
                    fallbackRowCreated: false,
                };

                writeProgress(
                    results.doneSet.size,
                    results.total,
                    domain,
                    finishTime,
                    `FAILED — no result row ${domain}`,
                    allDoneForProgress(),
                    results.failed
                );

                resolve({ domain, code, elapsed, success: false });
                return;
            }

            if (scanActuallyFailed) {
                const reason = failureReason || `index.js exited with code ${code}`;
                appendFailedLog(domain, reason, new Date().toISOString(), childJobId);

                if (!results.failed.includes(domain)) {
                    results.failed.push(domain);
                }

                results.failedDetails[domain] = {
                    reason,
                    time: new Date().toISOString(),
                    jobId: childJobId,
                    exitCode: code,
                    fallbackRowCreated,
                };
            }

            // FileMaker requirement: every finished domain with a CSV row is uploadable.
            // Even if one tool failed or a fallback row was created, it is completed for upload.
            results.completed.push(domain);
            console.log(`${label} ✅ uploadable result row ready in ${Math.floor(elapsed / 60)}m ${elapsed % 60}s`);
            await updateLatestForDomain(domain);

            // Optional post-child image sync. index.js already performs image sync before
            // writing the final CSV, so doing it again here can duplicate SSH/rsync work.
            // Enable only if you intentionally want multi-audit.js to perform a second
            // cleanup/sync pass after each child scan exits.
            if (MULTI_AUDIT_POST_SYNC_ENABLED) {
                try {
                    const domainDir = path.join(SCAN_BATCH_PATH, domain);
                    const syncResult = await syncDomainImages(domainDir, domain);
                    if (syncResult.skipped) {
                        console.log(`${label} [sync-images] Skipped: ${syncResult.reason}`);
                    } else if (syncResult.success) {
                        console.log(`${label} [sync-images] ✅ Synced and deleted ${syncResult.deletedLocal?.length || 0} image(s) for ${domain}`);
                    } else {
                        console.warn(`${label} [sync-images] ⚠️  Sync failed for ${domain}: ${syncResult.error}`);
                    }
                } catch (syncErr) {
                    console.error(`${label} [sync-images] ❌ Unexpected error for ${domain}: ${syncErr.message}`);
                }
            }

            // Checkpoint after every domain
            results.doneSet.add(domain);
            saveCheckpoint(results.doneSet);

            writeProgress(
                results.doneSet.size,
                results.total,
                domain,
                finishTime,
                `RUNNING — result row ready ${domain}`,
                allDoneForProgress(),
                results.failed
            );

            resolve({ domain, code, elapsed, success: code === 0 });
        });
    });
}

// ── Memory monitor ────────────────────────────────────────────────────────────
// IMPORTANT: process.memoryUsage().rss only reflects THIS orchestrator process.
// The real memory consumers are the spawned `node index.js <domain>` children
// and their Chrome instances — neither of which this process's own RSS sees.
// That meant the old check almost never tripped no matter how much RAM the
// children actually used, so the queue kept launching new groups even while
// the VM as a whole was running out of memory — eventually causing the whole
// server (not just the scan) to lock up, including SSH/login.
//
// We now check actual SYSTEM-wide free memory via /proc/meminfo's
// MemAvailable, which reflects everything running on the box: this
// orchestrator, all of its child scan processes, and every Chrome instance
// they've launched.
//
// We ALSO check CPU load average. This matters even when MAX_CONCURRENT is
// forced down to 1 by a minutes_gap setting: a SINGLE Chrome instance can
// spawn multiple subprocesses (renderer, GPU, network service, and — unless
// Site Isolation is disabled — one renderer PER origin/iframe on the page),
// which alone can push a 1-core VM's load average past 2.0. That kind of CPU
// starvation is what makes SSH/login unresponsive even though memory looks
// fine — pure memory monitoring would never catch it.
let memoryWarningCount = 0;
let loadWarningCount   = 0;
let systemPressure     = false;  // true = pause launching new domains (memory OR CPU)

const SYSTEM_MEM_CRITICAL_PCT = parseInt(process.env.SYSTEM_MEM_CRITICAL_PCT || '85', 10); // pause queue
const SYSTEM_MEM_HIGH_PCT     = parseInt(process.env.SYSTEM_MEM_HIGH_PCT     || '70', 10); // warn only

// Load average per CPU core. A value of 1.0 per core means the CPU is fully
// saturated; values above that mean processes are queued waiting for CPU.
const LOAD_PER_CPU_CRITICAL = parseFloat(process.env.LOAD_PER_CPU_CRITICAL || '1.5'); // pause queue
const LOAD_PER_CPU_HIGH     = parseFloat(process.env.LOAD_PER_CPU_HIGH     || '1.0'); // warn only

function getMemoryMB() {
    // Orchestrator's own RSS — kept for logging/visibility only, no longer
    // used as the throttle decision.
    return Math.round(process.memoryUsage().rss / 1024 / 1024);
}

function getSystemMemoryStats() {
    const totalMB = Math.round(os.totalmem() / 1024 / 1024);

    // Prefer /proc/meminfo's MemAvailable on Linux — this is what `free -m`
    // reports as "available" and correctly treats reclaimable page cache as
    // free. os.freemem() does NOT do this and can report false memory
    // pressure on a healthy box that simply has a lot of file cache.
    try {
        const meminfo = fs.readFileSync('/proc/meminfo', 'utf8');
        const match = meminfo.match(/MemAvailable:\s+(\d+)\s*kB/);
        if (match) {
            const freeMB  = Math.round(parseInt(match[1], 10) / 1024);
            const usedMB  = totalMB - freeMB;
            const usedPct = totalMB > 0 ? Math.round((usedMB / totalMB) * 100) : 0;
            return { totalMB, freeMB, usedMB, usedPct, source: 'MemAvailable' };
        }
    } catch (_) {
        // /proc/meminfo not present (non-Linux) — fall through to os.freemem()
    }

    const freeMB  = Math.round(os.freemem() / 1024 / 1024);
    const usedMB  = totalMB - freeMB;
    const usedPct = totalMB > 0 ? Math.round((usedMB / totalMB) * 100) : 0;
    return { totalMB, freeMB, usedMB, usedPct, source: 'os.freemem' };
}

function getSystemLoadStats() {
    const cpuCount = Math.max(1, os.cpus().length);
    const [load1, load5, load15] = os.loadavg();
    const loadPerCpu = load1 / cpuCount;
    return {
        cpuCount,
        load1: Math.round(load1 * 100) / 100,
        load5: Math.round(load5 * 100) / 100,
        load15: Math.round(load15 * 100) / 100,
        loadPerCpu: Math.round(loadPerCpu * 100) / 100,
    };
}

function checkMemory() {
    const orchestratorMB = getMemoryMB();
    const sys  = getSystemMemoryStats();
    const load = getSystemLoadStats();

    const memCritical  = sys.usedPct >= SYSTEM_MEM_CRITICAL_PCT;
    const memHigh      = sys.usedPct >= SYSTEM_MEM_HIGH_PCT;
    const loadCritical = load.loadPerCpu >= LOAD_PER_CPU_CRITICAL;
    const loadHigh     = load.loadPerCpu >= LOAD_PER_CPU_HIGH;

    if (memCritical || loadCritical) {
        const reasons = [];
        if (memCritical)  reasons.push(`memory ${sys.usedPct}% used (${sys.usedMB}MB/${sys.totalMB}MB, ${sys.freeMB}MB free)`);
        if (loadCritical) reasons.push(`load ${load.loadPerCpu}/core (load1=${load.load1} on ${load.cpuCount} CPU${load.cpuCount > 1 ? 's' : ''})`);

        console.log(
            `[pressure] 🔴 CRITICAL: ${reasons.join(' AND ')} — pausing queue. ` +
            `[orchestrator own RSS: ${orchestratorMB}MB]`
        );
        if (memCritical)  memoryWarningCount++;
        if (loadCritical) loadWarningCount++;
        systemPressure = true;
        if (global.gc) global.gc();
    } else if (memHigh || loadHigh) {
        const reasons = [];
        if (memHigh)  reasons.push(`memory ${sys.usedPct}% used (${sys.usedMB}MB/${sys.totalMB}MB, ${sys.freeMB}MB free)`);
        if (loadHigh) reasons.push(`load ${load.loadPerCpu}/core (load1=${load.load1} on ${load.cpuCount} CPU${load.cpuCount > 1 ? 's' : ''})`);

        console.log(`[pressure] 🟡 High: ${reasons.join(' AND ')} [orchestrator own RSS: ${orchestratorMB}MB]`);
        systemPressure = false;
        if (global.gc) global.gc();
    } else {
        if (systemPressure) {
            console.log(`[pressure] 🟢 System recovered: mem ${sys.usedPct}% / load ${load.loadPerCpu} per core — resuming queue`);
        }
        systemPressure = false;
    }
}

// Wait until memory OR CPU pressure drops before launching next group
async function waitForMemoryPressureRelief(maxWaitMs = 120000) {
    if (!systemPressure) return;
    const deadline = Date.now() + maxWaitMs;
    console.log('[pressure] ⏸️  Waiting for system pressure to drop before next group...');
    while (systemPressure && Date.now() < deadline) {
        if (global.gc) global.gc();
        await new Promise(r => setTimeout(r, 5000));
        checkMemory();
    }
    if (systemPressure) {
        console.log('[pressure] ⚠️  Pressure still high after wait — proceeding anyway to avoid deadlock');
        systemPressure = false;
    }
}

const memoryMonitor = setInterval(checkMemory, 15000);  // check every 15s not 30s

// ── Main loop ─────────────────────────────────────────────────────────────────
const overallStart = Date.now();

async function runAll() {
    // Start with domains not yet in the checkpoint
    let queue    = [...remaining];
    let groupNum = 0;

    writeProgress(doneBefore.size, results.total, '', '', 'RUNNING', allDoneForProgress(), results.failed);

    while (queue.length > 0) {
        // Live-tail: re-read the domain file so domains added mid-run by a
        // second FileMaker chunk call are picked up automatically.
        if (DOMAIN_LIST_FILE) {
            const fresh   = uniqueList(readDomainFile(DOMAIN_LIST_FILE));
            const newOnes = fresh.filter(d => !results.doneSet.has(d) && !queue.includes(d));
            if (newOnes.length > 0) {
                console.log(`[tail] +${newOnes.length} new domain(s) found in file`);
                queue.push(...newOnes);
                results.total += newOnes.length;
            }
        }

        // Before launching next group, check memory and wait if under pressure.
        // This is the primary guard against the server getting stuck due to OOM.
        checkMemory();
        await waitForMemoryPressureRelief();

        const group      = queue.splice(0, CONFIG.MAX_CONCURRENT);
        const groupStart = Date.now();
        groupNum++;

        const memNow = getMemoryMB();
        console.log('═'.repeat(70));
        console.log(`📦 GROUP ${groupNum} — ${group.length} domain(s)  (${queue.length} remaining)  [mem: ${memNow}MB]`);
        console.log('═'.repeat(70));

        const promises = group.map((domain, i) =>
            new Promise(resolve =>
                setTimeout(
                    () => resolve(runAudit(domain, groupNum, i)),
                    i * CONFIG.DOMAIN_LAUNCH_DELAY_MS
                )
            )
        );
        await Promise.all(promises);

        const groupTime = Math.round((Date.now() - groupStart) / 1000);
        results.timing.groups.push({ group: groupNum, domains: group.length, timeSec: groupTime });

        console.log('─'.repeat(70));
        console.log(`✅ GROUP ${groupNum} done in ${Math.floor(groupTime / 60)}m ${groupTime % 60}s`);
        console.log(`   Total done: ${results.doneSet.size}/${results.total}  |  failed: ${results.failed.length}`);

        // Wait the configured gap before the next group
        if (queue.length > 0) {
            const elapsed   = Date.now() - groupStart;
            const waitMs    = Math.max(0, CONFIG.GROUP_DELAY_MS - elapsed);
            if (waitMs > 0) {
                console.log(`\n⏸️  Waiting ${Math.ceil(waitMs / 1000)}s before next domain… [mem: ${getMemoryMB()}MB]`);
                checkMemory();
                // Split wait into 5s chunks so memory is checked periodically
                // and GC can run rather than blocking the event loop for 20-30s.
                const chunks = Math.ceil(waitMs / 5000);
                for (let c = 0; c < chunks; c++) {
                    await new Promise(r => setTimeout(r, Math.min(5000, waitMs - c * 5000)));
                    if (global.gc) global.gc();
                }
            }
        }
    }

    // ── Final report ──────────────────────────────────────────────────────────
    const totalTime    = Math.round((Date.now() - overallStart) / 1000);
    const ranCount     = results.completed.length + results.failed.length;
    const avgPerDomain = ranCount > 0 ? Math.round(totalTime / ranCount) : 0;

    console.log('\n╔══════════════════════════════════════════════════════════════════════════╗');
    console.log('║                            FINAL RESULTS                                  ║');
    console.log('╚══════════════════════════════════════════════════════════════════════════╝');
    console.log(`⏱️  Total time         : ${Math.floor(totalTime / 60)}m ${totalTime % 60}s`);
    console.log(`📊 Avg per domain      : ${Math.floor(avgPerDomain / 60)}m ${avgPerDomain % 60}s`);
    console.log(`✅ Completed (run)     : ${results.completed.length}`);
    console.log(`❌ Failed    (run)     : ${results.failed.length}`);
    console.log(`⏩ Skipped (checkpoint): ${doneBefore.size}`);

    if (results.failed.length > 0) {
        console.log('\n❌ Failed domains:');
        results.failed.forEach(d => console.log(`   • ${d}`));
        console.log('\n💡 Re-run failed: node multi-audit.js ' + results.failed.join(' '));
    }

    const stats = {
        batchPath:             SCAN_BATCH_PATH,
        batchFolder,
        timestamp:             new Date().toISOString(),
        total:                 results.total,
        ranThisRun:            ranCount,
        successful:            results.completed.length,
        failed:                results.failed.length,
        skippedByCheckpoint:   doneBefore.size,
        failedDomains:         results.failed,
        failedDetails:         results.failedDetails,
        failedLogFile:         FAILED_LOG_FILE,
        globalFailedLogFile:   GLOBAL_FAILED_LOG_FILE,
        errors:                results.errors,
        timing: {
            totalSeconds:       totalTime,
            avgSecondsPerDomain: avgPerDomain,
            groups:             results.timing.groups,
        },
        config:  CONFIG,
        memory:  {
            legacy_limit_mb:      MEMORY_LIMIT_MB,
            warnings:             memoryWarningCount,
            system_at_finish:     getSystemMemoryStats(),
            critical_pct_threshold: SYSTEM_MEM_CRITICAL_PCT,
            high_pct_threshold:     SYSTEM_MEM_HIGH_PCT,
        },
        cpu_load: {
            warnings:                 loadWarningCount,
            system_at_finish:         getSystemLoadStats(),
            critical_per_cpu_threshold: LOAD_PER_CPU_CRITICAL,
            high_per_cpu_threshold:     LOAD_PER_CPU_HIGH,
        },
    };
    fs.writeFileSync(statsPath, JSON.stringify(stats, null, 2));
    console.log(`\n📁 Stats: ${statsPath}`);

    writeProgress(results.doneSet.size, results.total, '', '', `DONE — ${results.doneSet.size} domains`, allDoneForProgress(), results.failed);

    if (results.failed.length === 0) {
        deleteCheckpoint();
        console.log('[checkpoint] Deleted (clean run)');
    } else {
        console.log(`[checkpoint] Kept — restart to resume the ${results.failed.length} failed domain(s)`);
    }

    clearInterval(memoryMonitor);
    console.log('\n✅ Multi-audit complete');
}

runAll().catch(err => {
    console.error('[multi-audit] Fatal:', err);
    process.exit(1);
});