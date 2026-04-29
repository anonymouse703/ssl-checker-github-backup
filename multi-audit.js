/**
 * multi-audit.js
 * Batch processor for multiple domains
 * Optimized for memory safety with 10-15 domains
 */

'use strict';

const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const { resolveBatchPath } = require('./audit-paths');
const { parseCSVRow } = require('./utils/csv-writer');
const { updateLatestResults } = require('./utils/latest-results');

// ── Parse domains first to determine optimal config ──────────────────────────
const args = process.argv.slice(2);
let domains = [];

console.log('\n🔍 MULTI-AUDIT DEBUG:');
console.log(`   Command line args: ${JSON.stringify(args)}`);

if (args.length === 1 && args[0].endsWith('.txt')) {
    const filePath = path.join(__dirname, args[0]);
    console.log(`   Reading from file: ${filePath}`);
    
    if (!fs.existsSync(filePath)) {
        console.error(`❌ File not found: ${filePath}`);
        process.exit(1);
    }
    
    const fileContent = fs.readFileSync(filePath, 'utf8');
    console.log(`   File content:\n${fileContent}`);
    
    domains = fileContent
        .split('\n')
        .map(d => d.trim())
        .filter(d => d && !d.startsWith('#'));
        
    console.log(`   Parsed ${domains.length} domains from file`);
} else {
    domains = args.filter(a => !a.startsWith('--'));
    console.log(`   Parsed ${domains.length} domains from command line: ${domains}`);
}

if (domains.length === 0) {
    console.log('Usage: node multi-audit.js domains.txt');
    console.log('       node multi-audit.js gotfloor.com google.com');
    process.exit(1);
}

// Sanitize function
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

const uniqueDomains = [...new Set(
    domains.map(sanitizeDomain).filter(Boolean)
)];

const total = uniqueDomains.length;

console.log(`\n✅ Final domains to process: ${uniqueDomains.length}`);
console.log(`   Domains: ${uniqueDomains.join(', ')}\n`);

// ── Memory-safe Configuration ─────────────────────────────────────────────────
const MEMORY_LIMIT_MB = parseInt(process.env.MEMORY_LIMIT_MB || '800', 10);
const MAX_CONCURRENT_DEFAULT = parseInt(process.env.MAX_CONCURRENT || '3', 10);

let CONFIG = {
    MAX_CONCURRENT: Math.min(4, MAX_CONCURRENT_DEFAULT),
    SSL_STAGGER_SEC: 60,
    GROUP_DELAY_MS: 30000,  // 30 seconds for memory cleanup
    DOMAIN_LAUNCH_DELAY_MS: 3000,  // 3 seconds between launches
};

// For large batches, be more conservative
if (total > 15) {
    CONFIG.MAX_CONCURRENT = Math.min(3, MAX_CONCURRENT_DEFAULT);
    CONFIG.GROUP_DELAY_MS = 45000;  // 45 seconds
}

console.log(`\n💾 Memory configuration:`);
console.log(`   • Memory limit: ${MEMORY_LIMIT_MB}MB`);
console.log(`   • Concurrent domains: ${CONFIG.MAX_CONCURRENT}`);
console.log(`   • Group cooldown: ${CONFIG.GROUP_DELAY_MS/1000}s`);
console.log(`   • Launch delay: ${CONFIG.DOMAIN_LAUNCH_DELAY_MS/1000}s\n`);

// ── Resolve batch path ONCE — all children share it ──────────────────────────
const SCAN_BATCH_PATH = resolveBatchPath();
console.log(`📁 Batch folder: ${SCAN_BATCH_PATH}`);

// Create the batch folder
if (!fs.existsSync(SCAN_BATCH_PATH)) {
    fs.mkdirSync(SCAN_BATCH_PATH, { recursive: true });
    console.log(`✓ Created batch folder: ${SCAN_BATCH_PATH}`);
}

const batchFolder = path.basename(SCAN_BATCH_PATH);
console.log(`✓ Batch folder name: ${batchFolder}`);

// Create summary.csv with headers if it doesn't exist
const summaryPath = path.join(SCAN_BATCH_PATH, 'summary.csv');
const { SUMMARY_FIELDS } = require('./config/constants');

if (!fs.existsSync(summaryPath)) {
    fs.writeFileSync(summaryPath, SUMMARY_FIELDS.join(',') + '\n', 'utf8');
    console.log(`✓ Created summary.csv at: ${summaryPath}`);
}

// Create batch stats file
const statsPath = path.join(SCAN_BATCH_PATH, '_batch_stats.json');

// ── Progress file for FileMaker ──────────────────────────────────────────────
const OUTPUT_DIR = process.env.OUTPUT_DIR || '/home/ind/ind_leads_inputs';
const JOB_ID = process.env.JOB_ID || `batch_${Date.now()}`;
const PROGRESS_FILE = path.join(OUTPUT_DIR, `progress_${JOB_ID}.txt`);

if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
}

// ── Updated writeProgress — now tracks per-domain completion lists ────────────
function writeProgress(completed, total, lastDomain, finishTime, status, completedList, failedList) {
    try {
        const lines = [
            `completed=${completed}`,
            `total=${total}`,
            `last_domain=${lastDomain}`,
            `last_finish=${finishTime}`,
            `status=${status}`,
            `job_id=${JOB_ID}`,
            `completed_domains=${(completedList || []).join(',')}`,
            `failed_domains=${(failedList || []).join(',')}`,
        ];
        fs.writeFileSync(PROGRESS_FILE, lines.join('\n'), 'utf8');
        const pct = Math.round((completed / total) * 100);
        console.log(`[progress] 📊 ${completed}/${total} (${pct}%) - ${status}`);
    } catch (e) {
        console.error(`[progress] Error: ${e.message}`);
    }
}

// ── Time estimate ─────────────────────────────────────────────────────────────
const groups = Math.ceil(total / CONFIG.MAX_CONCURRENT);
const sslTotalStagger = (CONFIG.MAX_CONCURRENT - 1) * CONFIG.SSL_STAGGER_SEC;
const estMinPerGroup = Math.ceil((4 * 60 + sslTotalStagger) / 60);
const estTotalMin = groups * estMinPerGroup + Math.ceil((groups - 1) * CONFIG.GROUP_DELAY_MS / 60000);

// ── Banner ────────────────────────────────────────────────────────────────────
console.log('\n╔══════════════════════════════════════════════════════════════════════════╗');
console.log('║                      FM AUDIT TOOL — BATCH PROCESSOR                      ║');
console.log('╚══════════════════════════════════════════════════════════════════════════╝');
console.log(`📋 Domains            : ${total}`);
console.log(`⚡ Concurrent         : ${CONFIG.MAX_CONCURRENT} domains/group`);
console.log(`🔒 SSL stagger        : ${CONFIG.SSL_STAGGER_SEC}s between domains`);
console.log(`⏱️  Group cooldown     : ${CONFIG.GROUP_DELAY_MS/1000}s`);
console.log(`🗂️  Batch folder       : ${batchFolder}/`);
console.log(`__BATCH_FOLDER__:${batchFolder}`);
console.log(`⏳ Estimated time     : ~${estTotalMin} min`);
console.log(`📊 Groups             : ${groups} groups\n`);

console.log('📁 Output structure:');
console.log(`   ${SCAN_BATCH_PATH}/`);
console.log(`   ├── summary.csv                ← batch summary (all domains)`);
console.log(`   ├── _batch_stats.json`);
console.log(`   └── [domain1]/`);
console.log(`       ├── summary.csv            ← per-domain summary`);
console.log(`       ├── [domain1]_results.csv  ← full per-domain CSV`);
console.log(`       └── images/`);
console.log(`           ├── ssl.png`);
console.log(`           ├── intodns.png`);
console.log(`           ├── pagespeed.png`);
console.log(`           ├── pingdom.png`);
console.log(`           ├── sucuri.png`);
console.log(`           └── ...\n`);

// Write initial progress
writeProgress(0, total, '', '', 'RUNNING', [], []);

const overallStart = Date.now();
const results = {
    completed: [],
    failed: [],
    errors: {
        sslLabs: 0,
        gtmetrix: 0,
        sslLabsDetails: {},
        gtmetrixDetails: {},
    },
    timing: { groups: [] }
};

// ── Memory monitoring for safe operation ──────────────────────────────────────
let memoryWarningCount = 0;

function checkMemory() {
    const usage = process.memoryUsage();
    const rssMB = Math.round(usage.rss / 1024 / 1024);
    const heapMB = Math.round(usage.heapUsed / 1024 / 1024);
    
    if (rssMB > MEMORY_LIMIT_MB) {
        console.log(`[memory] ⚠️ CRITICAL: Memory at ${rssMB}MB (limit: ${MEMORY_LIMIT_MB}MB)`);
        memoryWarningCount++;
        
        if (memoryWarningCount > 3) {
            console.log(`[memory] 🔥 Memory threshold exceeded! Reducing concurrency for next batch...`);
            CONFIG.GROUP_DELAY_MS = 60000; // 60 seconds
        }
        
        if (global.gc) {
            console.log(`[memory] Forcing garbage collection...`);
            global.gc();
        }
    } else if (rssMB > MEMORY_LIMIT_MB * 0.7) {
        console.log(`[memory] ⚠️ High memory: ${rssMB}MB / ${MEMORY_LIMIT_MB}MB (${Math.round((rssMB/MEMORY_LIMIT_MB)*100)}%)`);
    } else {
        console.log(`[memory] ✓ Memory: ${rssMB}MB RSS | ${heapMB}MB heap`);
    }
    
    return rssMB;
}

// Start memory monitor
const memoryMonitor = setInterval(() => {
    if (results.completed.length + results.failed.length < total) {
        checkMemory();
    } else {
        clearInterval(memoryMonitor);
    }
}, 30000);

// ── Helper to read domain CSV and update latest results ──────────────────────
async function updateLatestForDomain(domain) {
    const domainCsvPath = path.join(SCAN_BATCH_PATH, domain, `${domain}_results.csv`);
    
    if (!fs.existsSync(domainCsvPath)) {
        console.log(`[multi-audit] No CSV found for ${domain} at ${domainCsvPath}`);
        return false;
    }

    try {
        const content = fs.readFileSync(domainCsvPath, "utf8");
        const lines = content.split("\n").filter(l => l.trim());
        
        if (lines.length < 2) {
            console.log(`[multi-audit] Invalid CSV for ${domain}: not enough lines`);
            return false;
        }

        const headers = parseCSVRow(lines[0]);
        const cols = parseCSVRow(lines[lines.length - 1]);
        
        const rowData = {};
        headers.forEach((h, idx) => {
            rowData[h] = cols[idx] || "";
        });
        
        await updateLatestResults(rowData);
        console.log(`[multi-audit] ✅ Updated latest results for ${domain}`);
        return true;
    } catch (err) {
        console.error(`[multi-audit] Failed to update latest for ${domain}: ${err.message}`);
        return false;
    }
}

// ── Run a single domain audit ─────────────────────────────────────────────────

const FORCE_RESCAN = String(process.env.FORCE_RESCAN || '0') === '1';

function runAudit(domain, groupNum, posInGroup) {
  return new Promise((resolve) => {
    const start = Date.now();
    const label = `[G${groupNum}-D${posInGroup + 1}]`.padEnd(12);
    const sslDelayMs = posInGroup * CONFIG.SSL_STAGGER_SEC * 1000;

    console.log(`${label} 🚀 ${domain}`);
    console.log(`${label}    📁 ${path.join(batchFolder, domain)}/`);
    if (sslDelayMs > 0) {
      console.log(`${label}    🔒 SSL checks will start in ${sslDelayMs / 1000}s (stagger)`);
    }

    const childJobId = `${JOB_ID}_${domain.replace(/[^a-z0-9._-]/gi, '_')}`;

    const child = spawn('node', ['index.js', domain], {
      cwd: __dirname,
      env: {
        ...process.env,
        SCAN_BATCH_PATH,
        SSL_QUEUE_DELAY_MS: String(sslDelayMs),
        BATCH_MODE: 'true',
        JOB_ID: childJobId,
        // DO NOT set DOMAIN_LOCK_ALREADY_HELD – child will acquire its own lock
        ENABLED_TOOLS: process.env.ENABLED_TOOLS || '[]',
        NODE_OPTIONS: '--max-old-space-size=256',
        FORCE_RESCAN: FORCE_RESCAN ? '1' : '0',
      }
    });

    console.log(`__DOMAIN_START__:${domain}:${child.pid}:${childJobId}`);

    child.stdout.on('data', (data) => {
      String(data).split('\n').forEach(line => {
        const t = line.trim();
        if (!t) return;

        if (t.includes('SSL ⏳ Capacity')) {
          results.errors.sslLabs++;
          results.errors.sslLabsDetails[domain] = (results.errors.sslLabsDetails[domain] || 0) + 1;
        }

        if (t.includes('GTmetrix') && (t.includes('403') || t.includes('444'))) {
          results.errors.gtmetrix++;
          (results.errors.gtmetrixDetails[domain] = results.errors.gtmetrixDetails[domain] || []).push(t);
        }

        console.log(`${label} ${t}`);
      });
    });

    child.stderr.on('data', (data) => {
      String(data).split('\n').forEach(line => {
        const t = line.trim();
        if (t) console.error(`${label} ⚠️  ${t}`);
      });
    });

    child.on('close', async (code) => {
      const elapsed = Math.round((Date.now() - start) / 1000);
      const finishTime = new Date().toLocaleString('en-US', {
        month: '2-digit',
        day: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true
      });

      if (code === 0) {
        results.completed.push(domain);
        console.log(`${label} ✅ ${Math.floor(elapsed / 60)}m ${elapsed % 60}s → ${path.join(batchFolder, domain)}/`);
        await updateLatestForDomain(domain);
      } else {
        results.failed.push(domain);
        console.log(`${label} ❌ Failed in ${Math.floor(elapsed / 60)}m ${elapsed % 60}s`);
      }

      const doneCount = results.completed.length + results.failed.length;

      // ── Pass per-domain lists into writeProgress ──────────────────────────
      writeProgress(
        doneCount,
        total,
        domain,
        finishTime,
        code === 0 ? `RUNNING — completed ${domain}` : `RUNNING — failed ${domain}`,
        results.completed,
        results.failed
      );

      resolve({
        domain,
        code,
        elapsed,
        success: code === 0,
      });
    });
  });
}

// ── Main execution ────────────────────────────────────────────────────────────
async function runAll() {
    const groups = [];
    for (let i = 0; i < uniqueDomains.length; i += CONFIG.MAX_CONCURRENT) {
        groups.push(uniqueDomains.slice(i, i + CONFIG.MAX_CONCURRENT));
    }

    console.log(`📦 ${groups.length} group(s) × up to ${CONFIG.MAX_CONCURRENT} domains\n`);

    for (let g = 0; g < groups.length; g++) {
        const group = groups[g];
        const groupStart = Date.now();

        console.log('═'.repeat(70));
        console.log(`📦 GROUP ${g + 1}/${groups.length} — ${group.length} domain(s)`);
        console.log(`   🔒 SSL schedule:`);
        group.forEach((d, i) => {
            const delay = i * CONFIG.SSL_STAGGER_SEC;
            console.log(`      ${i === 0 ? '→' : '⏳'} ${d.padEnd(35)} SSL starts at t+${delay}s`);
        });
        console.log('═'.repeat(70));

        const promises = group.map((domain, i) =>
            new Promise(resolve =>
                setTimeout(
                    () => resolve(runAudit(domain, g + 1, i)),
                    i * CONFIG.DOMAIN_LAUNCH_DELAY_MS
                )
            )
        );

        await Promise.all(promises);

        const groupTime = Math.round((Date.now() - groupStart) / 1000);
        results.timing.groups.push({ group: g + 1, domains: group.length, timeSec: groupTime });

        const sslHitsThisGroup = group.reduce((n, d) => n + (results.errors.sslLabsDetails[d] || 0), 0);

        console.log('─'.repeat(70));
        console.log(`✅ GROUP ${g + 1} done in ${Math.floor(groupTime/60)}m ${groupTime%60}s`);
        console.log(`   Progress : ${results.completed.length}/${total} OK  |  ${results.failed.length} failed`);
        console.log(`   SSL capacity hits this group: ${sslHitsThisGroup} (total: ${results.errors.sslLabs})`);

        // Enhanced cooldown with memory cleanup
        if (g < groups.length - 1) {
            console.log(`\n⏸️  Group cooldown ${CONFIG.GROUP_DELAY_MS/1000}s...`);
            
            const beforeMem = checkMemory();
            
            await new Promise(r => setTimeout(r, CONFIG.GROUP_DELAY_MS));
            
            if (global.gc) {
                console.log(`[memory] Running GC before next group...`);
                global.gc();
                await new Promise(r => setTimeout(r, 1000));
            }
            
            const afterMem = checkMemory();
            const freed = beforeMem - afterMem;
            if (freed > 0) {
                console.log(`[memory] ✅ Freed ~${freed}MB after cooldown`);
            }
            
            console.log('');
        }
    }

    // ── Final report ──────────────────────────────────────────────────────────
    const totalTime = Math.round((Date.now() - overallStart) / 1000);
    const avgPerDomain = Math.round(totalTime / total);

    console.log('\n╔══════════════════════════════════════════════════════════════════════════╗');
    console.log('║                            FINAL RESULTS                                  ║');
    console.log('╚══════════════════════════════════════════════════════════════════════════╝');
    console.log(`⏱️  Total time      : ${Math.floor(totalTime/60)}m ${totalTime%60}s`);
    console.log(`📊 Avg per domain   : ${Math.floor(avgPerDomain/60)}m ${avgPerDomain%60}s`);
    console.log(`✅ Successful       : ${results.completed.length}/${total}`);
    console.log(`❌ Failed           : ${results.failed.length}/${total}`);

    if (results.errors.sslLabs > 0) {
        console.log(`\n📊 SSL capacity hits: ${results.errors.sslLabs}`);
    }

    if (results.failed.length > 0) {
        console.log('\n❌ Failed domains:');
        results.failed.forEach(d => console.log(`   • ${d}`));
        console.log(`\n💡 Re-run failed domains: node multi-audit.js ${results.failed.join(' ')}`);
    }

    // Save stats JSON
    const stats = {
        batchPath: SCAN_BATCH_PATH,
        batchFolder: batchFolder,
        timestamp: new Date().toISOString(),
        total,
        successful: results.completed.length,
        failed: results.failed.length,
        failedDomains: results.failed,
        errors: results.errors,
        timing: {
            totalSeconds: totalTime,
            avgSecondsPerDomain: avgPerDomain,
            groups: results.timing.groups,
        },
        config: CONFIG,
        memory: {
            limit_mb: MEMORY_LIMIT_MB,
            warnings: memoryWarningCount
        }
    };
    fs.writeFileSync(statsPath, JSON.stringify(stats, null, 2));
    console.log(`\n📁 Stats saved to: ${statsPath}`);

    console.log('\n📁 Results:');
    console.log(`   ${SCAN_BATCH_PATH}/`);
    results.completed.slice(0, 8).forEach(d => {
        console.log(`   ├── ${d}/`);
    });
    if (results.completed.length > 8) console.log(`   ├── … (${results.completed.length - 8} more)`);
    console.log(`   ├── summary.csv`);
    console.log(`   └── _batch_stats.json`);

    // Verify batch summary
    if (fs.existsSync(summaryPath)) {
        const content = fs.readFileSync(summaryPath, 'utf8');
        const lines = content.split('\n').filter(l => l.trim());
        const rowCount = lines.length - 1;
        console.log(`\n📊 Batch summary has ${rowCount} domains (expected ${total})`);
        if (rowCount < total) {
            console.log(`⚠️ Warning: Missing ${total - rowCount} domains in batch summary`);
            console.log(`   Completed domains: ${results.completed.length}`);
            console.log(`   Failed domains: ${results.failed.length}`);
        }
    }

    if (results.failed.length === 0) console.log('\n🎉 100% success rate!');
    console.log(`\n✅ Multi-audit completed`);

    // ── Mark progress as complete — pass final lists ──────────────────────────
    writeProgress(total, total, '', '', `DONE - ${total} domains completed`, results.completed, results.failed);
    
    // Clear memory monitor
    clearInterval(memoryMonitor);
}

runAll().catch(console.error);