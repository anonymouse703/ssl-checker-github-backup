/**
 * multi-audit.js
 *
 * EXPECTED TIMES (with SSL_STAGGER_SEC=60):
 *   2  domains @ 3 concurrent = ~8-12 min  (was 33+ min)
 *   10 domains @ 3 concurrent = ~12-18 min (was 45+ min)
 *   50 domains @ 3 concurrent = ~40-55 min
 */

'use strict';

const { spawn }  = require('child_process');
const path       = require('path');
const fs         = require('fs');
const { resolveBatchPath } = require('./audit-paths');

// ── Configuration ─────────────────────────────────────────────────────────────
const CONFIG = {
    MAX_CONCURRENT: parseInt(process.env.MAX_CONCURRENT || '2', 10),
    SSL_STAGGER_SEC: 60,      // 60s is enough to avoid SSL Labs rate limits (was 270s = 4.5 min!)
    GROUP_DELAY_MS: 15000,    // 15s cooldown between groups (was 60s)
    DOMAIN_LAUNCH_DELAY_MS: 2000,  // 2s process-spawn stagger (was 5s)
};

// ── Parse domains ─────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let domains = [];

if (args.length === 1 && args[0].endsWith('.txt')) {
    const filePath = path.join(__dirname, args[0]);
    domains = fs.readFileSync(filePath, 'utf8')
        .split('\n')
        .map(d => d.trim())
        .filter(d => d && !d.startsWith('#'));
} else {
    domains = args.filter(a => !a.startsWith('--'));
}

if (domains.length === 0) {
    console.log('Usage: node multi-audit.js domains.txt');
    console.log('       node multi-audit.js gotfloor.com google.com');
    process.exit(1);
}

const uniqueDomains = [...new Set(domains)];
const total         = uniqueDomains.length;

// ── Resolve batch path ONCE — all children share it ──────────────────────────
const SCAN_BATCH_PATH = resolveBatchPath();
if (!fs.existsSync(SCAN_BATCH_PATH)) {
    fs.mkdirSync(SCAN_BATCH_PATH, { recursive: true });
}

// ── Progress file — FileMaker reads this to show live status ──────────────────
const OUTPUT_DIR = '/home/ind/ind_leads_inputs';
const JOB_ID = process.env.JOB_ID || `batch_${Date.now()}`;
const PROGRESS_FILE = path.join(OUTPUT_DIR, `progress_${JOB_ID}.txt`);

// Ensure output dir exists
const progressDir = path.dirname(PROGRESS_FILE);
if (!fs.existsSync(progressDir)) fs.mkdirSync(progressDir, { recursive: true });

function writeProgress(completed, total, lastDomain, finishTime, status) {
    try {
        const lines = [
            `completed=${completed}`,
            `total=${total}`,
            `last_domain=${lastDomain}`,
            `last_finish=${finishTime}`,
            `status=${status}`,
        ];
        fs.writeFileSync(PROGRESS_FILE, lines.join('\n'), 'utf8');
    } catch (e) {
        // Non-fatal — don't crash the audit if progress file can't be written
    }
}

// ── Labels ────────────────────────────────────────────────────────────────────
const now         = new Date();
const batchFolder = path.basename(SCAN_BATCH_PATH);   // e.g. 2026-03-01 or 2026-03-01-2
const statsPath   = path.join(SCAN_BATCH_PATH, '_batch_stats.json');

// ── Time estimate ─────────────────────────────────────────────────────────────
const groups          = Math.ceil(total / CONFIG.MAX_CONCURRENT);
const sslTotalStagger = (CONFIG.MAX_CONCURRENT - 1) * CONFIG.SSL_STAGGER_SEC; 
const estMinPerGroup  = Math.ceil((4 * 60 + sslTotalStagger) / 60);        
const estTotalMin     = groups * estMinPerGroup + Math.ceil((groups - 1) * CONFIG.GROUP_DELAY_MS / 60000);

// ── Banner ────────────────────────────────────────────────────────────────────
console.log('╔══════════════════════════════════════════════════════════════════════════╗');
console.log('║                      FM AUDIT TOOL — BATCH PROCESSOR                      ║');
console.log('╚══════════════════════════════════════════════════════════════════════════╝');
console.log(`📋 Domains            : ${total}`);
console.log(`⚡ Concurrent         : ${CONFIG.MAX_CONCURRENT} domains/group`);
console.log(`🔒 SSL stagger        : ${CONFIG.SSL_STAGGER_SEC}s between domains`);
console.log(`⏱️  Group cooldown     : ${CONFIG.GROUP_DELAY_MS/1000}s`);
console.log(`🗂️  Batch folder       : ${batchFolder}/`);
console.log(`__BATCH_FOLDER__:${batchFolder}`);
console.log(`⏳ Estimated time     : ~${estTotalMin} min`);
console.log('');
console.log('');
console.log('📁 Output structure:');
console.log(`   /home/ind/${batchFolder}/`);
console.log(`   ├── summary.csv          ← all domains in one file`);
console.log(`   └── _batch_stats.json`);
console.log('');

// Write initial progress so FileMaker sees 0/total immediately
writeProgress(0, total, '', '', 'RUNNING');

const overallStart = Date.now();
const results = {
    completed:   [],
    failed:      [],
    errors: {
        sslLabs:          0,
        gtmetrix:         0,
        sslLabsDetails:   {},
        gtmetrixDetails:  {},
    },
    timing: { groups: [] }
};

// ── Run a single domain audit ─────────────────────────────────────────────────
function runAudit(domain, groupNum, posInGroup) {
    return new Promise((resolve) => {
        const start = Date.now();
        const label = `[G${groupNum}-D${posInGroup + 1}]`.padEnd(12);

        // Stagger SSL position 0 = no delay, position 1 = 45s, position 2 = 90s
        const sslDelayMs = posInGroup * CONFIG.SSL_STAGGER_SEC * 1000;

        console.log(`${label} 🚀 ${domain}`);
        if (sslDelayMs > 0) {
            console.log(`${label}    🔒 SSL checks will start in ${sslDelayMs/1000}s (stagger)`);
        }
        console.log(`${label}    📁 /home/ind/${batchFolder}/${domain}/`);

        const childJobId = `${JOB_ID}_${domain.replace(/[^a-z0-9._-]/gi, '_')}`;
        const child = spawn('node', ['index.js', domain], {
            cwd: __dirname,
            env: {
                ...process.env,
                SCAN_BATCH_PATH,
                SSL_QUEUE_DELAY_MS: String(sslDelayMs),   // ← tells full audit when to start SSL
                BATCH_MODE: 'true',
                JOB_ID: childJobId,
                DOMAIN_LOCK_ALREADY_HELD: '1',
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

        child.on('close', (code) => {
            const elapsed = Math.round((Date.now() - start) / 1000);
            const finishTime = new Date().toLocaleString('en-US', {
                month: '2-digit', day: '2-digit', year: 'numeric',
                hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
            });

            if (code === 0) {
                results.completed.push(domain);
                console.log(`${label} ✅ ${Math.floor(elapsed/60)}m ${elapsed%60}s → /home/ind/${batchFolder}/${domain}/`);
            } else {
                results.failed.push(domain);
                console.log(`${label} ❌ Failed in ${Math.floor(elapsed/60)}m ${elapsed%60}s`);
            }

            // Update progress file — FileMaker polls this
            const doneCount = results.completed.length + results.failed.length;
            const statusLine = code === 0
                ? `✅ ${domain} — finished at ${finishTime}`
                : `❌ ${domain} — failed at ${finishTime}`;
            writeProgress(doneCount, total, domain, finishTime, statusLine);
            console.log(`${label} 📊 Progress: ${doneCount}/${total}`);
            console.log(`__DOMAIN_DONE__:${domain}:${code}`);

            resolve();
        });

        child.on('error', (err) => {
            console.error(`${label} ❌ ${err.message}`);
            results.failed.push(domain);
            console.log(`__DOMAIN_DONE__:${domain}:1`);
            resolve();
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
        const group      = groups[g];
        const groupStart = Date.now();

        console.log('═'.repeat(70));
        console.log(`📦 GROUP ${g + 1}/${groups.length} — ${group.length} domain(s)`);
        console.log(`   🔒 SSL schedule:`);
        group.forEach((d, i) => {
            const delay = i * CONFIG.SSL_STAGGER_SEC;
            console.log(`      ${i===0?'→':'⏳'} ${d.padEnd(35)} SSL starts at t+${delay}s`);
        });
        console.log('═'.repeat(70));

        // Launch all domains with a tiny process-spawn stagger (2s),
        // but SSL staggering is handled inside each child via SSL_QUEUE_DELAY_MS
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

        if (g < groups.length - 1) {
            console.log(`\n⏸️  Group cooldown ${CONFIG.GROUP_DELAY_MS/1000}s...`);
            await new Promise(r => setTimeout(r, CONFIG.GROUP_DELAY_MS));
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
    console.log('');
    console.log('📊 SSL capacity hits:');
    if (results.errors.sslLabs === 0) {
        console.log('   ✅ Zero capacity hits! Staggering worked perfectly.');
    } else if (results.errors.sslLabs <= total) {
        console.log(`   ⚠️  ${results.errors.sslLabs} hits (minor — ${(results.errors.sslLabs/total).toFixed(1)}/domain)`);
        console.log(`   💡 Consider increasing SSL_STAGGER_SEC to ${CONFIG.SSL_STAGGER_SEC + 15}`);
    } else {
        console.log(`   ❌ ${results.errors.sslLabs} hits — increase SSL_STAGGER_SEC to ${CONFIG.SSL_STAGGER_SEC + 30}`);
    }

    const blockingDomains = Object.keys(results.errors.gtmetrixDetails);
    if (blockingDomains.length > 0) {
        console.log(`\n🚫 GTmetrix blocked by ${blockingDomains.length} domain(s) (normal for some sites)`);
    }

    if (results.failed.length > 0) {
        console.log('\n❌ Failed domains:');
        results.failed.forEach(d => console.log(`   • ${d}`));
        console.log(`\n💡 Re-run failed domains: node multi-audit.js ${results.failed.join(' ')}`);
    }

    // Save stats JSON
    const stats = {
        batchPath:   SCAN_BATCH_PATH,
        batchFolder: batchFolder,
        timestamp:   new Date().toISOString(),
        total, successful: results.completed.length, failed: results.failed.length,
        failedDomains: results.failed,
        errors:  results.errors,
        timing: {
            totalSeconds:        totalTime,
            avgSecondsPerDomain: avgPerDomain,
            groups:              results.timing.groups,
        },
        config: CONFIG,
    };
    fs.writeFileSync(statsPath, JSON.stringify(stats, null, 2));

    console.log('\n📁 Results:');
    console.log(`   ${SCAN_BATCH_PATH}/`);
    results.completed.slice(0, 8).forEach(d => {
        console.log(`   ├── ${d}/`);
    });
    if (results.completed.length > 8) console.log(`   ├── … (${results.completed.length - 8} more)`);
    console.log(`   ├── summary.csv`);
    console.log(`   └── _batch_stats.json`);
    console.log('');
    if (results.failed.length === 0) console.log('🎉 100% success rate!');
    console.log(`✅ Multi-audit completed with exit code 0`);

    // Mark progress as complete so FileMaker knows to stop polling
    writeProgress(total, total, '', '', `DONE job_id=${JOB_ID}`);
}

runAll().catch(console.error);