'use strict';

const { getErrorCode } = require('../utils/error-codes');
const path = require('path');
const {
  setFixedViewport,
  captureViewportWidthFullHeight,
  wait,
} = require('../utils/screenshot');

const INDEPTHDNS_VIEWPORT_WIDTH  = 1100;
const INDEPTHDNS_VIEWPORT_HEIGHT = 900;

// How long to wait for Phase 1 to finish (tool says it returns core DNS first).
const PHASE1_TIMEOUT_MS = parseInt(process.env.INDEPTHDNS_PHASE1_TIMEOUT_MS || '60000', 10);
// How long to wait for Phase 2 (email / security / SMTP checks) on top of Phase 1.
const PHASE2_TIMEOUT_MS = parseInt(process.env.INDEPTHDNS_PHASE2_TIMEOUT_MS || '90000', 10);
// Poll interval while waiting.
const POLL_MS = 1500;

async function runInDepthDNS(domain, context) {
  const { newTab, paths } = context;
  const screenshotPath = path.join(paths.imagesDir, 'indepthdns.png');
  const tab = await newTab();

  const toolUrl  = 'https://tool.indepthdns.com/';
  const resultUrl = toolUrl; // SPA — URL stays the same after submission

  // ── helpers ──────────────────────────────────────────────────────────────────

  /**
   * Read the current counter values shown in the results header:
   *   ALL / PASS / WARN / FAIL / INFO
   * Returns null when the counters are still at their initial "0 / Queued…" state.
   */
  async function readCounters(page) {
    return page.evaluate(() => {
      // The tool renders bold counters next to labels ALL / PASS / WARN / FAIL / INFO.
      // We scrape by finding the label text inside the results summary area.
      const bodyText = document.body ? document.body.innerText : '';

      function extractCount(label) {
        // Matches patterns like "PASS\n12" or "PASS  12" or "PASS **12**"
        const m = bodyText.match(new RegExp(label + '[\\s\\S]{0,10}?(\\d+)', 'i'));
        return m ? parseInt(m[1], 10) : null;
      }

      const all  = extractCount('ALL');
      const pass = extractCount('PASS');
      const warn = extractCount('WARN');
      const fail = extractCount('FAIL');
      const info = extractCount('INFO');

      return { all, pass, warn, fail, info };
    }).catch(() => null);
  }

  /**
   * Returns one of:
   *   'idle'      – page loaded, no report running yet
   *   'running'   – report in progress (spinner / "Running…" text visible)
   *   'phase1'    – Phase 1 completed, Phase 2 still going
   *   'done'      – both phases finished
   *   'error'     – page shows an error state
   */
  async function getReportState(page) {
    return page.evaluate(() => {
      const text = (document.body ? document.body.innerText : '').replace(/\s+/g, ' ');
      const lower = text.toLowerCase();

      // Error states
      if (/error|failed to fetch|network error|invalid domain/i.test(text)) {
        // Only treat as error if it appears in a result/alert context, not just labels
        const hasResultSection = /phase 1 time|phase 2 time|total elapsed/i.test(text);
        if (!hasResultSection && /error/i.test(text)) return 'error';
      }

      // Finished: both phase times are filled in (not "—")
      const phaseTimeFilled = (label) => {
        const m = text.match(new RegExp(label + '[\\s\\S]{0,20}?(\\d)', 'i'));
        return !!m;
      };

      const phase1Done = phaseTimeFilled('Phase 1 time');
      const phase2Done = phaseTimeFilled('Phase 2 time');

      if (phase1Done && phase2Done) return 'done';
      if (phase1Done) return 'phase1';

      // Running indicators
      if (
        /running|queued|checking|scanning|fetching/i.test(lower) ||
        document.querySelector('[data-loading], .spinner, .loading, [aria-busy="true"]')
      ) {
        return 'running';
      }

      // If counters exist and are non-zero something is happening
      const allMatch = text.match(/ALL\s*[*\s]*(\d+)/i);
      if (allMatch && parseInt(allMatch[1], 10) > 0) return 'phase1';

      return 'idle';
    }).catch(() => 'idle');
  }

  /** Fill the domain input and click Run report */
  async function submitDomain(page, domainArg) {
    // Try the most specific selector first, then fall back progressively.
    const inputSelectors = [
      'input[name="domain"]',
      'input[placeholder*="domain" i]',
      'input[type="text"]',
      'input[type="search"]',
      'input',
    ];

    let filled = false;
    for (const sel of inputSelectors) {
      try {
        await page.waitForSelector(sel, { timeout: 8000 });
        await page.click(sel, { clickCount: 3 });
        await page.keyboard.press('Backspace');
        await page.type(sel, domainArg, { delay: 30 });
        filled = true;
        console.log(`[indepthdns] Filled domain using selector: ${sel}`);
        break;
      } catch (_) { /* try next */ }
    }

    if (!filled) {
      throw new Error('Could not find domain input field on tool.indepthdns.com');
    }

    await wait(400);

    // Click the submit button
    const submitted = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button, input[type="submit"]'));
      const runBtn =
        buttons.find(el => /run\s*report|submit|check|lookup/i.test((el.innerText || el.value || '').trim())) ||
        document.querySelector('button[type="submit"]') ||
        document.querySelector('input[type="submit"]') ||
        buttons[0];

      if (runBtn) { runBtn.click(); return true; }

      const form = document.querySelector('form');
      if (form) { form.submit(); return true; }

      return false;
    });

    if (!submitted) {
      // Last resort: press Enter in the input
      await page.keyboard.press('Enter');
    }

    console.log(`[indepthdns] Report submitted for ${domainArg}`);
  }

  /** Wait until Phase 1 has data (or timeout). Returns true if Phase 1 done. */
  async function waitForPhase1(page) {
    const deadline = Date.now() + PHASE1_TIMEOUT_MS;

    while (Date.now() < deadline) {
      const state = await getReportState(page);
      if (state === 'phase1' || state === 'done') return true;
      if (state === 'error') {
        console.warn('[indepthdns] Error state detected during Phase 1 wait');
        return false;
      }
      await wait(POLL_MS);
    }

    console.warn(`[indepthdns] Phase 1 did not complete within ${PHASE1_TIMEOUT_MS}ms`);
    return false;
  }

  /** Wait until Phase 2 finishes (or timeout). Returns true if fully done. */
  async function waitForPhase2(page) {
    const deadline = Date.now() + PHASE2_TIMEOUT_MS;

    while (Date.now() < deadline) {
      const state = await getReportState(page);
      if (state === 'done') return true;
      if (state === 'error') {
        console.warn('[indepthdns] Error state detected during Phase 2 wait');
        return false;
      }
      await wait(POLL_MS);
    }

    console.warn(`[indepthdns] Phase 2 did not complete within ${PHASE2_TIMEOUT_MS}ms — capturing partial results`);
    return false;
  }

  /** Extract structured data from the completed report page */
  async function extractData(page) {
    return page.evaluate(() => {
      const clean = (v) => String(v || '').replace(/\s+/g, ' ').trim();
      const bodyText = clean(document.body ? document.body.innerText : '');

      function extractCount(label) {
        const m = bodyText.match(new RegExp(label + '[\\s\\S]{0,15}?(\\d+)', 'i'));
        return m ? parseInt(m[1], 10) : 0;
      }

      function extractTime(label) {
        // e.g. "Phase 1 time  1.23 s" or "Total elapsed  4.5s"
        const m = bodyText.match(new RegExp(label + '[\\s\\S]{0,20}?([\\d.]+\\s*s)', 'i'));
        return m ? clean(m[1]) : '—';
      }

      const counts = {
        all:  extractCount('ALL'),
        pass: extractCount('PASS'),
        warn: extractCount('WARN'),
        fail: extractCount('FAIL'),
        info: extractCount('INFO'),
      };

      const timing = {
        phase1: extractTime('Phase 1 time'),
        phase2: extractTime('Phase 2 time'),
        total:  extractTime('Total elapsed'),
      };

      // Derive an overall health from FAIL / WARN counts
      let overallHealth = 'GOOD';
      if (counts.fail > 0) overallHealth = 'FAIL';
      else if (counts.warn > 0) overallHealth = 'WARN';

      // Try to pull individual check rows for richer data.
      // The tool renders results as a list/table — each row has a status badge
      // (PASS / WARN / FAIL / INFO) followed by a label.
      const checkRows = [];
      const rows = Array.from(document.querySelectorAll(
        'tr, [class*="row"], [class*="check"], [class*="result"], li'
      ));

      for (const row of rows) {
        const rowText = clean(row.innerText || '');
        if (!rowText || rowText.length < 3) continue;

        // Look for a status badge in the row
        const statusMatch = rowText.match(/^(PASS|WARN|FAIL|INFO)\s+(.+)/i);
        if (statusMatch) {
          checkRows.push({
            status: statusMatch[1].toUpperCase(),
            label: clean(statusMatch[2]).substring(0, 120),
          });
        }
      }

      return {
        counts,
        timing,
        overallHealth,
        checkRows: checkRows.slice(0, 50), // cap at 50 rows
      };
    }).catch(() => ({
      counts: { all: 0, pass: 0, warn: 0, fail: 0, info: 0 },
      timing: { phase1: '—', phase2: '—', total: '—' },
      overallHealth: 'UNKNOWN',
      checkRows: [],
    }));
  }

  // ── main flow ─────────────────────────────────────────────────────────────────

  try {
    await setFixedViewport(tab, INDEPTHDNS_VIEWPORT_WIDTH, INDEPTHDNS_VIEWPORT_HEIGHT);

    console.log(`[indepthdns] Navigating to ${toolUrl}`);
    await tab.goto(toolUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

    // Wait for the page to be interactive
    await wait(1500);

    // Fill in the domain and submit
    await submitDomain(tab, domain);

    // Short pause to let the JS start the report
    await wait(2000);

    // Wait for Phase 1 results
    const phase1Ready = await waitForPhase1(tab);

    if (!phase1Ready) {
      // Take a debug screenshot and return an error
      try {
        await captureViewportWidthFullHeight(tab, screenshotPath, { width: INDEPTHDNS_VIEWPORT_WIDTH });
      } catch (_) {}

      return {
        status: 'FAILED',
        data: {
          counts: { all: 0, pass: 0, warn: 0, fail: 0, info: 0 },
          timing: { phase1: '—', phase2: '—', total: '—' },
          overallHealth: 'UNKNOWN',
          checkRows: [],
        },
        error: 'InDepthDNS Phase 1 did not produce results in time.',
        errorCode: getErrorCode({ error: 'INDEPTHDNS_PHASE1_TIMEOUT' }),
        url: toolUrl,
        screenshot: null,
      };
    }

    // Wait for Phase 2 (email / security checks) — we still screenshot even if partial
    await waitForPhase2(tab);

    // Extra settle before screenshot
    await wait(1500);

    // Scroll to top so the full report is captured from the beginning
    await tab.evaluate(() => window.scrollTo(0, 0)).catch(() => {});
    await wait(500);

    const data = await extractData(tab);

    await captureViewportWidthFullHeight(tab, screenshotPath, {
      width: INDEPTHDNS_VIEWPORT_WIDTH,
      left: 0,
      right: 0,
      top: 0,
      bottom: 0,
    });

    console.log(
      `[indepthdns] Done for ${domain} — ` +
      `PASS:${data.counts.pass} WARN:${data.counts.warn} FAIL:${data.counts.fail} INFO:${data.counts.info}`
    );

    return {
      status: 'SUCCESS',
      data,
      error: null,
      errorCode: null,
      url: toolUrl,
      screenshot: 'indepthdns.png',
    };

  } catch (err) {
    console.error(`[indepthdns] Unexpected error for ${domain}: ${err.message}`);

    let screenshot = null;
    try {
      await captureViewportWidthFullHeight(tab, screenshotPath, {
        width: INDEPTHDNS_VIEWPORT_WIDTH,
      });
      screenshot = 'indepthdns.png';
    } catch (_) {}

    return {
      status: 'FAILED',
      data: {
        counts: { all: 0, pass: 0, warn: 0, fail: 0, info: 0 },
        timing: { phase1: '—', phase2: '—', total: '—' },
        overallHealth: 'UNKNOWN',
        checkRows: [],
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
      url: toolUrl,
      screenshot,
    };

  } finally {
    await tab.close().catch(() => {});
  }
}

module.exports = { runInDepthDNS };