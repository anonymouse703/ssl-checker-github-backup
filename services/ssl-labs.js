'use strict';

const { httpGet } = require('../utils/http');
const { getErrorCode } = require('../utils/error-codes');

// Load constants defensively. Some older index/tool installs have config/constants.js
// with a different export shape, or a partially restored constants file. Without
// these fallbacks, SSL Labs fails before its own try/catch with:
// Cannot read properties of undefined (reading 'SSL_BROWSER').
let constants = {};
try {
  constants = require('../config/constants') || {};
} catch (_) {
  constants = {};
}

const toPositiveInt = (value, fallback) => {
  const n = parseInt(value, 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
};

const rawTimeouts = constants.TIMEOUTS || {};
const TIMEOUTS = {
  SSL_BROWSER: toPositiveInt(rawTimeouts.SSL_BROWSER ?? constants.SSL_BROWSER, 300000),
  SSL_API_POLL: toPositiveInt(rawTimeouts.SSL_API_POLL ?? constants.SSL_API_POLL, 15000),
};

const GRADE_ORDER = Array.isArray(constants.GRADE_ORDER) && constants.GRADE_ORDER.length
  ? constants.GRADE_ORDER
  : ['A+', 'A', 'A-', 'B', 'C', 'D', 'E', 'F', 'T', 'M'];

const rawEndpoints = constants.ENDPOINTS || {};
const ENDPOINTS = {
  SSL_LABS_API: rawEndpoints.SSL_LABS_API || constants.SSL_LABS_API || 'https://api.ssllabs.com/api/v3/analyze',
  SSL_LABS_WEB: rawEndpoints.SSL_LABS_WEB || constants.SSL_LABS_WEB || 'https://www.ssllabs.com/ssltest/analyze.html',
};

const rawSslLabs = constants.SSL_LABS || {};
const SSL_LABS = {
  MAX_RETRIES: toPositiveInt(rawSslLabs.MAX_RETRIES ?? constants.SSL_LABS_MAX_RETRIES, 5),
  BASE_RETRY_DELAY: toPositiveInt(rawSslLabs.BASE_RETRY_DELAY ?? constants.SSL_LABS_BASE_RETRY_DELAY, 10000),
  POLL_INTERVAL: toPositiveInt(rawSslLabs.POLL_INTERVAL ?? constants.SSL_LABS_POLL_INTERVAL, 5000),
};

const path = require('path');
const fs = require('fs');
const {
  setFixedViewport,
  captureViewportWidthFullHeight,
  wait,
} = require('../utils/screenshot');

const SSLLABS_VIEWPORT_WIDTH = 1095;
const SSLLABS_VIEWPORT_HEIGHT = 900;

// Real desktop Chrome UA — prevents sites from serving a narrow/mobile layout to headless browsers
const DESKTOP_USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

async function runSSLLabs(domain, context) {
  const { newTab, DEBUG, paths, sslDelay } = context;

  if (sslDelay > 0) {
    await wait(sslDelay);
  }

  const screenshotPath = path.join(paths.imagesDir, 'ssl.png');

  const envInt = (name, fallback) => {
    const raw = process.env[name];
    const n = parseInt(raw || '', 10);
    return Number.isFinite(n) && n > 0 ? n : fallback;
  };

  const SSL_BROWSER_MAX_WAIT_MS = envInt('SSL_LABS_BROWSER_MAX_WAIT_MS', TIMEOUTS.SSL_BROWSER);
  const SSL_SCREENSHOT_POLL_MS = envInt('SSL_LABS_SCREENSHOT_POLL_MS', 3000);
  const SSL_FINAL_SETTLE_MS = envInt('SSL_LABS_FINAL_SETTLE_MS', 2000);
  const SSL_SCREENSHOT_MIN_BYTES = envInt('SSL_LABS_SCREENSHOT_MIN_BYTES', 1000);

  const screenshotFileIsUsable = () => {
    try {
      return fs.existsSync(screenshotPath) && fs.statSync(screenshotPath).size >= SSL_SCREENSHOT_MIN_BYTES;
    } catch (_) {
      return false;
    }
  };

  const escapeHtml = (value) => String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');

  async function ensureScreenshotFile(sslParsedData, sourceLabel = 'SSL Labs result') {
    if (screenshotFileIsUsable()) return 'ssl.png';

    // Non-fatal visual fallback: API values can succeed while the SSL Labs web
    // page screenshot fails or never writes a valid PNG. FileMaker expects a
    // URL for ssl.png, so create a real PNG summary instead of returning a
    // missing file. This keeps SSL values and screenshot URL consistent.
    const tab = await newTab();
    try {
      const summary = sslParsedData?.summary || {};
      const endpoints = Array.isArray(sslParsedData?.endpoints) ? sslParsedData.endpoints : [];
      const rows = endpoints.length
        ? endpoints.map((ep) => `
          <tr>
            <td>${escapeHtml(ep.ipAddress || '')}</td>
            <td>${escapeHtml(ep.grade || 'N/A')}</td>
            <td>${escapeHtml(ep.statusMessage || '')}</td>
          </tr>`).join('')
        : '<tr><td colspan="3">No endpoint rows returned by SSL Labs.</td></tr>';

      await tab.setViewport({ width: SSLLABS_VIEWPORT_WIDTH, height: SSLLABS_VIEWPORT_HEIGHT, deviceScaleFactor: 1 }).catch(() => {});
      await tab.setContent(`<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>SSL Labs Summary - ${escapeHtml(domain)}</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 34px; color: #1f2937; background: #f8fafc; }
    .card { background:#fff; border:1px solid #d9e2ec; border-radius:14px; padding:28px; box-shadow:0 8px 28px rgba(15,23,42,.08); }
    .brand { font-size:28px; font-weight:700; color:#173f5f; margin-bottom:8px; }
    .domain { font-size:22px; margin-bottom:24px; color:#334155; }
    .grade { display:inline-block; font-size:54px; font-weight:800; color:#0f766e; border:3px solid #0f766e; border-radius:16px; padding:10px 24px; margin:12px 0 22px; }
    .meta { font-size:16px; line-height:1.8; margin-bottom:22px; }
    table { border-collapse:collapse; width:100%; font-size:15px; }
    th,td { border:1px solid #cbd5e1; padding:10px 12px; text-align:left; }
    th { background:#e2e8f0; }
    .note { margin-top:22px; font-size:13px; color:#64748b; }
  </style>
</head>
<body>
  <div class="card">
    <div class="brand">SSL Labs Result</div>
    <div class="domain">${escapeHtml(domain)}</div>
    <div>Overall Grade</div>
    <div class="grade">${escapeHtml(sslParsedData?.overallGrade || 'N/A')}</div>
    <div class="meta">
      Status: <b>${escapeHtml(sslParsedData?.status || 'READY')}</b><br>
      Endpoints: <b>${escapeHtml(summary.totalEndpoints ?? endpoints.length ?? 0)}</b><br>
      All Grades: <b>${escapeHtml(summary.allGrades || 'N/A')}</b><br>
      Source: ${escapeHtml(sourceLabel)}
    </div>
    <table>
      <thead><tr><th>IP Address</th><th>Grade</th><th>Status</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="note">Generated automatically because the SSL Labs web-page screenshot did not produce a usable ssl.png file.</div>
  </div>
</body>
</html>`, { waitUntil: 'load' });
      await wait(500);
      await tab.screenshot({ path: screenshotPath, fullPage: true });
      return screenshotFileIsUsable() ? 'ssl.png' : null;
    } catch (err) {
      console.error(`[ssl-labs] ensureScreenshotFile failed for ${domain}: ${err.message}`);
      return null;
    } finally {
      await tab.close().catch(() => {});
    }
  }

  const isRealGrade = (grade) => /^(A\+|A-|A|B|C|D|E|F|T|M)$/.test((grade || '').trim());

  const isCapacityError = (d) => {
    if (!d || typeof d !== 'object') return false;
    if (Array.isArray(d.errors) && d.errors.some(e =>
      typeof e.message === 'string' && (
        e.message.toLowerCase().includes('capacity') ||
        e.message.toLowerCase().includes('try again') ||
        e.message.toLowerCase().includes('overloaded') ||
        e.message.toLowerCase().includes('too many')
      )
    )) return true;
    if (d.status === 'ERROR' && typeof d.statusMessage === 'string' && (
      d.statusMessage.toLowerCase().includes('capacity') ||
      d.statusMessage.toLowerCase().includes('try again')
    )) return true;
    if (typeof d.message === 'string' && (
      d.message.toLowerCase().includes('capacity') ||
      d.message.toLowerCase().includes('try again')
    )) return true;
    return false;
  };

  const sslFetch = async (url, label, maxRetries = SSL_LABS.MAX_RETRIES) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      let data;
      try {
        data = await httpGet(url);
      } catch (_) {
        if (attempt < maxRetries) {
          await wait(10000);
          continue;
        }
        return null;
      }

      if (DEBUG) {
        const fs = require('fs');
        fs.writeFileSync(
          path.join(paths.domainDir, `ssllabs-${label}-attempt${attempt}.json`),
          JSON.stringify(data, null, 2)
        );
      }

      if (!isCapacityError(data)) return data;

      const waitMs = Math.min(SSL_LABS.BASE_RETRY_DELAY * Math.pow(1.5, attempt - 1), 60000);
      if (attempt < maxRetries) await wait(waitMs);
    }

    return null;
  };

  const parseReady = (sslData) => {
    const endpoints = (sslData.endpoints || []).map(ep => ({
      ipAddress: ep.ipAddress,
      grade: ep.grade,
      hasWarnings: ep.hasWarnings,
      isExceptional: ep.isExceptional,
      statusMessage: ep.statusMessage,
    }));

    const gradedOnly = endpoints.filter(ep => isRealGrade(ep.grade));
    const overallGrade = gradedOnly.length > 0
      ? gradedOnly.reduce((worst, ep) =>
          GRADE_ORDER.indexOf(ep.grade) > GRADE_ORDER.indexOf(worst) ? ep.grade : worst,
          gradedOnly[0].grade
        )
      : 'N/A';

    return {
      overallGrade,
      host: sslData.host,
      status: sslData.status,
      endpoints,
      summary: {
        totalEndpoints: endpoints.length,
        gradedEndpoints: gradedOnly.length,
        allGrades: [...new Set(gradedOnly.map(e => e.grade))].join(', ') || 'N/A',
        anyWarnings: endpoints.some(e => e.hasWarnings),
        anyExceptional: endpoints.some(e => e.isExceptional),
      },
    };
  };

  const parsePartial = (sslData) => {
    const endpoints = (sslData.endpoints || []).map(ep => ({
      ipAddress: ep.ipAddress,
      grade: ep.grade || 'PENDING',
      hasWarnings: ep.hasWarnings,
      isExceptional: ep.isExceptional,
      statusMessage: ep.statusMessage,
    }));

    const gradedOnly = endpoints.filter(ep => isRealGrade(ep.grade));
    const overallGrade = gradedOnly.length > 0
      ? gradedOnly.reduce((worst, ep) =>
          GRADE_ORDER.indexOf(ep.grade) > GRADE_ORDER.indexOf(worst) ? ep.grade : worst,
          gradedOnly[0].grade
        )
      : 'N/A';

    return {
      overallGrade,
      host: sslData.host,
      status: sslData.status,
      earlyGrade: true,
      endpoints,
      summary: {
        totalEndpoints: endpoints.length,
        gradedEndpoints: gradedOnly.length,
        allGrades: [...new Set(gradedOnly.map(e => e.grade))].join(', ') || 'PENDING',
        anyWarnings: endpoints.some(e => e.hasWarnings),
        anyExceptional: endpoints.some(e => e.isExceptional),
      },
    };
  };

  async function shootTab(tab) {
    try {
      await setFixedViewport(tab, SSLLABS_VIEWPORT_WIDTH, SSLLABS_VIEWPORT_HEIGHT);
      await wait(500);

      await captureViewportWidthFullHeight(tab, screenshotPath, {
        width: SSLLABS_VIEWPORT_WIDTH,
        left: 0,
        right: 0,
        top: 0,
        bottom: 0,
      });

      return 'ssl.png';
    } catch (err) {
      console.error(`[ssl-labs] primary screenshot failed for ${domain}: ${err.message}`);
      try {
        await tab.screenshot({ path: screenshotPath, fullPage: true });
        return 'ssl.png';
      } catch (shotErr) {
        console.error(`[ssl-labs] fallback screenshot also failed for ${domain}: ${shotErr.message}`);
        return null;
      }
    }
  }

  // Polls until at least ONE endpoint has a real grade, then returns immediately.
  // Does NOT wait for all endpoints — the caller handles waiting for full completion.
  async function pollApiUntilFirstGrade(maxMs = envInt('SSL_LABS_FIRST_GRADE_MAX_WAIT_MS', 12 * 60 * 1000)) {
    let sslData = await sslFetch(
      `${ENDPOINTS.SSL_LABS_API}?host=${domain}&startNew=on&all=done`,
      'startnew',
      3
    );

    if (!sslData || sslData.status === null) return null;

    const deadline = Date.now() + maxMs;
    let apiAttempts = 0;

    const hasAnyGrade = (d) => (d.endpoints || []).some(ep => isRealGrade(ep.grade));

    // Already done or already has a grade on first response
    if (sslData.status === 'READY') return parseReady(sslData);
    if (sslData.status === 'IN_PROGRESS' && hasAnyGrade(sslData)) return parsePartial(sslData);

    while (
      sslData &&
      sslData.status !== 'READY' &&
      sslData.status !== 'ERROR' &&
      Date.now() < deadline
    ) {
      apiAttempts++;
      await wait(TIMEOUTS.SSL_API_POLL);

      sslData = await sslFetch(
        `${ENDPOINTS.SSL_LABS_API}?host=${domain}&all=done`,
        `poll${apiAttempts}`,
        2
      );

      if (!sslData) break;
      if (sslData.status === 'READY') return parseReady(sslData);

      // Return as soon as the FIRST endpoint has a grade — don't wait for the rest
      if (sslData.status === 'IN_PROGRESS' && hasAnyGrade(sslData)) return parsePartial(sslData);
    }

    if (sslData && sslData.status === 'READY') return parseReady(sslData);
    return null;
  }

  // Polls until status === READY (all endpoints fully graded).
  // No artificial time cap — uses a generous hard ceiling of 20 min to avoid
  // hanging forever on a broken scan. Screenshot is taken AFTER this returns.
  async function pollApiUntilAllDone(maxMs = envInt('SSL_LABS_ALL_DONE_MAX_WAIT_MS', 25 * 60 * 1000)) {
    const deadline = Date.now() + maxMs;
    let attempts = 0;

    while (Date.now() < deadline) {
      attempts++;
      const sslData = await sslFetch(
        `${ENDPOINTS.SSL_LABS_API}?host=${domain}&fromCache=on&maxAge=1&all=done`,
        `alldone-${attempts}`,
        2
      );

      if (sslData && sslData.status === 'READY') return parseReady(sslData);
      if (sslData && sslData.status === 'ERROR') return null;

      await wait(TIMEOUTS.SSL_API_POLL);
    }

    return null;
  }

  // ========== IMPROVED PAGE STATE DETECTION ==========
  const SSL_PAGE_STATE_FN = () => {
    const bodyText = document.body ? document.body.innerText : '';
    const lowerBody = bodyText.toLowerCase();

    // --- #warningBox is the most reliable signal SSL Labs provides ---
    // When it exists and is visible, the scan is still running.
    // When it's gone or hidden, the page has moved past the loading phase.
    const warningBox = document.querySelector('#warningBox');
    if (warningBox) {
      const style = window.getComputedStyle(warningBox);
      const isVisible =
        style.display !== 'none' &&
        style.visibility !== 'hidden' &&
        style.opacity !== '0' &&
        warningBox.offsetParent !== null;
      if (isVisible) return 'pending';
    }

    // Error states — check before any further "pending" logic
    const errBox = document.querySelector('#errorbox');
    if (errBox) {
      const t = errBox.innerText.toLowerCase();
      if (t.includes('no secure protocols')) return 'nossl';
      if (t.includes('dns resolution failed')) return 'dns_fail';
      if (t.includes('assessment failed')) return 'failed';
      return 'error';
    }

    if (lowerBody.includes('assessment failed')) return 'failed';
    if (lowerBody.includes('unable to connect')) return 'failed';
    if (lowerBody.includes('no secure protocols')) return 'nossl';
    if (lowerBody.includes('dns resolution failed')) return 'dns_fail';

    // Secondary text-based scanning indicators (fallback if #warningBox isn't used)
    const isScanning =
      /\d+%\s*complete/i.test(bodyText) ||
      /assessment in progress/i.test(bodyText) ||
      /calculating/i.test(bodyText) ||
      /testing tls/i.test(bodyText) ||
      /testing protocol/i.test(bodyText) ||
      /determining available/i.test(bodyText) ||
      /queued/i.test(bodyText);

    if (isScanning) return 'pending';

    // Check every endpoint row for unfinished assessments
    const endpointRows = document.querySelectorAll('#endpointData tr');
    for (const row of endpointRows) {
      const rowText = row.innerText.toLowerCase();
      if (
        rowText.includes('assessment in progress') ||
        rowText.includes('testing') ||
        rowText.includes('please wait') ||
        rowText.includes('queued') ||
        rowText.includes('in progress')
      ) {
        return 'pending';
      }
    }

    // Final readiness checks — removed hasMisc gate because "Miscellaneous"
    // does not always appear for fresh/uncached domains and causes an infinite loop.
    const hasOverallRating = /Overall Rating/i.test(bodyText);
    const hasEndpointRows = endpointRows.length > 0;
    if (hasOverallRating && hasEndpointRows) return 'ready';

    const hasChartBars = document.querySelectorAll('.chartBar_g').length >= 3;
    if (hasChartBars && hasOverallRating) return 'ready';

    // If warningBox is gone and we have a grade anywhere on the page, accept it
    if (!warningBox && /\b(A\+|A-|A|B|C|D|E|F|T|M)\b/.test(bodyText)) return 'ready';

    return 'pending';
  };
  // ===================================================

  async function runSSLLabsBrowser() {
    const tab = await newTab();
    try {
      await tab.setViewport({ width: SSLLABS_VIEWPORT_WIDTH, height: SSLLABS_VIEWPORT_HEIGHT, deviceScaleFactor: 1 }).catch(() => {});
      await tab.setUserAgent(DESKTOP_USER_AGENT).catch(() => {});
      await setFixedViewport(tab, SSLLABS_VIEWPORT_WIDTH, SSLLABS_VIEWPORT_HEIGHT);

      const url = `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}&hideResults=on&latest`;
      await tab.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

      const deadline = Date.now() + SSL_BROWSER_MAX_WAIT_MS;
      let pageState = 'pending';
      let elapsedSec = 0;

      while (Date.now() < deadline) {
        await wait(SSL_LABS.POLL_INTERVAL);
        elapsedSec += SSL_LABS.POLL_INTERVAL / 1000;

        pageState = await tab.evaluate(SSL_PAGE_STATE_FN).catch(() => 'pending');
        if (pageState !== 'pending') break;

        if (DEBUG && elapsedSec % 60 === 0) {
          try {
            await tab.screenshot({
              path: path.join(paths.domainDir, `ssl-debug-${elapsedSec}s.png`),
            });
          } catch (_) {}
        }
      }

      if (pageState === 'pending') {
        try {
          const fs = require('fs');
          await tab.screenshot({ path: path.join(paths.domainDir, 'ssllabs-timeout.png') });
          fs.writeFileSync(path.join(paths.domainDir, 'ssllabs-timeout.html'), await tab.content());
        } catch (_) {}
        throw new Error(`Browser scan timed out after ${Math.round(SSL_BROWSER_MAX_WAIT_MS / 60000)} min`);
      }

      if (pageState === 'nossl') {
        const screenshot = await shootTab(tab);
        return {
          overallGrade: 'F',
          source: 'browser',
          screenshot,
          endpoints: [],
          summary: {
            totalEndpoints: 0,
            gradedEndpoints: 0,
            allGrades: 'F',
            anyWarnings: true,
            anyExceptional: false,
          },
        };
      }

      if (pageState === 'dns_fail') {
        throw new Error('DNS resolution failed — domain may not exist');
      }

      await wait(SSL_FINAL_SETTLE_MS);

      const scraped = await tab.evaluate(() => {
        const isGrade = t => /^(A\+|A-|A|B|C|D|E|F|T|M)$/.test((t || '').trim());
        const endpoints = [];

        for (const row of document.querySelectorAll('#endpointData tr')) {
          const tds = Array.from(row.querySelectorAll('td'));
          if (tds.length < 2) continue;
          const ip = tds[0]?.innerText?.trim() || '';

          // SSL Labs table is usually: Server | Test time | Grade.
          // Older code read tds[1], which is often the test-time column.
          let grade = '';
          const gradeSpan = row.querySelector('.grade, .rating, .score, .letterGrade');
          if (gradeSpan && isGrade(gradeSpan.innerText.trim())) {
            grade = gradeSpan.innerText.trim();
          }

          if (!isGrade(grade)) {
            for (let i = tds.length - 1; i >= 1; i--) {
              const cellText = (tds[i]?.innerText || '').replace(/\s+/g, ' ').trim();
              const m = cellText.match(/\b(A\+|A-|A|B|C|D|E|F|T|M)\b/);
              if (m) {
                grade = m[1];
                break;
              }
            }
          }

          if (ip && isGrade(grade)) endpoints.push({ ipAddress: ip, grade });
        }

        // Final text fallback. This catches pages where SSL Labs renders grade cells
        // without the older endpointData structure.
        if (endpoints.length === 0) {
          const body = document.body ? document.body.innerText : '';
          const tableLines = body.split('\n').map(x => x.trim()).filter(Boolean);
          let syntheticIndex = 1;
          for (const line of tableLines) {
            const gradeMatch = line.match(/\b(A\+|A-|A|B|C|D|E|F|T|M)\b/);
            const ipMatch = line.match(/\b(?:\d{1,3}\.){3}\d{1,3}\b|\b[0-9a-f:]{8,}\b/i);
            if (gradeMatch && ipMatch) {
              endpoints.push({ ipAddress: ipMatch[0], grade: gradeMatch[1] });
            }
          }
          if (endpoints.length === 0) {
            const gradeMatches = Array.from(body.matchAll(/\b(A\+|A-|A|B|C|D|E|F|T|M)\b/g)).map(m => m[1]);
            if (gradeMatches.length > 0) {
              endpoints.push({ ipAddress: `endpoint-${syntheticIndex++}`, grade: gradeMatches[gradeMatches.length - 1] });
            }
          }
        }

        let ratingGrade = null;
        const rb = document.querySelector('#rating');
        if (rb) {
          const m = rb.innerText.match(/(A\+|A-|A|B|C|D|E|F|T|M)/);
          if (m) ratingGrade = m[1];
        }

        let pageGrade = null;
        if (!ratingGrade && endpoints.length === 0) {
          const body = document.body ? document.body.innerText : '';
          for (const pat of [
            /Overall Rating\s*[:\-]?\s*(A\+|A-|A|B|C|D|E|F|T|M)/i,
            /Grade\s*[:\-]?\s*(A\+|A-|A|B|C|D|E|F|T|M)/i,
            /Rating\s*[:\-]?\s*(A\+|A-|A|B|C|D|E|F|T|M)/i,
          ]) {
            const m = body.match(pat);
            if (m) {
              pageGrade = m[1];
              break;
            }
          }
        }

        return { endpoints, ratingGrade, pageGrade };
      });

      let overallGrade = 'N/A';
      if (scraped.endpoints.length > 0) {
        overallGrade = scraped.endpoints.reduce(
          (worst, ep) => GRADE_ORDER.indexOf(ep.grade) > GRADE_ORDER.indexOf(worst) ? ep.grade : worst,
          scraped.endpoints[0].grade
        );
      } else if (scraped.ratingGrade) {
        overallGrade = scraped.ratingGrade;
      } else if (scraped.pageGrade) {
        overallGrade = scraped.pageGrade;
      }

      const screenshot = await shootTab(tab);

      return {
        overallGrade,
        source: 'browser',
        screenshot,
        endpoints: scraped.endpoints,
        summary: {
          totalEndpoints: scraped.endpoints.length,
          gradedEndpoints: scraped.endpoints.length,
          allGrades: [...new Set(scraped.endpoints.map(e => e.grade))].join(', ') || overallGrade,
          anyWarnings: false,
          anyExceptional: false,
        },
      };
    } catch (err) {
      throw new Error(`Browser scrape failed: ${err.message}`);
    } finally {
      await tab.close().catch(() => {});
    }
  }

  async function screenshotViaFreshTab() {
    const tab = await newTab();
    try {
      await tab.setViewport({ width: SSLLABS_VIEWPORT_WIDTH, height: SSLLABS_VIEWPORT_HEIGHT, deviceScaleFactor: 1 }).catch(() => {});
      await tab.setUserAgent(DESKTOP_USER_AGENT).catch(() => {});
      await setFixedViewport(tab, SSLLABS_VIEWPORT_WIDTH, SSLLABS_VIEWPORT_HEIGHT);

      await tab.goto(
        `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}&fromCache=on&maxAge=1`,
        { waitUntil: 'domcontentloaded', timeout: 60000 }
      );

      let finalState = 'pending';
      let stallCount = 0;
      const STALL_LIMIT = 10; // after 10 consecutive 'pending' polls (~30s), take screenshot anyway

      // No time limit — poll until the page confirms results are shown.
      // API already said READY, so this is just waiting for the browser to render it.
      // Stall detector breaks out if the page is stuck so we always get a screenshot.
      while (true) {
        await wait(SSL_SCREENSHOT_POLL_MS);
        finalState = await tab.evaluate(SSL_PAGE_STATE_FN).catch(() => 'pending');

        if (finalState === 'ready') break;
        if (['nossl', 'dns_fail', 'failed', 'error'].includes(finalState)) break;

        stallCount++;
        if (stallCount >= STALL_LIMIT) {
          if (DEBUG) {
            console.log(`[ssl-labs] screenshot stall limit reached for ${domain}, taking screenshot anyway`);
          }
          break;
        }

        try {
          await tab.reload({ waitUntil: 'domcontentloaded', timeout: 30000 });
        } catch (_) {}
      }

      await wait(SSL_FINAL_SETTLE_MS);

      await captureViewportWidthFullHeight(tab, screenshotPath, {
        width: SSLLABS_VIEWPORT_WIDTH,
        left: 0,
        right: 0,
        top: 0,
        bottom: 0,
      });

      return 'ssl.png';
    } catch (err) {
      console.error(`[ssl-labs] screenshotViaFreshTab failed for ${domain}: ${err.message}`);
      try {
        await tab.screenshot({ path: screenshotPath, fullPage: true });
        return 'ssl.png';
      } catch (shotErr) {
        console.error(`[ssl-labs] screenshotViaFreshTab fallback also failed for ${domain}: ${shotErr.message}`);
        return null;
      }
    } finally {
      await tab.close().catch(() => {});
    }
  }

  try {
    const cached = await sslFetch(
      `${ENDPOINTS.SSL_LABS_API}?host=${domain}&fromCache=on&maxAge=24&all=done`,
      'cache',
      3
    );

    if (cached && cached.status === 'READY') {
      const data = parseReady(cached);
      await screenshotViaFreshTab();
      const screenshot = await ensureScreenshotFile(data, 'SSL Labs cached result');
      return {
        status: 'SUCCESS',
        data,
        error: null,
        errorCode: null,
        url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
        screenshot,
      };
    }

    // Step 1: Wait until at least the FIRST server has a grade.
    // Returns immediately when any one endpoint is graded — does not wait for all.
    const firstGradeData = await pollApiUntilFirstGrade();

    if (firstGradeData) {
      if (DEBUG) {
        console.log(`[ssl-labs] first grade for ${domain}: ${firstGradeData.overallGrade}${firstGradeData.earlyGrade ? ' (partial — more servers pending)' : ' (all done)'}`);
      }

      // Step 2: If already fully done (single IP or all finished at once), go straight to screenshot.
      // Otherwise keep polling until ALL servers are graded — no artificial time cap.
      let finalData = firstGradeData;
      if (firstGradeData.earlyGrade) {
        if (DEBUG) {
          console.log(`[ssl-labs] waiting for all servers to finish for ${domain}...`);
        }
        const allDoneData = await pollApiUntilAllDone();
        finalData = allDoneData || firstGradeData;
        if (DEBUG) {
          console.log(`[ssl-labs] all servers done for ${domain}: ${finalData.overallGrade}`);
        }
      }

      // Step 3: Take screenshot ONLY after all servers are done so the page shows complete results.
      await screenshotViaFreshTab();
      const screenshot = await ensureScreenshotFile(finalData, 'SSL Labs final result');

      return {
        status: 'SUCCESS',
        data: finalData,
        error: null,
        errorCode: null,
        url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
        screenshot,
      };
    }

    const browserData = await runSSLLabsBrowser();
    const browserScreenshot = await ensureScreenshotFile(browserData, 'SSL Labs browser scrape result');
    return {
      status: 'SUCCESS',
      data: browserData,
      error: null,
      errorCode: null,
      url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
      screenshot: browserScreenshot || browserData.screenshot || null,
    };
  } catch (err) {
    const msg = err instanceof AggregateError
      ? err.errors.map(e => e.message).join(' | ')
      : err.message;

    return {
      status: 'FAILED',
      data: { overallGrade: 'N/A', error: msg },
      error: msg,
      errorCode: getErrorCode({ error: msg }),
      url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
      screenshot: null,
    };
  }
}

module.exports = { runSSLLabs };