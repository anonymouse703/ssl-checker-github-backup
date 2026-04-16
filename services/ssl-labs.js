'use strict';

const { httpGet } = require('../utils/http');
const { getErrorCode } = require('../utils/error-codes');
const { TIMEOUTS, GRADE_ORDER, ENDPOINTS, SSL_LABS } = require('../config/constants');
const path = require('path');
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
    } catch (_) {
      return null;
    }
  }

  // Polls until at least ONE endpoint has a real grade, then returns immediately.
  // Does NOT wait for all endpoints — the caller handles waiting for full completion.
  async function pollApiUntilFirstGrade(maxMs = 10 * 60 * 1000) {
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
  async function pollApiUntilAllDone(maxMs = 20 * 60 * 1000) {
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
          let grade = tds[1]?.innerText?.trim()?.split('\n')[0]?.trim() || '';
          const gradeSpan = tds[1]?.querySelector('.grade, .rating, .score');
          if (gradeSpan) grade = gradeSpan.innerText.trim();
          if (ip && isGrade(grade)) endpoints.push({ ipAddress: ip, grade });
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
    } catch (_) {
      return null;
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
      const screenshot = await screenshotViaFreshTab();
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
    const firstGradeData = await pollApiUntilFirstGrade(10 * 60 * 1000);

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
        const allDoneData = await pollApiUntilAllDone(20 * 60 * 1000);
        finalData = allDoneData || firstGradeData;
        if (DEBUG) {
          console.log(`[ssl-labs] all servers done for ${domain}: ${finalData.overallGrade}`);
        }
      }

      // Step 3: Take screenshot ONLY after all servers are done so the page shows complete results.
      const screenshot = await screenshotViaFreshTab();

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
    return {
      status: 'SUCCESS',
      data: browserData,
      error: null,
      errorCode: null,
      url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
      screenshot: browserData.screenshot || null,
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