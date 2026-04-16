const path = require('path');
const { getErrorCode } = require('../utils/error-codes');
const {
  setFixedViewport,
  captureViewportWidthFullHeight,
  wait,
} = require('../utils/screenshot');

const SUCURI_VIEWPORT_WIDTH = 1026; 
const SUCURI_VIEWPORT_HEIGHT = 900;

async function runSucuri(domain, context) {
  const { newTab, paths, DEBUG } = context;
  const screenshotPath = path.join(paths.imagesDir, 'sucuri.png');
  const start = Date.now();
  const tab = await newTab();

  const resultUrl = `https://sitecheck.sucuri.net/results/${domain}`;
  const homeUrl = 'https://sitecheck.sucuri.net/';

  const envInt = (name, fallback) => {
    const raw = process.env[name];
    const n = parseInt(raw || '', 10);
    return Number.isFinite(n) && n > 0 ? n : fallback;
  };

  const SUCURI_MAX_WAIT_MS = envInt('SUCURI_MAX_WAIT_MS', 20000);
  const SUCURI_POLL_MS = envInt('SUCURI_POLL_MS', 500);
  const SUCURI_SETTLE_MS = envInt('SUCURI_SETTLE_MS', 300);

  async function getPageState(page, expectedDomain) {
    return page.evaluate((domainArg) => {
      const text = (document.body?.innerText || '').replace(/\s+/g, ' ').trim();
      const lower = text.toLowerCase();
      const href = window.location.href;
      const domainLower = String(domainArg || '').toLowerCase();

      const resultSignals =
        /No Malware Found/i.test(text) ||
        /Malware Found/i.test(text) ||
        /Site is not Blacklisted/i.test(text) ||
        /Blacklist Status/i.test(text) ||
        /Website Malware & Security/i.test(text) ||
        /Website Blacklist Status/i.test(text) ||
        /IP address:/i.test(text) ||
        /CMS:/i.test(text) ||
        /Powered by:/i.test(text) ||
        /Running on:/i.test(text) ||
        /Redirects to:/i.test(text) ||
        /Blacklists checked/i.test(text);

      const strongResultSignals =
        /No Malware Found/i.test(text) ||
        /Site is not Blacklisted/i.test(text) ||
        /Blacklists checked/i.test(text) ||
        (/IP address:/i.test(text) && /CMS:/i.test(text));

      const domainVisible =
        lower.includes(domainLower) ||
        Array.from(document.querySelectorAll('h1, h2, h3, a, div, span, p')).some(el =>
          (el.innerText || '').toLowerCase().includes(domainLower)
        );

      const loadingSignals =
        /loading/i.test(lower) ||
        /please wait/i.test(lower) ||
        /scanning/i.test(lower) ||
        /checking/i.test(lower);

      const hasInputForm =
        !!document.querySelector('input[type="text"], input[placeholder*=".com"], form');

      const homeHero =
        /free website malware and security checker/i.test(text);

      if (!loadingSignals && strongResultSignals && (domainVisible || /\/results\//i.test(href))) {
        return 'ready';
      }

      if (!loadingSignals && resultSignals && /\/results\//i.test(href) && !homeHero) {
        return 'ready';
      }

      if (loadingSignals) return 'loading';
      if (hasInputForm && homeHero && !resultSignals) return 'input';

      return 'unknown';
    }, expectedDomain).catch(() => 'unknown');
  }

  async function waitForDirectResult(page, expectedDomain) {
    const deadline = Date.now() + SUCURI_MAX_WAIT_MS;

    while (Date.now() < deadline) {
      const state = await getPageState(page, expectedDomain);
      if (state === 'ready') return true;
      if (state === 'input') return false;
      await wait(SUCURI_POLL_MS);
    }

    return false;
  }

  async function submitFromHome(page, domainToScan) {
    await page.goto(homeUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

    const inputSelector = 'input[type="text"], input[placeholder*=".com"]';
    await page.waitForSelector(inputSelector, { timeout: 15000 });

    await page.click(inputSelector, { clickCount: 3 }).catch(() => {});
    await page.keyboard.down('Control').catch(() => {});
    await page.keyboard.press('A').catch(() => {});
    await page.keyboard.up('Control').catch(() => {});
    await page.keyboard.press('Backspace').catch(() => {});
    await page.type(inputSelector, domainToScan, { delay: 20 });

    const beforeUrl = page.url();

    const submitWorked = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button, input[type="submit"]'));
      const submitBtn =
        buttons.find(el => /submit|scan website/i.test((el.innerText || el.value || '').trim())) ||
        document.querySelector('button[type="submit"]') ||
        document.querySelector('input[type="submit"]');

      if (submitBtn) {
        submitBtn.click();
        return true;
      }

      const form = document.querySelector('form');
      if (form) {
        form.submit();
        return true;
      }

      return false;
    });

    if (!submitWorked) {
      await page.keyboard.press('Enter').catch(() => {});
    }

    try {
      await page.waitForFunction(
        oldUrl => window.location.href !== oldUrl || /\/results\//i.test(window.location.href),
        { timeout: 30000 },
        beforeUrl
      );
    } catch (_) {}

    const deadline = Date.now() + SUCURI_MAX_WAIT_MS;

    while (Date.now() < deadline) {
      const state = await getPageState(page, domainToScan);
      if (state === 'ready') return true;
      await wait(SUCURI_POLL_MS);
    }

    return false;
  }

  async function ensureResultPage(page, domainToScan) {
    await page.goto(resultUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

    if (await getPageState(page, domainToScan) === 'ready') {
      return { ready: true, fast: true };
    }

    const directReady = await waitForDirectResult(page, domainToScan);
    if (directReady) return { ready: true, fast: false };

    const submittedReady = await submitFromHome(page, domainToScan);
    if (submittedReady) return { ready: true, fast: false };

    try {
      await page.goto(resultUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

      if (await getPageState(page, domainToScan) === 'ready') {
        return { ready: true, fast: true };
      }

      const retryReady = await waitForDirectResult(page, domainToScan);
      return { ready: retryReady, fast: false };
    } catch (_) {
      return { ready: false, fast: false };
    }
  }

  async function extractData(page) {
    return page.evaluate(() => {
      const clean = (v) => String(v || '').replace(/\s+/g, ' ').trim();
      const rawText = document.body?.innerText || '';
      const pageText = clean(rawText);

      let overallStatus = 'UNKNOWN';
      let malwareStatus = 'Unknown';
      let blacklistStatus = 'Unknown';

      if (/No Malware Found/i.test(pageText)) malwareStatus = 'No Malware Found';
      else if (/Malware Found/i.test(pageText)) malwareStatus = 'Malware Found';

      if (/Site is not Blacklisted/i.test(pageText) || /Not Blacklisted/i.test(pageText)) {
        blacklistStatus = 'Not Blacklisted';
      } else if (/Blacklisted/i.test(pageText) && !/Site is not Blacklisted/i.test(pageText)) {
        blacklistStatus = 'Blacklisted';
      }

      if (malwareStatus === 'No Malware Found' && blacklistStatus === 'Not Blacklisted') {
        overallStatus = 'CLEAN';
      } else if (malwareStatus === 'Malware Found' || blacklistStatus === 'Blacklisted') {
        overallStatus = 'ISSUES_FOUND';
      }

      const getField = (label) => {
        const m = pageText.match(new RegExp(label + '\\s*:\\s*([^\\n\\r]+)', 'i'));
        return m ? clean(m[1]) : '';
      };

      return {
        overallStatus,
        malware: { status: malwareStatus },
        blacklist: { status: blacklistStatus },
        cms: getField('CMS'),
        ipAddress: getField('IP address'),
        hosting: getField('Hosting'),
        poweredBy: getField('Powered by'),
        runningOn: getField('Running on'),
        redirectsTo: getField('Redirects to')
      };
    });
  }

  try {
    await setFixedViewport(tab, SUCURI_VIEWPORT_WIDTH, SUCURI_VIEWPORT_HEIGHT);

    const result = await ensureResultPage(tab, domain);
    if (!result.ready) {
      throw new Error('Sucuri results did not fully load in time.');
    }

    await wait(result.fast ? 200 : SUCURI_SETTLE_MS);

    const data = await extractData(tab);

    const finalState = await getPageState(tab, domain);
    if (finalState !== 'ready') {
      throw new Error('Sucuri page was not on a valid result screen at capture time.');
    }

    await wait(200);

    await captureViewportWidthFullHeight(tab, screenshotPath, {
      width: SUCURI_VIEWPORT_WIDTH,
      left: 0,
      right: 0,
      top: 0,
      bottom: 0,
    });

    return {
      status: 'SUCCESS',
      data,
      error: null,
      errorCode: null,
      url: tab.url() || resultUrl,
      screenshot: 'sucuri.png',
      duration_ms: Date.now() - start
    };
  } catch (err) {
    let screenshot = null;

    if (DEBUG) {
      try {
        const fs = require('fs');
        await tab.screenshot({
          path: path.join(paths.domainDir, 'sucuri-debug-failed.png'),
          fullPage: true
        });
        fs.writeFileSync(
          path.join(paths.domainDir, 'sucuri-debug-failed.html'),
          await tab.content()
        );
      } catch (_) {}
    }

    try {
      await captureViewportWidthFullHeight(tab, screenshotPath, {
        width: SUCURI_VIEWPORT_WIDTH,
        left: 0,
        right: 0,
        top: 0,
        bottom: 0,
      });
      screenshot = 'sucuri.png';
    } catch (_) {}

    return {
      status: 'FAILED',
      data: {
        overallStatus: 'UNKNOWN',
        malware: { status: 'Unknown' },
        blacklist: { status: 'Unknown' },
        cms: '',
        ipAddress: '',
        hosting: '',
        poweredBy: '',
        runningOn: '',
        redirectsTo: ''
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
      url: tab.url() || resultUrl,
      screenshot,
      duration_ms: Date.now() - start
    };
  } finally {
    await tab.close().catch(() => {});
  }
}

module.exports = { runSucuri };