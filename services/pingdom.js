'use strict';

const { getErrorCode } = require('../utils/error-codes');
const fs = require('fs');
const path = require('path');
const {
  setFixedViewport,
  captureViewportWidthFullHeight,
  wait,
} = require('../utils/screenshot');

const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

// 1280px keeps "URL + Test from + START TEST" all on one row without wrapping
const PINGDOM_VIEWPORT_WIDTH = 1064;
const PINGDOM_VIEWPORT_HEIGHT = 900;
const WANTED_LOCATION = 'North America - USA - San Francisco';

async function waitForFonts(page, timeout = 15000) {
  try {
    await page.evaluate(async (t) => {
      if (!document.fonts) return;
      await Promise.race([
        document.fonts.ready,
        new Promise(resolve => setTimeout(resolve, t))
      ]);
    }, timeout);
  } catch (_) {}
}

async function fillPingdomUrl(page, domain) {
  const targetUrl = `https://${domain}`;
  const urlInputSelector = 'input[placeholder="www.example.com"], input[type="text"]';

  await page.waitForSelector(urlInputSelector, { timeout: 20000 });
  await page.click(urlInputSelector, { clickCount: 3 }).catch(() => {});
  await page.keyboard.down('Control').catch(() => {});
  await page.keyboard.press('A').catch(() => {});
  await page.keyboard.up('Control').catch(() => {});
  await page.keyboard.press('Backspace').catch(() => {});
  await page.type(urlInputSelector, targetUrl, { delay: 40 });
}

async function setPingdomLocation(page) {
  const wanted = WANTED_LOCATION;

  const normalize = (s) =>
    String(s || '')
      .replace(/\s+/g, ' ')
      .replace(/[^\x20-\x7E]/g, '')
      .trim()
      .toLowerCase();

  const dropdownFound = await page.evaluate(() => {
    const labels = Array.from(document.querySelectorAll('label, span, div, p'));
    const testFromLabel = labels.find(el => /test\s*from/i.test(el.textContent || ''));
    if (!testFromLabel) return false;

    const container = testFromLabel.closest('div, form, fieldset') || testFromLabel.parentElement;
    const select = container?.querySelector('app-select');
    if (!select) return false;

    select.click();
    return true;
  });

  if (!dropdownFound) {
    throw new Error('Could not find "Test from" dropdown');
  }

  await page.waitForSelector('app-select .options', { visible: true, timeout: 5000 });
  await wait(500);

  const optionSelector = 'app-select .options .option';
  await page.waitForSelector(optionSelector, { visible: true, timeout: 5000 });

  const options = await page.$$eval(optionSelector, els =>
    els.map(el => (el.textContent || '').trim())
  );

  const wantedIndex = options.findIndex(opt => normalize(opt) === normalize(wanted));
  if (wantedIndex === -1) {
    console.error('[pingdom] Available location options:', options);
    throw new Error(`Option "${wanted}" not found in dropdown`);
  }

  const optionElements = await page.$$(optionSelector);
  await optionElements[wantedIndex].click({ delay: 100 });

  await page.waitForFunction(
    () => !document.querySelector('app-select .options'),
    { timeout: 5000 }
  ).catch(() => {});

  await page.waitForFunction(
    (wantedText) => {
      const normalizeInner = (s) =>
        String(s || '')
          .replace(/\s+/g, ' ')
          .replace(/[^\x20-\x7E]/g, '')
          .trim()
          .toLowerCase();

      const appSelects = Array.from(document.querySelectorAll('app-select'));
      return appSelects.some(el => normalizeInner(el.textContent || '') === normalizeInner(wantedText));
    },
    { timeout: 10000 },
    wanted
  );
}

async function clickPingdomStartTest(page) {
  const locationVerified = await page.evaluate((wantedText) => {
    const normalize = (s) => String(s || '').replace(/\s+/g, ' ').trim().toLowerCase();
    const wantedNorm = normalize(wantedText);

    const appSelects = Array.from(document.querySelectorAll('app-select'));
    return appSelects.some((el) => normalize(el.textContent || '') === wantedNorm);
  }, WANTED_LOCATION);

  if (!locationVerified) {
    throw new Error(`Pingdom visible location is not "${WANTED_LOCATION}" before clicking START TEST.`);
  }

  const clicked = await page.evaluate(() => {
    const elements = Array.from(
      document.querySelectorAll('input[value="START TEST"], button, input[type="submit"]')
    );
    const btn = elements.find((el) =>
      /start test/i.test((el.value || el.innerText || '').trim())
    );
    if (btn) {
      btn.click();
      return true;
    }
    return false;
  });

  if (!clicked) {
    throw new Error('Could not click START TEST.');
  }
}

async function waitForPingdomResults(page, domain) {
  console.log(`[pingdom] Waiting for test results for ${domain}...`);

  const maxWaitMs = 180000;
  const startTime = Date.now();

  while (Date.now() - startTime < maxWaitMs) {
    await wait(5000);

    const state = await page.evaluate(() => {
      const text = document.body.innerText || '';

      const hasGrade = /Performance\s+grade/i.test(text) || /\bGrade\b/i.test(text);
      const hasLoadTime = /Load\s+time/i.test(text);
      const hasPageSize = /Page\s+size/i.test(text);
      const hasRequests = /\bRequests\b/i.test(text);
      const hasDownloadHar = /DOWNLOAD HAR/i.test(text);

      return {
        hasResults: ((hasGrade && hasLoadTime) || (hasPageSize && hasRequests) || hasDownloadHar),
      };
    }).catch(() => ({ hasResults: false }));

    if (state.hasResults) {
      console.log(`[pingdom] Test completed for ${domain}`);
      return;
    }
  }

  throw new Error(`Pingdom test timeout for ${domain} after 3 minutes`);
}

function parsePingdomText(pageText) {
  let grade = 'N/A';
  let score = 'N/A';
  let pageSize = 'N/A';
  let loadTime = 'N/A';
  let requests = 'N/A';

  const gradeMatch = pageText.match(/Performance\s+grade\s*([A-F][+-]?)\s*(\d{1,3})?/i);
  if (gradeMatch) {
    grade = gradeMatch[1].toUpperCase();
    score = gradeMatch[2] || 'N/A';
  }

  if (grade === 'N/A') {
    const altGrade = pageText.match(/\b([A-F][+-]?)\s+(\d{1,3})\b/);
    if (altGrade) {
      grade = altGrade[1].toUpperCase();
      score = altGrade[2] || 'N/A';
    }
  }

  const loadMatch = pageText.match(/Load\s+time\s*([\d.]+)\s*(ms|s)/i);
  if (loadMatch) loadTime = `${loadMatch[1]} ${loadMatch[2]}`;

  const sizeMatch = pageText.match(/Page\s+size\s*([\d.]+)\s*(KB|MB)/i);
  if (sizeMatch) pageSize = `${sizeMatch[1]} ${sizeMatch[2].toUpperCase()}`;

  const reqMatch = pageText.match(/Requests\s*([\d]+)/i) || pageText.match(/Requests\s*\n?\s*(\d+)/i);
  if (reqMatch) requests = reqMatch[1];

  return {
    performanceGrade: grade !== 'N/A' ? `${grade} ${score}`.trim() : 'N/A',
    gradeLetter: grade,
    gradeNumber: score !== 'N/A' ? parseInt(score, 10) : null,
    loadTime,
    pageSize,
    requests,
  };
}

async function runPingdom(domain, context) {
  const { newTab, paths } = context;
  const debugDir = paths.domainDir;
  const screenshotPath = path.join(paths.imagesDir, 'pingdom.png');
  let tab = null;

  try {
    tab = await newTab();

    // Set viewport and UA BEFORE navigation so the page renders at full desktop width.
    // Fonts must NOT be blocked — Pingdom uses web fonts that affect element sizing and layout.
    await tab.setViewport({
      width: PINGDOM_VIEWPORT_WIDTH,
      height: PINGDOM_VIEWPORT_HEIGHT,
      deviceScaleFactor: 1,
    });
    await tab.setUserAgent(USER_AGENT);
    tab.setDefaultNavigationTimeout(300000);
    tab.setDefaultTimeout(300000);

    // Allow fonts through — override any request interception set by the pool/browser utils.
    // Without fonts, web font fallbacks change element widths and break the form row layout.
    const existingInterception = await tab.evaluate(() => true).then(() => false).catch(() => false);
    if (!existingInterception) {
      await tab.setRequestInterception(true).catch(() => {});
    }
    tab.on('request', (req) => {
      const url = req.url().toLowerCase();
      const type = req.resourceType();
      if (
        type === 'media' ||
        url.includes('google-analytics.com') ||
        url.includes('googletagmanager.com') ||
        url.includes('doubleclick.net') ||
        url.includes('hotjar.com') ||
        url.includes('clarity.ms')
      ) {
        req.abort().catch(() => {});
        return;
      }
      req.continue().catch(() => {});
    });

    // networkidle0 ensures fonts and all assets are fully loaded before we interact
    await tab.goto('https://tools.pingdom.com/', {
      waitUntil: 'networkidle0',
      timeout: 60000,
    });

    // Wait for fonts to render — this prevents layout shifts that cause the form row to wrap
    await waitForFonts(tab, 15000);
    await wait(2000);

    await fillPingdomUrl(tab, domain);
    await setPingdomLocation(tab);
    await wait(800);
    await clickPingdomStartTest(tab);
    await waitForPingdomResults(tab, domain);

    // Extra settle time for charts and animations to fully render before screenshot
    await waitForFonts(tab, 10000);
    await wait(3000);

    await setFixedViewport(tab, PINGDOM_VIEWPORT_WIDTH, PINGDOM_VIEWPORT_HEIGHT);

    await captureViewportWidthFullHeight(tab, screenshotPath, {
      width: PINGDOM_VIEWPORT_WIDTH,
      left: 0,
      right: 25,
      top: 0,
      bottom: 0,
    }).catch(() => {});

    const pageText = await tab.evaluate(() => document.body.innerText || '');
    const parsed = parsePingdomText(pageText);

    return {
      status: 'SUCCESS',
      data: parsed,
      error: null,
      errorCode: null,
      url: tab.url(),
      screenshot: 'pingdom.png',
    };
  } catch (err) {
    console.error(`[pingdom] Error for ${domain}:`, err.message);

    try {
      if (tab) {
        await captureViewportWidthFullHeight(tab, screenshotPath, {
          width: PINGDOM_VIEWPORT_WIDTH,
          left: 0,
          right: 0,
          top: 0,
          bottom: 0,
        }).catch(() => {});

        const html = await tab.content().catch(() => '');
        if (html) {
          fs.writeFileSync(path.join(debugDir, 'pingdom-error.html'), html);
        }
      }
    } catch (_) {}

    return {
      status: 'FAILED',
      data: {
        performanceGrade: 'N/A',
        gradeLetter: 'N/A',
        gradeNumber: null,
        loadTime: 'N/A',
        pageSize: 'N/A',
        requests: 'N/A',
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
      url: 'https://tools.pingdom.com/',
      screenshot: 'pingdom.png',
    };
  } finally {
    if (tab) await tab.close().catch(() => {});
  }
}

module.exports = { runPingdom };