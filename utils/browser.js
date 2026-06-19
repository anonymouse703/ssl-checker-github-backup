/**
 * utils/browser.js
 * Puppeteer browser management utilities
 * Fixed to prevent duplicate request interception errors
 * Patched to actually enable puppeteer-extra stealth when installed.
 */

'use strict';

let puppeteer;
try {
  puppeteer = require('puppeteer-extra');
  const StealthPlugin = require('puppeteer-extra-plugin-stealth');
  puppeteer.use(StealthPlugin());
  console.log('[browser] puppeteer-extra stealth enabled');
} catch (err) {
  // Fallback keeps older VMs running if stealth package is not installed yet.
  puppeteer = require('puppeteer');
  console.warn(`[browser] puppeteer-extra stealth not available, using normal puppeteer: ${err.message}`);
}

const { resolveChromePath } = require('./chrome-path');

function parseHeadlessMode() {
  const value = String(process.env.PUPPETEER_HEADLESS ?? 'true').trim().toLowerCase();
  if (['0', 'false', 'no', 'off', 'headful'].includes(value)) return false;
  return true;
}

const PUPPETEER_HEADLESS = parseHeadlessMode();

let tmpCleanup = null;
try {
  tmpCleanup = require('./tmp-cleanup');
} catch (err) {
  console.warn(`[browser] tmp cleanup helper not available: ${err.message}`);
}

const USER_AGENT =
  process.env.BROWSER_USER_AGENT ||
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36';

const COMMON_HEADERS = {
  'Accept-Language': 'en-US,en;q=0.9',
  'Upgrade-Insecure-Requests': '1',
  'DNT': '1',
};

const LAUNCH_ARGS = [
  '--no-sandbox',
  '--disable-setuid-sandbox',
  '--disable-dev-shm-usage',
  '--disable-gpu',
  '--window-size=1280,900',
  '--disable-extensions',
  '--disable-background-networking',
  '--disable-sync',
  '--disable-translate',
  '--disable-default-apps',
  '--mute-audio',
  '--no-first-run',
  '--disable-backgrounding-occluded-windows',
  '--disable-renderer-backgrounding',
  '--js-flags=--max-old-space-size=128',
  '--disable-features=TranslateUI,BlinkGenPropertyTrees',
  '--disable-blink-features=AutomationControlled',
  '--lang=en-US,en',
  // '--single-process',
];

async function launchBrowser() {
  const executablePath = resolveChromePath();

  if (!executablePath) {
    throw new Error(
      'Chrome executable not found. Set PUPPETEER_EXECUTABLE_PATH in .env or install Chrome.'
    );
  }

  const userDataDir = tmpCleanup && typeof tmpCleanup.createManagedUserDataDir === 'function'
    ? tmpCleanup.createManagedUserDataDir()
    : undefined;

  console.log(`[browser] launching Chrome from: ${executablePath}`);

  const browser = await puppeteer.launch({
    headless: PUPPETEER_HEADLESS,
    executablePath,
    userDataDir,
    args: LAUNCH_ARGS,
    protocolTimeout: 600000,
    timeout: 600000,
    ignoreHTTPSErrors: true,
  });

  return patchBrowserCloseForTmpCleanup(browser, userDataDir);
}


async function dismissCommonOverlays(page) {
  if (!page || (typeof page.isClosed === 'function' && page.isClosed())) return { clicked: 0 };

  const clickRound = async () => {
    return await page.evaluate(() => {
      // IMPORTANT:
      // Do not click normal footer/legal links. The old /agree/i rule matched
      // "Software Services Agreement" and navigated Pingdom screenshots to the
      // SolarWinds legal agreement page.
      // Never run generic overlay auto-clicking on Pingdom/SolarWinds pages.
      // Pingdom screenshots are handled by services/pingdom.js after metrics are parsed.
      if (/((^|\.)pingdom\.com$)|((^|\.)solarwinds\.com$)/i.test(location.hostname || '')) {
        return 0;
      }

      const textPatterns = [
        /^accept$/i,
        /^accept all$/i,
        /^allow all$/i,
        /\bi agree\b/i,
        /^agree$/i,
        /^ok(?:ay)?$/i,
        /^got it$/i,
        /^continue$/i,
        /^enter$/i,
        /^yes$/i,
        /\bi am over\s*(18|21)\b/i,
        /\bover\s*(18|21)\b/i,
        /^confirm$/i,
        /^not now$/i,
        /^no thanks$/i,
        /^close$/i,
        /^skip$/i,
      ];

      const selectorCandidates = [
        'button',
        '[role="button"]',
        'input[type="button"]',
        'input[type="submit"]',
        'a[role="button"]',
        'a.close',
        'a.modal-close',
        'a.popup-close',
        'a.cookie-close',
        '[class*="close" i]',
        '[class*="accept" i]',
        '[class*="consent" i]',
        '[aria-label*="close" i]',
        '[aria-label*="accept" i]',
        '[aria-label*="continue" i]',
      ];

      const isVisible = (el) => {
        const style = window.getComputedStyle(el);
        const rect = el.getBoundingClientRect();
        return style && style.visibility !== 'hidden' && style.display !== 'none' && rect.width > 0 && rect.height > 0;
      };

      const labelOf = (el) => {
        return [
          el.innerText,
          el.textContent,
          el.value,
          el.getAttribute && el.getAttribute('aria-label'),
          el.getAttribute && el.getAttribute('title'),
          el.getAttribute && el.getAttribute('data-testid'),
        ].filter(Boolean).join(' ').replace(/\s+/g, ' ').trim();
      };

      const isUnsafeLegalLink = (el, label) => {
        const tag = String(el.tagName || '').toUpperCase();
        const href = String(el.getAttribute && el.getAttribute('href') || '');
        const combined = `${label} ${href}`.toLowerCase();
        return tag === 'A' && /(agreement|terms|privacy|legal|license|policy|conditions|service-agreement|software-services-agreement)/i.test(combined);
      };

      const isButtonLikeAnchor = (el) => {
        const tag = String(el.tagName || '').toUpperCase();
        if (tag !== 'A') return true;
        const role = String(el.getAttribute && el.getAttribute('role') || '').toLowerCase();
        const cls = String(el.className || '').toLowerCase();
        const aria = String(el.getAttribute && el.getAttribute('aria-label') || '').toLowerCase();
        return role === 'button' || /(button|btn|close|modal|popup|cookie|consent|accept|continue)/i.test(`${cls} ${aria}`);
      };

      let clicked = 0;
      const seen = new Set();

      for (const selector of selectorCandidates) {
        const nodes = Array.from(document.querySelectorAll(selector)).slice(0, 200);
        for (const el of nodes) {
          if (!el || seen.has(el)) continue;
          seen.add(el);
          if (!isVisible(el)) continue;

          const label = labelOf(el);
          if (!label) continue;
          if (!isButtonLikeAnchor(el)) continue;
          if (isUnsafeLegalLink(el, label)) continue;

          const shouldClick = textPatterns.some((rx) => rx.test(label));
          if (!shouldClick) continue;

          try {
            el.click();
            clicked++;
            if (clicked >= 4) return clicked;
          } catch (_) {}
        }
      }

      return clicked;
    });
  };

  let totalClicked = 0;
  for (let i = 0; i < 3; i++) {
    try {
      const clicked = await clickRound();
      totalClicked += Number(clicked || 0);
      if (!clicked) break;
      await page.waitForTimeout(350);
    } catch (_) {
      break;
    }
  }

  return { clicked: totalClicked };
}

function patchPageGotoForOverlays(page) {
  if (!page || page.__overlayGotoPatched) return page;
  page.__overlayGotoPatched = true;

  const originalGoto = page.goto.bind(page);
  page.__overlayOriginalGoto = originalGoto;
  page.goto = async (...args) => {
    const response = await originalGoto(...args);
    try {
      await dismissCommonOverlays(page);
    } catch (_) {}
    return response;
  };

  return page;
}

function patchBrowserCloseForTmpCleanup(browser, userDataDir) {
  if (!browser || !userDataDir || !tmpCleanup || typeof tmpCleanup.removeBrowserProfileDir !== 'function') {
    return browser;
  }

  const originalClose = browser.close.bind(browser);
  browser.close = async (...args) => {
    try {
      return await originalClose(...args);
    } finally {
      try {
        tmpCleanup.removeBrowserProfileDir(userDataDir, { force: true });
      } catch (_) {}
    }
  };

  browser.on('disconnected', () => {
    setTimeout(() => {
      try {
        tmpCleanup.removeBrowserProfileDir(userDataDir);
      } catch (_) {}
    }, 1000).unref?.();
  });

  return browser;
}

async function hardenPage(page) {
  await page.setViewport({
    width: 1280,
    height: 900,
    deviceScaleFactor: 1,
    isMobile: false,
    hasTouch: false,
  });

  await page.setUserAgent(USER_AGENT);
  await page.setExtraHTTPHeaders(COMMON_HEADERS);

  page.setDefaultNavigationTimeout(600000);
  page.setDefaultTimeout(600000);

  // Extra protections for sites that check basic automation flags.
  await page.evaluateOnNewDocument(() => {
    try {
      Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
      Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
      Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
      Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
    } catch (_) {}
  });

  patchPageGotoForOverlays(page);
  return page;
}

function safeAbort(request) {
  try {
    if (typeof request.isInterceptResolutionHandled === 'function' && request.isInterceptResolutionHandled()) return;
    request.abort();
  } catch (_) {}
}

function safeContinue(request) {
  try {
    if (typeof request.isInterceptResolutionHandled === 'function' && request.isInterceptResolutionHandled()) return;
    request.continue();
  } catch (_) {}
}

function shouldBlockDefault(request) {
  const url = request.url().toLowerCase();
  const resourceType = request.resourceType();

  return (
    resourceType === 'media' ||
    url.includes('google-analytics.com') ||
    url.includes('googletagmanager.com') ||
    url.includes('doubleclick.net') ||
    url.includes('facebook.net') ||
    url.includes('clarity.ms') ||
    url.includes('hotjar.com')
  );
}

function shouldBlockCustomDefault(request) {
  const url = request.url().toLowerCase();
  const resourceType = request.resourceType();

  return (
    resourceType === 'media' ||
    resourceType === 'font' ||
    url.includes('google-analytics.com') ||
    url.includes('googletagmanager.com') ||
    url.includes('doubleclick.net')
  );
}

async function createNewPage(browser) {
  const page = await browser.newPage();
  await hardenPage(page);

  // Enable request interception ONCE
  await page.setRequestInterception(true);

  // Set a single request handler that blocks unnecessary resources
  page.on('request', (request) => {
    if (shouldBlockDefault(request)) {
      safeAbort(request);
      return;
    }

    // Allow all other requests
    safeContinue(request);
  });

  return page;
}

async function createNewPageWithCustomInterception(browser, customFilter) {
  const page = await browser.newPage();
  await hardenPage(page);

  // Enable request interception
  await page.setRequestInterception(true);

  // Set a single request handler with custom filtering
  page.on('request', (request) => {
    if (shouldBlockCustomDefault(request)) {
      safeAbort(request);
      return;
    }

    // Apply custom filter if provided
    try {
      if (customFilter && customFilter(request)) {
        safeAbort(request);
        return;
      }
    } catch (_) {
      // If custom filter throws, do not kill navigation.
    }

    safeContinue(request);
  });

  return page;
}

module.exports = {
  launchBrowser,
  createNewPage,
  createNewPageWithCustomInterception,
  hardenPage,
  dismissCommonOverlays,
};
