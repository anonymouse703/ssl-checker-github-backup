/**
 * utils/browser-pool.js
 * Browser pool for shared Puppeteer pages.
 * Patched to actually enable puppeteer-extra stealth when installed.
 */

'use strict';

let puppeteer;
try {
  puppeteer = require('puppeteer-extra');
  const StealthPlugin = require('puppeteer-extra-plugin-stealth');
  puppeteer.use(StealthPlugin());
  console.log('[pool] puppeteer-extra stealth enabled');
} catch (err) {
  // Fallback keeps older VMs running if stealth package is not installed yet.
  puppeteer = require('puppeteer');
  console.warn(`[pool] puppeteer-extra stealth not available, using normal puppeteer: ${err.message}`);
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
  console.warn(`[pool] tmp cleanup helper not available: ${err.message}`);
}

const POOL_SIZE = Math.max(1, parseInt(process.env.BROWSER_POOL_SIZE || '2', 10));
const DEFAULT_TIMEOUT_MS = parseInt(process.env.BROWSER_POOL_TIMEOUT_MS || '600000', 10);
const IDLE_TIMEOUT_MS = parseInt(process.env.BROWSER_IDLE_TIMEOUT_MS || '300000', 10);

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
  '--js-flags=--max-old-space-size=256',
  '--disable-features=TranslateUI,BlinkGenPropertyTrees',
  '--disable-blink-features=AutomationControlled',
  '--lang=en-US,en',
  // '--single-process',
  '--disable-accelerated-2d-canvas',
  '--disable-accelerated-jpeg-decoding',
  '--disable-accelerated-mjpeg-decode',
  '--disable-accelerated-video-decode',
];

const pool = [];
const queue = [];
let initialized = false;
let initPromise = null;
let idleTimer = null;
let lastActivity = Date.now();


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


function resetIdleTimer() {
  lastActivity = Date.now();
  if (idleTimer) clearTimeout(idleTimer);

  idleTimer = setTimeout(async () => {
    const idleMs = Date.now() - lastActivity;
    const inUseCount = pool.filter(s => s.inUse).length;

    // Do not close the pool while a long-running tool, especially Pingdom,
    // still owns a browser lease. The previous version could close Chrome
    // after BROWSER_IDLE_TIMEOUT_MS even when Pingdom was still running,
    // causing "Connection closed" on later retry attempts.
    if (inUseCount > 0) {
      resetIdleTimer();
      return;
    }

    if (idleMs >= IDLE_TIMEOUT_MS) {
      console.log(`[pool] Browser pool idle for ${Math.round(idleMs / 1000)}s, closing...`);
      await closePool();
    }
  }, IDLE_TIMEOUT_MS);
}

async function spawnBrowser(id) {
  const executablePath = resolveChromePath();

  if (!executablePath) {
    throw new Error(
      'Chrome executable not found. Set PUPPETEER_EXECUTABLE_PATH in .env or install Chrome.'
    );
  }

  // Clean stale profile folders from older crashed scans. This skips active
  // Chrome profiles and only removes old ones.
  try {
    if (tmpCleanup && typeof tmpCleanup.cleanupStaleBrowserProfiles === 'function') {
      tmpCleanup.cleanupStaleBrowserProfiles({
        minAgeMs: Number(process.env.PUPPETEER_PROFILE_CLEANUP_STARTUP_MINUTES || 60) * 60 * 1000,
      });
    }
  } catch (_) {}

  const userDataDir = tmpCleanup && typeof tmpCleanup.createManagedUserDataDir === 'function'
    ? tmpCleanup.createManagedUserDataDir()
    : undefined;

  const browser = patchBrowserCloseForTmpCleanup(await puppeteer.launch({
    headless: PUPPETEER_HEADLESS,
    executablePath,
    userDataDir,
    args: LAUNCH_ARGS,
    protocolTimeout: 600000,
    timeout: 600000,
    ignoreHTTPSErrors: true,
  }), userDataDir);

  browser.on('disconnected', async () => {
    const slot = pool.find(s => s.id === id);
    if (!slot) return;

    try {
      console.log(`[pool] Browser ${id} disconnected, restarting...`);
      const replacement = await spawnBrowser(id);
      slot.browser = replacement;
      slot.inUse = false;
      drainQueue();
    } catch (err) {
      console.error(`[pool] browser ${id} failed to restart: ${err.message}`);
      slot.inUse = false;
    }
  });

  return browser;
}

async function initPool() {
  if (initialized) return;
  if (initPromise) return initPromise;

  initPromise = (async () => {
    console.log(`[pool] Initializing browser pool with ${POOL_SIZE} browsers`);
    for (let i = 0; i < POOL_SIZE; i++) {
      try {
        const browser = await spawnBrowser(i);
        pool.push({ browser, inUse: false, id: i });
        console.log(`[pool] Browser ${i} started`);
      } catch (err) {
        console.error(`[pool] Failed to start browser ${i}: ${err.message}`);
        // Continue with fewer browsers
      }
    }
    initialized = true;
    resetIdleTimer();
  })();

  try {
    await initPromise;
  } finally {
    initPromise = null;
  }
}

async function acquireBrowser(timeoutMs = DEFAULT_TIMEOUT_MS) {
  if (!initialized) {
    await initPool();
  }

  // Try to reuse an existing idle browser first
  const slot = pool.find(s => !s.inUse);
  if (slot) {
    slot.inUse = true;
    resetIdleTimer();
    return {
      browser: slot.browser,
      release: () => releaseSlot(slot),
    };
  }

  // No free browsers, queue the request
  return new Promise((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      const idx = queue.findIndex(q => q.timeoutId === timeoutId);
      if (idx !== -1) queue.splice(idx, 1);
      reject(
        new Error(
          `Browser pool timeout after ${Math.round(timeoutMs / 1000)}s — all ${pool.length} browsers busy`
        )
      );
    }, timeoutMs);

    queue.push({ resolve, reject, timeoutId });
  });
}

function releaseSlot(slot) {
  if (queue.length > 0) {
    const next = queue.shift();
    clearTimeout(next.timeoutId);
    slot.inUse = true;
    resetIdleTimer();
    next.resolve({
      browser: slot.browser,
      release: () => releaseSlot(slot),
    });
  } else {
    slot.inUse = false;
    resetIdleTimer();
    console.log(`[pool] browser ${slot.id} released (${pool.filter(s => s.inUse).length}/${pool.length} in use)`);
  }
}

function drainQueue() {
  const slot = pool.find(s => !s.inUse);
  if (!slot || queue.length === 0) return;

  slot.inUse = true;
  const next = queue.shift();
  clearTimeout(next.timeoutId);
  resetIdleTimer();

  next.resolve({
    browser: slot.browser,
    release: () => releaseSlot(slot),
  });
}

async function closePool() {
  if (idleTimer) {
    clearTimeout(idleTimer);
    idleTimer = null;
  }

  const browsers = pool.splice(0, pool.length);
  queue.splice(0, queue.length).forEach(item => clearTimeout(item.timeoutId));
  initialized = false;
  initPromise = null;
  lastActivity = 0;

  for (const slot of browsers) {
    try {
      if (slot.browser) {
        console.log(`[pool] Closing browser ${slot.id}...`);
        await slot.browser.close();
      }
    } catch (_) {
      // ignore close errors
    }
  }
  console.log(`[pool] All ${browsers.length} browsers closed`);
}

function poolStats() {
  return {
    total: pool.length,
    inUse: pool.filter(s => s.inUse).length,
    free: pool.filter(s => !s.inUse).length,
    queued: queue.length,
    chromePath: resolveChromePath(),
    idleMs: lastActivity ? Math.round((Date.now() - lastActivity) / 1000) : 0,
  };
}

async function hardenPage(page) {
  await page.setUserAgent(USER_AGENT);
  await page.setExtraHTTPHeaders(COMMON_HEADERS);
  await page.setViewport({
    width: 1280,
    height: 900,
    deviceScaleFactor: 1,
    isMobile: false,
    hasTouch: false,
  });

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

function shouldBlockRequest(request) {
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

async function withBrowserPage(fn, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const lease = await acquireBrowser(timeoutMs);
  let page = null;

  try {
    page = await lease.browser.newPage();
    await hardenPage(page);

    // Disable unnecessary resources
    await page.setRequestInterception(true);
    page.on('request', (request) => {
      if (shouldBlockRequest(request)) {
        safeAbort(request);
        return;
      }
      safeContinue(request);
    });

    return await fn(page);
  } finally {
    try {
      if (page) {
        // Clear page memory
        await page.evaluate(() => {
          localStorage.clear();
          sessionStorage.clear();
        });
        await page.close();
      }
    } catch (_) {
      // ignore
    }
    lease.release();
  }
}

module.exports = {
  initPool,
  acquireBrowser,
  closePool,
  poolStats,
  withBrowserPage,
  hardenPage,
  dismissCommonOverlays,
};
