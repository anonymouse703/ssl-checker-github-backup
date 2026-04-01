'use strict';

const puppeteer = require('puppeteer');
const { resolveChromePath } = require('./chrome-path');

const POOL_SIZE = Math.max(1, parseInt(process.env.BROWSER_POOL_SIZE || '2', 10));
const DEFAULT_TIMEOUT_MS = parseInt(process.env.BROWSER_POOL_TIMEOUT_MS || '600000', 10);
const IDLE_TIMEOUT_MS = parseInt(process.env.BROWSER_IDLE_TIMEOUT_MS || '300000', 10);

const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36';

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

function resetIdleTimer() {
  lastActivity = Date.now();
  if (idleTimer) clearTimeout(idleTimer);
  
  idleTimer = setTimeout(async () => {
    const idleMs = Date.now() - lastActivity;
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

  const browser = await puppeteer.launch({
    headless: true,
    executablePath,
    args: LAUNCH_ARGS,
    protocolTimeout: 600000,
    timeout: 600000,
    ignoreHTTPSErrors: true,
  });

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

async function withBrowserPage(fn, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const lease = await acquireBrowser(timeoutMs);
  let page = null;
  
  try {
    page = await lease.browser.newPage();
    await page.setUserAgent(USER_AGENT);
    await page.setViewport({ width: 1280, height: 900 });
    
    // Disable unnecessary resources
    await page.setRequestInterception(true);
    page.on('request', (request) => {
      const url = request.url().toLowerCase();
      const resourceType = request.resourceType();
      
      if (
        resourceType === 'media' ||
        resourceType === 'font' ||
        url.includes('google-analytics.com') ||
        url.includes('googletagmanager.com') ||
        url.includes('doubleclick.net')
      ) {
        request.abort();
        return;
      }
      request.continue();
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
};