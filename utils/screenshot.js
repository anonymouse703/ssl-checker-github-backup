'use strict';

const fs = require('fs');
const path = require('path');

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForSelectorSafe(page, selector, timeoutMs = 15000) {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    try {
      const found = await page.$(selector);
      if (found) return true;
    } catch (_) {}

    await wait(300);
  }

  return false;
}

async function setFixedViewport(page, width, height = 900) {
  await page.setViewport({
    width,
    height,
    deviceScaleFactor: 1,
    isMobile: false,
    hasTouch: false,
  });

  return { width, height };
}

async function getFullPageHeight(page) {
  return page.evaluate(() => {
    const body = document.body;
    const html = document.documentElement;

    return Math.max(
      body ? body.scrollHeight : 0,
      body ? body.offsetHeight : 0,
      html ? html.clientHeight : 0,
      html ? html.scrollHeight : 0,
      html ? html.offsetHeight : 0
    );
  });
}

function verifyScreenshotFile(filepath, minBytes = 1000) {
  if (!fs.existsSync(filepath)) {
    throw new Error(`Screenshot file was not created: ${filepath}`);
  }

  const size = fs.statSync(filepath).size;

  if (size < minBytes) {
    throw new Error(`Screenshot file is too small: ${filepath} (${size} bytes)`);
  }

  return true;
}


async function dismissCommonOverlays(page, options = {}) {
  const settleMs = options.settleMs || 500;
  try {
    await page.evaluate(() => {
      const clean = (v) => String(v || '').replace(/\s+/g, ' ').trim().toLowerCase();
      const clickWords = [
        'accept all', 'accept cookies', 'accept', 'agree', 'i agree', 'allow all', 'got it',
        'continue', 'continue to site', 'enter site', 'yes, i am 21', 'i am 21', 'i am over 21',
        'yes', 'close', 'no thanks', 'not now', 'skip'
      ];
      const candidates = Array.from(document.querySelectorAll('button, a, input[type="button"], input[type="submit"], [role="button"]'));
      for (const el of candidates) {
        const text = clean(el.innerText || el.textContent || el.value || el.getAttribute('aria-label') || el.getAttribute('title'));
        if (!text) continue;
        if (clickWords.some(w => text === w || text.includes(w))) {
          try { el.click(); } catch (_) {}
        }
      }

      const blockers = Array.from(document.querySelectorAll('div, section, aside, dialog'));
      for (const el of blockers) {
        try {
          const st = window.getComputedStyle(el);
          if (!st || st.display === 'none' || st.visibility === 'hidden') continue;
          const pos = st.position;
          if (pos !== 'fixed' && pos !== 'sticky') continue;
          const rect = el.getBoundingClientRect();
          const area = rect.width * rect.height;
          const screenArea = window.innerWidth * window.innerHeight;
          if (area < screenArea * 0.12) continue;
          const txt = clean(el.innerText || el.textContent || '');
          const looksLikeOverlay = /cookie|cookies|privacy|newsletter|subscribe|sign up|age|verify|modal|popup|offer|discount/.test(txt) || Number(st.zIndex || 0) >= 1000;
          if (looksLikeOverlay) {
            el.setAttribute('data-ind-hidden-overlay', '1');
            el.style.setProperty('display', 'none', 'important');
            el.style.setProperty('visibility', 'hidden', 'important');
            el.style.setProperty('opacity', '0', 'important');
          }
        } catch (_) {}
      }

      try {
        document.documentElement.style.overflow = 'auto';
        document.body.style.overflow = 'auto';
      } catch (_) {}
    });
  } catch (_) {}
  if (settleMs > 0) await wait(settleMs);
}

async function captureViewportWidthFullHeight(page, filepath, options = {}) {
  const {
    width,
    left = 0,
    right = 0,
    top = 0,
    bottom = 0,
    minHeight = 100,
    minBytes = 1000,
  } = options;

  if (!width) {
    throw new Error('captureViewportWidthFullHeight requires width.');
  }

  fs.mkdirSync(path.dirname(filepath), { recursive: true });

  await dismissCommonOverlays(page).catch(() => {});
  await page.evaluate(() => window.scrollTo(0, 0));
  await wait(500);

  const fullHeight = await getFullPageHeight(page);

  const clip = {
    x: Math.max(0, left),
    y: Math.max(0, top),
    width: Math.max(100, width - left - right),
    height: Math.max(minHeight, fullHeight - top - bottom),
  };

  await page.screenshot({
    path: filepath,
    clip,
  });

  verifyScreenshotFile(filepath, minBytes);

  return path.basename(filepath);
}

async function captureWithRetry(page, filepath, options = {}, maxRetries = 3) {
  const { retryBaseDelayMs = 3000, settleBeforeCaptureMs = 1000 } = options;
  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      if (settleBeforeCaptureMs > 0) {
        await wait(settleBeforeCaptureMs);
      }

      const file = await captureViewportWidthFullHeight(page, filepath, options);
      verifyScreenshotFile(filepath, options.minBytes || 1000);
      return file;
    } catch (err) {
      lastError = err;
      console.error(`[screenshot] Attempt ${attempt}/${maxRetries} failed for ${path.basename(filepath)}: ${err.message}`);

      try {
        if (fs.existsSync(filepath)) {
          fs.unlinkSync(filepath);
        }
      } catch (_) {}

      if (attempt < maxRetries) {
        await wait(retryBaseDelayMs * attempt);
      }
    }
  }

  throw lastError || new Error(`Screenshot failed after ${maxRetries} retries: ${filepath}`);
}

module.exports = {
  wait,
  waitForSelectorSafe,
  setFixedViewport,
  getFullPageHeight,
  verifyScreenshotFile,
  captureViewportWidthFullHeight,
  captureWithRetry,
  dismissCommonOverlays,
};