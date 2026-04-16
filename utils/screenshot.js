const path = require("path");

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

async function captureViewportWidthFullHeight(page, filepath, options = {}) {
  const {
    width,
    left = 0,
    right = 0,
    top = 0,
    bottom = 0,
    minHeight = 100,
  } = options;

  if (!width) {
    throw new Error("captureViewportWidthFullHeight requires width.");
  }

  await page.evaluate(() => window.scrollTo(0, 0));
  await wait(250);

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

  return path.basename(filepath);
}

module.exports = {
  wait,
  waitForSelectorSafe,
  setFixedViewport,
  getFullPageHeight,
  captureViewportWidthFullHeight,
};