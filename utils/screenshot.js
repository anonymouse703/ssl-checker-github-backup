const path = require("path");

const TOOL_VIEWPORTS = {
  intodns: 1080,
  ssllabs: 1212,
  pagespeed: 961,
  pingdom: 1044,
  sucuri: 961,
  dns: 1280,
  pagerank: 1280,
  default: 1280,
};

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function setToolViewport(page, toolName, height = 900) {
  const width = TOOL_VIEWPORTS[toolName] || TOOL_VIEWPORTS.default;

  await page.setViewport({
    width,
    height,
    deviceScaleFactor: 1,
  });

  return width;
}

async function waitForSelector(page, selector, timeoutMs = 15000) {
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

// Generic fast screenshot — used by DNS, PageRank, and any generic URL.
// Uses domcontentloaded instead of networkidle2 so it doesn't wait for
// every third-party tracker/beacon to finish before capturing.
async function screenshotUrl(
  page,
  url,
  filepath,
  waitForSel = null,
  extraWaitMs = 800,
  maxWaitMs = 30000,
  fullPage = true,
  toolName = "default",
) {
  try {
    await setToolViewport(page, toolName);

    await page.goto(url, { waitUntil: "domcontentloaded", timeout: maxWaitMs });

    if (waitForSel) {
      await waitForSelector(page, waitForSel, 10000);
    }

    await wait(extraWaitMs);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: filepath, fullPage });
    return path.basename(filepath);
  } catch (err) {
    return null;
  }
}

// ── SSL Labs helpers ──────────────────────────────────────────────────────────

function urlMatchesTarget(url, domain) {
  if (!url) return false;
  const variants = [
    `d=${domain}`,
    `d=${encodeURIComponent(domain)}`,
    `d=${domain.replace(/\./g, "%2E")}`,
    `d=${domain.replace(/\./g, "%2e")}`,
  ];
  return variants.some((v) => url.includes(v));
}

function htmlLooksLikeSslProgress(html) {
  return (
    /Please wait/i.test(html) ||
    /in progress/i.test(html) ||
    /assessment in progress/i.test(html) ||
    /calculating/i.test(html) ||
    /testing tls/i.test(html) ||
    /testing protocol intolerance/i.test(html) ||
    /queued/i.test(html) ||
    /starting/i.test(html)
  );
}

function htmlLooksLikeSslFinal(html) {
  return (
    /Miscellaneous/i.test(html) &&
    (/Protocol Support/i.test(html) ||
      /Cipher Suites/i.test(html) ||
      /Handshake Simulation/i.test(html) ||
      /HTTP Requests/i.test(html) ||
      /Certificate #1/i.test(html) ||
      /Additional Certificates/i.test(html) ||
      /Server Key and Certificate/i.test(html) ||
      /Overall Rating/i.test(html) ||
      /Grade/i.test(html))
  );
}

// SSL Labs screenshot
async function screenshotSslLabs(page, domain, filepath) {
  try {
    await setToolViewport(page, "ssllabs");

    const url = `https://www.ssllabs.com/ssltest/analyze.html?d=${domain}&hideResults=on&latest`;

    await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });

    const html = await page.content();
    const alreadyDone =
      urlMatchesTarget(page.url(), domain) &&
      !htmlLooksLikeSslProgress(html) &&
      htmlLooksLikeSslFinal(html);

    if (alreadyDone) {
      await wait(1500);
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.screenshot({ path: filepath, fullPage: true });
      return path.basename(filepath);
    }

    const maxWaitMs = 3 * 60 * 1000;
    const pollEveryMs = 10000;
    const start = Date.now();

    while (Date.now() - start < maxWaitMs) {
      await wait(pollEveryMs);

      try {
        await page.reload({ waitUntil: "networkidle2", timeout: 60000 });
      } catch (_) {}

      const currentUrl = page.url();
      const pageHtml = await page.content();
      const isProgress = htmlLooksLikeSslProgress(pageHtml);
      const isFinal = htmlLooksLikeSslFinal(pageHtml);

      if (urlMatchesTarget(currentUrl, domain) && !isProgress && isFinal) {
        await wait(1500);
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.screenshot({ path: filepath, fullPage: true });
        return path.basename(filepath);
      }
    }

    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: filepath, fullPage: true });
    return path.basename(filepath);
  } catch (err) {
    return null;
  }
}

// Cookie/consent banner dismissal for PageSpeed
async function dismissPageSpeedCookieBanner(page) {
  const selectors = [
    "button",
    "[role='button']",
    "input[type='button']",
    "input[type='submit']",
  ];

  for (const selector of selectors) {
    const elements = await page.$$(selector);
    for (const el of elements) {
      try {
        const text = await page.evaluate(
          (node) =>
            (node.innerText || node.value || node.textContent || "").trim(),
          el,
        );
        if (/ok, got it|got it|accept|i agree|agree/i.test(text)) {
          await el.click({ delay: 100 });
          await wait(1000);
          return true;
        }
      } catch (_) {}
    }
  }

  return false;
}

function pageSpeedLooksLikeFinal(html) {
  return (
    /First Contentful Paint/i.test(html) ||
    /Largest Contentful Paint/i.test(html) ||
    /Total Blocking Time/i.test(html) ||
    /Cumulative Layout Shift/i.test(html) ||
    /Speed Index/i.test(html) ||
    /Performance/i.test(html)
  );
}

// PageSpeed screenshot
async function screenshotPageSpeed(page, domain, filepath) {
  try {
    await setToolViewport(page, "pagespeed");

    const reportUrl = `https://pagespeed.web.dev/report?url=https://${domain}`;

    await page.goto(reportUrl, { waitUntil: "networkidle2", timeout: 60000 });
    await wait(2000);
    await dismissPageSpeedCookieBanner(page);

    const maxWaitMs = 90 * 1000;
    const pollEveryMs = 5000;
    const start = Date.now();
    let ready = false;

    while (Date.now() - start < maxWaitMs) {
      const html = await page.content();

      const isFinal = pageSpeedLooksLikeFinal(html);
      const stillAnalyzing = await page
        .evaluate(() => {
          const buttons = Array.from(
            document.querySelectorAll(
              "button, [role='button'], input[type='submit']",
            ),
          );
          return buttons.some((btn) =>
            /analyze/i.test((btn.innerText || btn.value || "").trim()),
          );
        })
        .catch(() => false);

      if (isFinal && !stillAnalyzing) {
        ready = true;
        break;
      }

      await wait(pollEveryMs);
    }

    await wait(ready ? 2000 : 1000);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: filepath, fullPage: true });
    return path.basename(filepath);
  } catch (err) {
    return null;
  }
}

// Sucuri screenshot
async function screenshotSucuri(page, domain, filepath) {
  try {
    await setToolViewport(page, "sucuri");

    const url = `https://sitecheck.sucuri.net/results/${domain}`;

    await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });

    await waitForSelector(page, ".scan-results, .site-status, h2, body", 15000);
    await wait(1500);

    await page.evaluate(() => window.scrollTo(0, 0));
    await page.screenshot({ path: filepath, fullPage: true });
    return path.basename(filepath);
  } catch (err) {
    return null;
  }
}

module.exports = {
  wait,
  waitForSelector,
  setToolViewport,
  screenshotUrl,
  screenshotSslLabs,
  screenshotPageSpeed,
  screenshotSucuri,
};