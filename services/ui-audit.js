"use strict";

const fs = require("fs");
const path = require("path");

const {
  checkBrokenLinks,
  buildStartUrl,
  normalizeDomain
} = require("./broken-links");

const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

function logIf(debug, ...args) {
  if (debug) console.log("[ui-audit]", ...args);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeFileName(input) {
  return String(input || "site")
    .replace(/^https?:\/\//i, "")
    .replace(/[^a-z0-9._-]+/gi, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120);
}

function gradeFromScore(score) {
  if (score >= 90) return "A";
  if (score >= 80) return "B";
  if (score >= 70) return "C";
  if (score >= 60) return "D";
  return "F";
}

function statusFromScore(score) {
  if (score >= 90) return "excellent";
  if (score >= 75) return "good";
  if (score >= 60) return "needs_improvement";
  return "poor";
}

async function gotoWithFallback(page, startUrl, timeoutMs, debug) {
  const urlsToTry = [];

  urlsToTry.push(startUrl);

  if (startUrl.startsWith("https://")) {
    urlsToTry.push(startUrl.replace(/^https:\/\//i, "http://"));
  } else if (startUrl.startsWith("http://")) {
    urlsToTry.push(startUrl.replace(/^http:\/\//i, "https://"));
  }

  let lastError = null;

  for (const testUrl of urlsToTry) {
    try {
      logIf(debug, "Opening:", testUrl);

      const response = await page.goto(testUrl, {
        waitUntil: "domcontentloaded",
        timeout: timeoutMs
      });

      await sleep(1000);

      return {
        ok: true,
        url: page.url() || testUrl,
        status: response ? response.status() : 0
      };
    } catch (err) {
      lastError = err;
      logIf(debug, "Open failed:", testUrl, err.message);
    }
  }

  return {
    ok: false,
    url: startUrl,
    status: 0,
    error: lastError ? lastError.message : "Unable to open page"
  };
}

async function collectPageMetrics(page) {
  return page.evaluate(() => {
    function cleanText(value) {
      return String(value || "")
        .replace(/\s+/g, " ")
        .trim();
    }

    function isVisible(el) {
      if (!el) return false;

      const style = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();

      return (
        style &&
        style.display !== "none" &&
        style.visibility !== "hidden" &&
        Number(style.opacity || 1) !== 0 &&
        rect.width > 0 &&
        rect.height > 0
      );
    }

    const title = cleanText(document.title || "");

    const metaDescriptionTag = document.querySelector("meta[name='description']");
    const metaDescription = metaDescriptionTag
      ? cleanText(metaDescriptionTag.getAttribute("content") || "")
      : "";

    const faviconTag =
      document.querySelector("link[rel~='icon']") ||
      document.querySelector("link[rel='shortcut icon']") ||
      document.querySelector("link[rel='apple-touch-icon']");

    const canonicalTag = document.querySelector("link[rel='canonical']");

    const h1s = Array.from(document.querySelectorAll("h1"))
      .filter(isVisible)
      .map((h) => cleanText(h.innerText || h.textContent || ""))
      .filter(Boolean);

    const images = Array.from(document.querySelectorAll("img"));
    const visibleImages = images.filter(isVisible);

    const imagesWithoutAlt = visibleImages.filter((img) => {
      const alt = img.getAttribute("alt");
      return alt === null || cleanText(alt) === "";
    });

    const links = Array.from(document.querySelectorAll("a[href]"));
    const emptyLinks = links.filter((a) => {
      const text = cleanText(a.innerText || a.getAttribute("aria-label") || a.getAttribute("title") || "");
      return !text;
    });

    const buttons = Array.from(document.querySelectorAll("button, [role='button'], input[type='button'], input[type='submit']"));
    const emptyButtons = buttons.filter((b) => {
      const text = cleanText(
        b.innerText ||
        b.value ||
        b.getAttribute("aria-label") ||
        b.getAttribute("title") ||
        ""
      );

      return isVisible(b) && !text;
    });

    const textElements = Array.from(document.querySelectorAll("p, li, span, a, div"))
      .filter(isVisible)
      .filter((el) => cleanText(el.innerText || el.textContent || "").length >= 30);

    let smallTextCount = 0;

    for (const el of textElements.slice(0, 500)) {
      const style = window.getComputedStyle(el);
      const size = parseFloat(style.fontSize || "16");

      if (size > 0 && size < 12) {
        smallTextCount++;
      }
    }

    const bodyText = cleanText(document.body ? document.body.innerText || "" : "");
    const bodyTextLength = bodyText.length;

    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;

    const scrollWidth = Math.max(
      document.body ? document.body.scrollWidth : 0,
      document.documentElement ? document.documentElement.scrollWidth : 0
    );

    const scrollHeight = Math.max(
      document.body ? document.body.scrollHeight : 0,
      document.documentElement ? document.documentElement.scrollHeight : 0
    );

    const hasHorizontalScroll = scrollWidth > viewportWidth + 8;

    const copyrightYears = Array.from(bodyText.matchAll(/\b(19|20)\d{2}\b/g))
      .map((m) => Number(m[0]))
      .filter((y) => y >= 1990 && y <= 2100);

    const latestYear = copyrightYears.length ? Math.max(...copyrightYears) : null;
    const currentYear = new Date().getFullYear();

    const oldCopyrightYear =
      latestYear !== null && latestYear < currentYear - 1 ? latestYear : null;

    return {
      finalUrl: window.location.href,
      title,
      titleLength: title.length,
      metaDescription,
      metaDescriptionLength: metaDescription.length,
      canonical: canonicalTag ? canonicalTag.href || "" : "",
      faviconFound: !!faviconTag,
      faviconHref: faviconTag ? faviconTag.href || "" : "",
      h1Count: h1s.length,
      h1Text: h1s[0] || "",
      imageCount: visibleImages.length,
      imagesWithoutAltCount: imagesWithoutAlt.length,
      linkCount: links.length,
      emptyLinksCount: emptyLinks.length,
      buttonCount: buttons.length,
      emptyButtonsCount: emptyButtons.length,
      smallTextCount,
      bodyTextLength,
      viewportWidth,
      viewportHeight,
      scrollWidth,
      scrollHeight,
      hasHorizontalScroll,
      oldCopyrightYear
    };
  });
}

async function saveScreenshot(page, screenshotDir, domain, label) {
  if (!screenshotDir) return "";

  fs.mkdirSync(screenshotDir, { recursive: true });

  const stamp = new Date()
    .toISOString()
    .replace(/[:.]/g, "-");

  const filename = `${safeFileName(domain)}-${label}-${stamp}.png`;
  const fullPath = path.join(screenshotDir, filename);

  await page.screenshot({
    path: fullPath,
    fullPage: true
  });

  return fullPath;
}

function scoreUiAudit({
  desktop,
  mobile,
  consoleErrors,
  pageErrors,
  requestFailures,
  brokenResult
}) {
  let score = 100;
  const issues = [];

  function deduct(points, message) {
    score -= points;
    issues.push(message);
  }

  if (!desktop.title) {
    deduct(10, "Missing page title");
  } else if (desktop.titleLength < 10) {
    deduct(4, "Page title is too short");
  } else if (desktop.titleLength > 70) {
    deduct(3, "Page title may be too long");
  }

  if (!desktop.metaDescription) {
    deduct(8, "Missing meta description");
  } else if (desktop.metaDescriptionLength < 50) {
    deduct(3, "Meta description is too short");
  } else if (desktop.metaDescriptionLength > 170) {
    deduct(3, "Meta description may be too long");
  }

  if (!desktop.faviconFound) {
    deduct(5, "Missing favicon");
  }

  if (desktop.h1Count === 0) {
    deduct(6, "Missing H1 heading");
  } else if (desktop.h1Count > 1) {
    deduct(3, "Multiple H1 headings found");
  }

  if (desktop.bodyTextLength < 300) {
    deduct(6, "Homepage has very little visible text");
  }

  if (desktop.hasHorizontalScroll) {
    deduct(6, "Desktop layout has horizontal scroll");
  }

  if (mobile.hasHorizontalScroll) {
    deduct(12, "Mobile layout has horizontal scroll");
  }

  if (desktop.imageCount > 0) {
    const missingAltRatio = desktop.imagesWithoutAltCount / desktop.imageCount;

    if (desktop.imagesWithoutAltCount >= 10 || missingAltRatio >= 0.5) {
      deduct(10, "Many images are missing alt text");
    } else if (desktop.imagesWithoutAltCount > 0) {
      deduct(5, "Some images are missing alt text");
    }
  }

  if (desktop.emptyLinksCount >= 5) {
    deduct(5, "Several links have no readable text");
  } else if (desktop.emptyLinksCount > 0) {
    deduct(2, "Some links have no readable text");
  }

  if (desktop.emptyButtonsCount > 0) {
    deduct(4, "Some buttons have no readable label");
  }

  if (desktop.smallTextCount >= 10) {
    deduct(6, "Many text elements may be too small");
  } else if (desktop.smallTextCount > 0) {
    deduct(3, "Some text may be too small");
  }

  if (desktop.oldCopyrightYear) {
    deduct(4, `Old copyright year detected: ${desktop.oldCopyrightYear}`);
  }

  if (consoleErrors.length > 0) {
    deduct(Math.min(10, consoleErrors.length * 2), `${consoleErrors.length} browser console error(s) detected`);
  }

  if (pageErrors.length > 0) {
    deduct(Math.min(10, pageErrors.length * 3), `${pageErrors.length} page JavaScript error(s) detected`);
  }

  if (requestFailures.length > 0) {
    deduct(Math.min(8, requestFailures.length), `${requestFailures.length} failed resource request(s) detected`);
  }

  const brokenCount = brokenResult ? Number(brokenResult.BrokenLinks_Broken_Count || 0) : 0;
  const warningCount = brokenResult ? Number(brokenResult.BrokenLinks_Warning_Count || 0) : 0;

  if (brokenCount > 0) {
    deduct(Math.min(25, brokenCount * 5), `${brokenCount} broken link(s) found`);
  }

  if (warningCount > 0) {
    deduct(Math.min(8, warningCount * 2), `${warningCount} link warning(s) found`);
  }

  score = Math.max(0, Math.min(100, Math.round(score)));

  return {
    score,
    grade: gradeFromScore(score),
    status: statusFromScore(score),
    issues
  };
}

async function runUiAudit(options) {
  const opts = options || {};

  const debug = !!opts.debug;
  const timeoutMs = Number(opts.timeoutMs || 25000);
  const newTab = opts.newTab;

  if (typeof newTab !== "function") {
    throw new Error("runUiAudit requires newTab function");
  }

  const startUrl = buildStartUrl({
    domain: opts.domain,
    url: opts.url,
    startPath: opts.startPath
  });

  const cleanDomain = normalizeDomain(opts.domain || startUrl);

  const screenshotDir = opts.screenshotDir || "";
  const takeScreenshots = opts.takeScreenshots !== false;
  const checkLinks = opts.checkLinks !== false;

  const consoleErrors = [];
  const pageErrors = [];
  const requestFailures = [];

  let page = null;
  let desktop = {};
  let mobile = {};
  let desktopScreenshot = "";
  let mobileScreenshot = "";
  let finalUrl = startUrl;
  let openStatus = 0;

  try {
    page = await newTab();

    page.on("console", (msg) => {
      if (msg.type && msg.type() === "error") {
        consoleErrors.push(String(msg.text ? msg.text() : "").slice(0, 300));
      }
    });

    page.on("pageerror", (err) => {
      pageErrors.push(String(err.message || err).slice(0, 300));
    });

    page.on("requestfailed", (req) => {
      const failure = req.failure ? req.failure() : null;
      requestFailures.push({
        url: req.url(),
        errorText: failure ? failure.errorText : "request failed"
      });
    });

    await page.setUserAgent(DEFAULT_USER_AGENT);

    await page.setViewport({
      width: 1366,
      height: 768,
      deviceScaleFactor: 1
    });

    const openResult = await gotoWithFallback(page, startUrl, timeoutMs, debug);

    if (!openResult.ok) {
      throw new Error(openResult.error || "Unable to open website");
    }

    finalUrl = openResult.url || startUrl;
    openStatus = openResult.status || 0;

    desktop = await collectPageMetrics(page);

    if (takeScreenshots && screenshotDir) {
      desktopScreenshot = await saveScreenshot(page, screenshotDir, cleanDomain, "desktop");
    }

    await page.setViewport({
      width: 390,
      height: 844,
      isMobile: true,
      hasTouch: true,
      deviceScaleFactor: 2
    });

    await page.goto(finalUrl, {
      waitUntil: "domcontentloaded",
      timeout: timeoutMs
    }).catch(() => {});

    await sleep(1000);

    mobile = await collectPageMetrics(page);

    if (takeScreenshots && screenshotDir) {
      mobileScreenshot = await saveScreenshot(page, screenshotDir, cleanDomain, "mobile");
    }
  } finally {
    if (page) {
      try {
        await page.close();
      } catch (_) {}
    }
  }

  let brokenResult = null;

  if (checkLinks) {
    brokenResult = await checkBrokenLinks({
      domain: opts.domain,
      url: opts.url,
      startPath: opts.startPath,
      newTab,
      timeoutMs,
      maxPages: Number(opts.maxPages || 1),
      maxLinks: Number(opts.maxLinks || 80),
      includeExternal: !!opts.includeExternal,
      debug
    });
  }

  const scored = scoreUiAudit({
    desktop,
    mobile,
    consoleErrors,
    pageErrors,
    requestFailures,
    brokenResult
  });

  return {
    UI_Status: scored.status,
    UI_Score: scored.score,
    UI_Grade: scored.grade,
    UI_Source: startUrl,
    UI_Final_URL: finalUrl,
    UI_HTTP_Status: openStatus,

    UI_Title: desktop.title || "",
    UI_Title_Length: desktop.titleLength || 0,
    UI_MetaDescription: desktop.metaDescription || "",
    UI_MetaDescription_Length: desktop.metaDescriptionLength || 0,
    UI_Canonical: desktop.canonical || "",

    UI_FaviconFound: !!desktop.faviconFound,
    UI_Favicon: desktop.faviconHref || "",

    UI_H1_Count: desktop.h1Count || 0,
    UI_H1_Text: desktop.h1Text || "",

    UI_Image_Count: desktop.imageCount || 0,
    UI_ImagesWithoutAlt_Count: desktop.imagesWithoutAltCount || 0,

    UI_Link_Count: desktop.linkCount || 0,
    UI_EmptyLinks_Count: desktop.emptyLinksCount || 0,

    UI_Button_Count: desktop.buttonCount || 0,
    UI_EmptyButtons_Count: desktop.emptyButtonsCount || 0,

    UI_SmallText_Count: desktop.smallTextCount || 0,
    UI_BodyText_Length: desktop.bodyTextLength || 0,

    UI_Desktop_HorizontalScroll: !!desktop.hasHorizontalScroll,
    UI_Mobile_HorizontalScroll: !!mobile.hasHorizontalScroll,
    UI_IsMobileFriendly: !mobile.hasHorizontalScroll,

    UI_Desktop_Screenshot: desktopScreenshot,
    UI_Mobile_Screenshot: mobileScreenshot,

    UI_ConsoleErrors_Count: consoleErrors.length,
    UI_PageErrors_Count: pageErrors.length,
    UI_RequestFailures_Count: requestFailures.length,

    UI_ConsoleErrors_JSON: JSON.stringify(consoleErrors),
    UI_PageErrors_JSON: JSON.stringify(pageErrors),
    UI_RequestFailures_JSON: JSON.stringify(requestFailures.slice(0, 30)),

    UI_BrokenLinks_Count: brokenResult ? brokenResult.BrokenLinks_Broken_Count : 0,
    UI_BrokenLinks_Warning_Count: brokenResult ? brokenResult.BrokenLinks_Warning_Count : 0,
    UI_BrokenLinks_JSON: brokenResult ? brokenResult.BrokenLinks_Broken_JSON : "[]",
    UI_BrokenLinks_Warnings_JSON: brokenResult ? brokenResult.BrokenLinks_Warnings_JSON : "[]",

    UI_Issues: scored.issues,
    UI_Issues_JSON: JSON.stringify(scored.issues),

    UI_Error: ""
  };
}

module.exports = {
  runUiAudit
};