"use strict";

const axios = require("axios");
const { URL } = require("url");

const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

function logIf(debug, ...args) {
  if (debug) console.log("[broken-links]", ...args);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeDomain(input) {
  let s = String(input || "").trim();

  s = s.replace(/^https?:\/\//i, "");
  s = s.replace(/^www\./i, "");
  s = s.split("/")[0];
  s = s.split("?")[0];
  s = s.split("#")[0];

  return s.toLowerCase();
}

function normalizeHostname(hostname) {
  return String(hostname || "")
    .trim()
    .toLowerCase()
    .replace(/^www\./i, "");
}

function buildStartUrl({ domain, url, startPath }) {
  if (url && /^https?:\/\//i.test(String(url))) {
    return String(url).trim();
  }

  const cleanDomain = normalizeDomain(domain);

  if (!cleanDomain) {
    throw new Error("Missing domain or url");
  }

  let path = String(startPath || "/").trim();
  if (!path.startsWith("/")) path = "/" + path;

  return "https://" + cleanDomain + path;
}

function stripHash(rawUrl) {
  try {
    const u = new URL(rawUrl);
    u.hash = "";
    return u.toString();
  } catch (_) {
    return rawUrl;
  }
}

function isSkippableHref(href) {
  if (!href) return true;

  const h = String(href).trim().toLowerCase();

  if (!h) return true;
  if (h === "#") return true;
  if (h.startsWith("#")) return true;
  if (h.startsWith("mailto:")) return true;
  if (h.startsWith("tel:")) return true;
  if (h.startsWith("sms:")) return true;
  if (h.startsWith("javascript:")) return true;
  if (h.startsWith("data:")) return true;
  if (h.startsWith("whatsapp:")) return true;
  if (h.startsWith("skype:")) return true;

  return false;
}

function normalizeHref(baseUrl, href) {
  if (isSkippableHref(href)) return null;

  try {
    const absolute = new URL(String(href).trim(), baseUrl);
    if (!/^https?:$/i.test(absolute.protocol)) return null;

    absolute.hash = "";
    return absolute.toString();
  } catch (_) {
    return null;
  }
}

function isInternalUrl(testUrl, rootHostname) {
  try {
    const u = new URL(testUrl);
    return normalizeHostname(u.hostname) === normalizeHostname(rootHostname);
  } catch (_) {
    return false;
  }
}

function isLikelyHtmlPage(testUrl) {
  try {
    const u = new URL(testUrl);
    const pathname = u.pathname.toLowerCase();

    const skipExtensions = [
      ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
      ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
      ".zip", ".rar", ".7z", ".tar", ".gz",
      ".mp4", ".mov", ".avi", ".mp3", ".wav",
      ".css", ".js", ".json", ".xml"
    ];

    return !skipExtensions.some((ext) => pathname.endsWith(ext));
  } catch (_) {
    return false;
  }
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
      logIf(debug, "Opening page:", testUrl);

      const response = await page.goto(testUrl, {
        waitUntil: "domcontentloaded",
        timeout: timeoutMs
      });

      await sleep(800);

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

async function collectLinksFromPage(page, pageUrl, debug) {
  try {
    const rawLinks = await page.evaluate(() => {
      return Array.from(document.querySelectorAll("a[href]")).map((a) => {
        return {
          href: a.getAttribute("href") || "",
          text: (a.innerText || a.getAttribute("aria-label") || a.getAttribute("title") || "")
            .replace(/\s+/g, " ")
            .trim()
            .slice(0, 160)
        };
      });
    });

    const normalized = [];

    for (const item of rawLinks || []) {
      const href = normalizeHref(pageUrl, item.href);
      if (!href) continue;

      normalized.push({
        url: stripHash(href),
        text: item.text || "",
        source: pageUrl
      });
    }

    return normalized;
  } catch (err) {
    logIf(debug, "collectLinksFromPage error:", err.message);
    return [];
  }
}

async function checkUrlStatus(targetUrl, timeoutMs, debug) {
  const headers = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
  };

  async function request(method) {
    return axios.request({
      method,
      url: targetUrl,
      timeout: timeoutMs,
      maxRedirects: 5,
      validateStatus: () => true,
      headers
    });
  }

  try {
    let response = await request("HEAD");

    /*
      Some servers block HEAD even when the page is valid.
      Retry with GET for those cases.
    */
    if ([403, 405, 406, 429, 500, 501].includes(response.status)) {
      response = await request("GET");
    }

    const status = response.status || 0;
    const finalUrl =
      response &&
      response.request &&
      response.request.res &&
      response.request.res.responseUrl
        ? response.request.res.responseUrl
        : targetUrl;

    let state = "ok";

    if ([401, 403, 429].includes(status)) {
      state = "warning";
    } else if (status >= 400 || status === 0) {
      state = "broken";
    }

    return {
      url: targetUrl,
      finalUrl,
      status,
      state,
      error: ""
    };
  } catch (err) {
    logIf(debug, "Status check failed:", targetUrl, err.message);

    return {
      url: targetUrl,
      finalUrl: targetUrl,
      status: 0,
      state: "broken",
      error: err.message || String(err)
    };
  }
}

async function mapLimit(items, limit, worker) {
  const results = [];
  let index = 0;

  async function runOne() {
    while (index < items.length) {
      const currentIndex = index++;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  }

  const workers = [];
  const workerCount = Math.min(limit, items.length);

  for (let i = 0; i < workerCount; i++) {
    workers.push(runOne());
  }

  await Promise.all(workers);
  return results;
}

async function checkBrokenLinks(options) {
  const opts = options || {};

  const debug = !!opts.debug;
  const timeoutMs = Number(opts.timeoutMs || 20000);
  const linkTimeoutMs = Number(opts.linkTimeoutMs || 12000);
  const maxPages = Math.max(1, Number(opts.maxPages || 1));
  const maxLinks = Math.max(1, Number(opts.maxLinks || 80));
  const concurrency = Math.max(1, Number(opts.concurrency || 5));
  const includeExternal = !!opts.includeExternal;

  const newTab = opts.newTab;

  if (typeof newTab !== "function") {
    throw new Error("checkBrokenLinks requires newTab function");
  }

  const startUrl = buildStartUrl({
    domain: opts.domain,
    url: opts.url,
    startPath: opts.startPath
  });

  const rootHostname = new URL(startUrl).hostname;

  const discoveredMap = new Map();
  const pagesToVisit = [startUrl];
  const visitedPages = new Set();

  let page = null;
  let sourceStatus = "";
  let sourceError = "";

  try {
    page = await newTab();

    await page.setUserAgent(DEFAULT_USER_AGENT);
    await page.setViewport({
      width: 1366,
      height: 768,
      deviceScaleFactor: 1
    });

    while (pagesToVisit.length > 0 && visitedPages.size < maxPages) {
      const currentPageUrl = pagesToVisit.shift();
      if (!currentPageUrl || visitedPages.has(currentPageUrl)) continue;

      visitedPages.add(currentPageUrl);

      const openResult = await gotoWithFallback(page, currentPageUrl, timeoutMs, debug);

      if (!openResult.ok) {
        sourceStatus = "error";
        sourceError = openResult.error || "Unable to open source page";
        continue;
      }

      sourceStatus = "ok";

      const finalPageUrl = openResult.url || currentPageUrl;
      const pageLinks = await collectLinksFromPage(page, finalPageUrl, debug);

      for (const link of pageLinks) {
        const internal = isInternalUrl(link.url, rootHostname);

        if (!internal && !includeExternal) {
          continue;
        }

        if (!discoveredMap.has(link.url)) {
          discoveredMap.set(link.url, {
            url: link.url,
            text: link.text || "",
            source: link.source || finalPageUrl,
            internal
          });
        }

        if (
          internal &&
          isLikelyHtmlPage(link.url) &&
          !visitedPages.has(link.url) &&
          !pagesToVisit.includes(link.url) &&
          pagesToVisit.length + visitedPages.size < maxPages
        ) {
          pagesToVisit.push(link.url);
        }
      }
    }
  } catch (err) {
    sourceStatus = "error";
    sourceError = err.message || String(err);
  } finally {
    if (page) {
      try {
        await page.close();
      } catch (_) {}
    }
  }

  const linksToCheck = Array.from(discoveredMap.values()).slice(0, maxLinks);

  const checked = await mapLimit(linksToCheck, concurrency, async (item) => {
    const status = await checkUrlStatus(item.url, linkTimeoutMs, debug);

    return {
      url: item.url,
      finalUrl: status.finalUrl,
      status: status.status,
      state: status.state,
      text: item.text || "",
      source: item.source || "",
      internal: item.internal,
      error: status.error || ""
    };
  });

  const broken = checked.filter((x) => x.state === "broken");
  const warnings = checked.filter((x) => x.state === "warning");

  let finalStatus = "ok";

  if (sourceStatus === "error") {
    finalStatus = "error";
  } else if (broken.length > 0) {
    finalStatus = "has_broken_links";
  } else if (warnings.length > 0) {
    finalStatus = "has_warnings";
  }

  return {
    BrokenLinks_Status: finalStatus,
    BrokenLinks_Source: startUrl,
    BrokenLinks_Pages_Checked: visitedPages.size,
    BrokenLinks_Found_Count: discoveredMap.size,
    BrokenLinks_Checked_Count: checked.length,
    BrokenLinks_Broken_Count: broken.length,
    BrokenLinks_Warning_Count: warnings.length,
    BrokenLinks_Broken: broken,
    BrokenLinks_Warnings: warnings,
    BrokenLinks_Checked: checked,
    BrokenLinks_Broken_JSON: JSON.stringify(broken),
    BrokenLinks_Warnings_JSON: JSON.stringify(warnings),
    BrokenLinks_Checked_JSON: JSON.stringify(checked),
    BrokenLinks_Error: sourceError || ""
  };
}

module.exports = {
  checkBrokenLinks,
  buildStartUrl,
  normalizeDomain,
  normalizeHostname
};