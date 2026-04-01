const { httpGet } = require('../utils/http');
const { getErrorCode } = require('../utils/error-codes');
const { TIMEOUTS, GRADE_ORDER, ENDPOINTS, SSL_LABS } = require('../config/constants');
const path = require('path');

async function runSSLLabs(domain, context) {
  const { wait, newTab, DEBUG, paths, sslDelay } = context;
  const start = Date.now();

  if (sslDelay > 0) {
    await wait(sslDelay);
  }

  const screenshotPath = path.join(paths.imagesDir, 'ssl.png');

  // ── Capacity error detection ──────────────────────────────────────────────
  const isCapacityError = (d) => {
    if (!d || typeof d !== "object") return false;
    if (Array.isArray(d.errors) && d.errors.some(e =>
      typeof e.message === "string" && (
        e.message.toLowerCase().includes("capacity") ||
        e.message.toLowerCase().includes("try again") ||
        e.message.toLowerCase().includes("overloaded") ||
        e.message.toLowerCase().includes("too many")
      )
    )) return true;
    if (d.status === "ERROR" && typeof d.statusMessage === "string" && (
      d.statusMessage.toLowerCase().includes("capacity") ||
      d.statusMessage.toLowerCase().includes("try again")
    )) return true;
    if (typeof d.message === "string" && (
      d.message.toLowerCase().includes("capacity") ||
      d.message.toLowerCase().includes("try again")
    )) return true;
    return false;
  };

  // ── API fetch with retry ──────────────────────────────────────────────────
  const sslFetch = async (url, label, maxRetries = SSL_LABS.MAX_RETRIES) => {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      let data;
      try {
        data = await httpGet(url);
      } catch (e) {
        if (attempt < maxRetries) { await wait(10000); continue; }
        return null;
      }
      if (DEBUG) {
        const fs = require('fs');
        fs.writeFileSync(
          path.join(paths.domainDir, `ssllabs-${label}-attempt${attempt}.json`),
          JSON.stringify(data, null, 2)
        );
      }
      if (!isCapacityError(data)) return data;
      const waitMs = Math.min(SSL_LABS.BASE_RETRY_DELAY * Math.pow(1.5, attempt - 1), 60000);
      if (attempt < maxRetries) await wait(waitMs);
    }
    return null;
  };

  // ── Parse a READY API response ────────────────────────────────────────────
  const parseReady = (sslData) => {
    const endpoints = (sslData.endpoints || []).map(ep => ({
      ipAddress: ep.ipAddress,
      grade: ep.grade,
      hasWarnings: ep.hasWarnings,
      isExceptional: ep.isExceptional,
      statusMessage: ep.statusMessage,
    }));
    const overallGrade = endpoints.length > 0
      ? endpoints.reduce((worst, ep) =>
          GRADE_ORDER.indexOf(ep.grade) > GRADE_ORDER.indexOf(worst) ? ep.grade : worst,
          endpoints[0].grade)
      : "N/A";
    return {
      overallGrade,
      host: sslData.host,
      status: sslData.status,
      endpoints,
      summary: {
        totalEndpoints: endpoints.length,
        allGrades: [...new Set(endpoints.map(e => e.grade))].join(", "),
        anyWarnings: endpoints.some(e => e.hasWarnings),
        anyExceptional: endpoints.some(e => e.isExceptional),
      },
    };
  };

  // ── Shoot the current tab ─────────────────────────────────────────────────
  async function shootTab(tab) {
    try {
      await tab.evaluate(() => window.scrollTo(0, 0));
      await wait(1000);
      await tab.screenshot({ path: screenshotPath, fullPage: true });
      return 'ssl.png';
    } catch (_) { return null; }
  }

  // ── API polling loop — resolves early once ANY endpoint has a grade ──────
  //
  // For CSV/JSON we only need the best available grade — we don't have to wait
  // for every server to finish.  As soon as at least one endpoint has a real
  // grade letter we parse what we have and return it.  The screenshot path
  // (screenshotViaFreshTab) runs independently and keeps waiting for the full
  // page to be complete before capturing.
  //
  // "Early" grade logic:
  //   - status === "READY"            → all done, use as-is
  //   - status === "IN_PROGRESS" AND at least one endpoint has a grade letter
  //     → return a partial result flagged with earlyGrade: true
  //     → overall grade is computed from graded endpoints only (worst grade wins)
  //     → un-graded endpoints are included with grade "PENDING"
  async function pollApiUntilReady(maxMs = 8 * 60 * 1000) {
    let sslData = await sslFetch(
      `${ENDPOINTS.SSL_LABS_API}?host=${domain}&startNew=on&all=done`,
      "startnew", 3
    );

    if (!sslData || sslData.status === null) return null;

    const deadline = Date.now() + maxMs;
    let apiAttempts = 0;

    // Helper: does this API response have at least one graded endpoint?
    const hasAnyGrade = (d) => {
      const eps = d.endpoints || [];
      return eps.some(ep => ep.grade && /^(A\+|A-|A|B|C|D|E|F|T|M)$/.test(ep.grade));
    };

    // Helper: parse a possibly-partial IN_PROGRESS response
    const parsePartial = (d) => {
      const endpoints = (d.endpoints || []).map(ep => ({
        ipAddress: ep.ipAddress,
        grade: ep.grade || "PENDING",
        hasWarnings: ep.hasWarnings,
        isExceptional: ep.isExceptional,
        statusMessage: ep.statusMessage,
      }));
      const gradedOnly = endpoints.filter(ep =>
        /^(A\+|A-|A|B|C|D|E|F|T|M)$/.test(ep.grade)
      );
      const overallGrade = gradedOnly.length > 0
        ? gradedOnly.reduce((worst, ep) =>
            GRADE_ORDER.indexOf(ep.grade) > GRADE_ORDER.indexOf(worst) ? ep.grade : worst,
            gradedOnly[0].grade)
        : "N/A";
      return {
        overallGrade,
        host: d.host,
        status: d.status,
        earlyGrade: true,          // flag so callers know this is a partial result
        endpoints,
        summary: {
          totalEndpoints: endpoints.length,
          gradedEndpoints: gradedOnly.length,
          allGrades: [...new Set(gradedOnly.map(e => e.grade))].join(", ") || "PENDING",
          anyWarnings: endpoints.some(e => e.hasWarnings),
          anyExceptional: endpoints.some(e => e.isExceptional),
        },
      };
    };

    // Check the very first response immediately before entering the loop
    if (sslData.status === "READY") return parseReady(sslData);
    if (sslData.status === "IN_PROGRESS" && hasAnyGrade(sslData)) {
      return parsePartial(sslData);
    }

    while (
      sslData &&
      sslData.status !== "READY" &&
      sslData.status !== "ERROR" &&
      Date.now() < deadline
    ) {
      apiAttempts++;

      await wait(TIMEOUTS.SSL_API_POLL); // 20s between polls

      sslData = await sslFetch(
        `${ENDPOINTS.SSL_LABS_API}?host=${domain}&all=done`,
        `poll${apiAttempts}`, 2
      );

      if (!sslData) break;

      // Full result — all servers done
      if (sslData.status === "READY") return parseReady(sslData);

      // ✅ Early exit — at least one server has a grade, don't wait for the rest
      if (sslData.status === "IN_PROGRESS" && hasAnyGrade(sslData)) {
        return parsePartial(sslData);
      }
    }

    // Last chance: fully READY
    if (sslData && sslData.status === "READY") return parseReady(sslData);

    return null;
  }

  // ── Shared "is this SSL Labs page fully done?" evaluator ─────────────────
  //
  // Called inside page.evaluate() — runs in the browser context.
  // Returns one of: "ready" | "nossl" | "dns_fail" | "failed" | "error" | "pending"
  //
  // KEY RULE: "Please wait..." banner still visible = NOT done, even if a
  // grade or certificate section is already partially rendered on the page.
  // SSL Labs renders sections incrementally while scanning, so a visible
  // grade does NOT mean the scan is complete.  We must confirm the banner
  // is gone AND at least one of the definitive "done" signals is present.
  //
  // Definitive "done" signals (scan 100% finished):
  //   1. "Miscellaneous" section — the very last section SSL Labs renders
  //   2. "Overall Rating" summary card visible AND no progress banner
  //   3. #endpointData rows present AND no progress banner
  const SSL_PAGE_STATE_FN = () => {
    const bodyText  = document.body ? document.body.innerText : "";
    const lowerBody = bodyText.toLowerCase();

    // ── STEP 1: Check for "still scanning" banner FIRST ──────────────────
    // This must be checked before any "done" signal — the banner can appear
    // alongside partial results (certificate section, grade letter, etc.)
    // while the scan is still running (e.g. "Please wait... 61% complete").
    const progressBanner = document.querySelector(
      "#progress, .progress, #warningBox[id='warningBox']"
    );
    const bannerText = progressBanner ? progressBanner.innerText : "";
    const isScanning =
      /please wait/i.test(bannerText) ||
      /\d+%\s*complete/i.test(bannerText) ||
      /please wait/i.test(bodyText) ||
      /\d+%\s*complete/i.test(bodyText) ||
      /assessment in progress/i.test(bodyText) ||
      /calculating/i.test(bodyText) ||
      /testing tls/i.test(bodyText) ||
      /testing protocol/i.test(bodyText) ||
      /determining available/i.test(bodyText) ||   // "Determining available cipher suites"
      /queued/i.test(bodyText);

    if (isScanning) return "pending";  // still running — do not accept partial results

    // ── STEP 2: Check for terminal error states ───────────────────────────
    const errBox = document.querySelector("#errorbox");
    if (errBox) {
      const t = errBox.innerText.toLowerCase();
      if (t.includes("no secure protocols"))   return "nossl";
      if (t.includes("dns resolution failed")) return "dns_fail";
      if (t.includes("assessment failed"))     return "failed";
      return "error";
    }
    if (lowerBody.includes("assessment failed"))     return "failed";
    if (lowerBody.includes("unable to connect"))     return "failed";
    if (lowerBody.includes("no secure protocols"))   return "nossl";
    if (lowerBody.includes("dns resolution failed")) return "dns_fail";

    // ── STEP 3: "Done" signals — only reached if no progress banner ───────
    // Signal A (strongest): Miscellaneous section — the last thing SSL Labs
    // renders.  Its presence guarantees the full report is on the page.
    const hasMisc = /Miscellaneous/i.test(bodyText);
    if (hasMisc) return "ready";

    // Signal B: Summary card with Overall Rating + endpoint rows present
    const hasOverallRating = /Overall Rating/i.test(bodyText);
    const hasEndpointRows  = document.querySelectorAll("#endpointData tr").length > 0;
    if (hasOverallRating && hasEndpointRows) return "ready";

    // Signal C: Summary card with grade + chart bars rendered
    // (.chartBar_g is only added by JS after the scan completes)
    const hasChartBars = document.querySelectorAll(".chartBar_g").length >= 3;
    const hasGrade     = /Overall Rating/i.test(bodyText);
    if (hasChartBars && hasGrade) return "ready";

    return "pending";
  };

  // ── Browser scraper — opens tab, waits for scan to finish ────────────────
  // Returns { overallGrade, endpoints, summary, screenshot } or throws.
  async function runSSLLabsBrowser() {
    const tab = await newTab();
    try {
      const url = `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}&hideResults=on&latest`;
      await tab.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

      const deadline  = Date.now() + TIMEOUTS.SSL_BROWSER; // 10 min
      let pageState   = "pending";
      let elapsedSec  = 0;

      while (Date.now() < deadline) {
        await wait(SSL_LABS.POLL_INTERVAL); // 8s
        elapsedSec += SSL_LABS.POLL_INTERVAL / 1000;

        pageState = await tab.evaluate(SSL_PAGE_STATE_FN).catch(() => "pending");

        if (pageState !== "pending") break;

        if (DEBUG && elapsedSec % 60 === 0) {
          try {
            const fs = require('fs');
            await tab.screenshot({
              path: path.join(paths.domainDir, `ssl-debug-${elapsedSec}s.png`)
            });
          } catch (_) {}
        }
      }

      if (pageState === "pending") {
        try {
          const fs = require('fs');
          await tab.screenshot({ path: path.join(paths.domainDir, "ssllabs-timeout.png") });
          fs.writeFileSync(path.join(paths.domainDir, "ssllabs-timeout.html"), await tab.content());
        } catch (_) {}
        throw new Error(`Browser scan timed out after ${TIMEOUTS.SSL_BROWSER / 60000} min`);
      }

      if (pageState === "nossl") {
        const screenshot = await shootTab(tab);
        return {
          overallGrade: "F", source: "browser", screenshot,
          endpoints: [],
          summary: { totalEndpoints: 0, allGrades: "F", anyWarnings: true, anyExceptional: false },
        };
      }

      if (pageState === "dns_fail") {
        throw new Error("DNS resolution failed — domain may not exist");
      }

      await wait(2000);

      const scraped = await tab.evaluate(() => {
        const isGrade = t => /^(A\+|A-|A|B|C|D|E|F|T|M)$/.test((t || "").trim());
        const endpoints = [];
        for (const row of document.querySelectorAll("#endpointData tr")) {
          const tds = Array.from(row.querySelectorAll("td"));
          if (tds.length < 2) continue;
          const ip = tds[0]?.innerText?.trim() || "";
          let grade = tds[1]?.innerText?.trim()?.split("\n")[0]?.trim() || "";
          const gradeSpan = tds[1]?.querySelector(".grade, .rating, .score");
          if (gradeSpan) grade = gradeSpan.innerText.trim();
          if (ip && isGrade(grade)) endpoints.push({ ipAddress: ip, grade });
        }
        let ratingGrade = null;
        const rb = document.querySelector("#rating");
        if (rb) { const m = rb.innerText.match(/(A\+|A-|A|B|C|D|E|F|T|M)/); if (m) ratingGrade = m[1]; }
        let pageGrade = null;
        if (!ratingGrade && endpoints.length === 0) {
          const body = document.body ? document.body.innerText : "";
          for (const pat of [
            /Overall Rating\s*[:\-]?\s*(A\+|A-|A|B|C|D|E|F|T|M)/i,
            /Grade\s*[:\-]?\s*(A\+|A-|A|B|C|D|E|F|T|M)/i,
            /Rating\s*[:\-]?\s*(A\+|A-|A|B|C|D|E|F|T|M)/i,
          ]) { const m = body.match(pat); if (m) { pageGrade = m[1]; break; } }
        }
        return { endpoints, ratingGrade, pageGrade };
      });

      let overallGrade = "N/A";
      if (scraped.endpoints.length > 0) {
        overallGrade = scraped.endpoints.reduce(
          (worst, ep) => GRADE_ORDER.indexOf(ep.grade) > GRADE_ORDER.indexOf(worst) ? ep.grade : worst,
          scraped.endpoints[0].grade
        );
      } else if (scraped.ratingGrade) {
        overallGrade = scraped.ratingGrade;
      } else if (scraped.pageGrade) {
        overallGrade = scraped.pageGrade;
      }

      const screenshot = await shootTab(tab);

      return {
        overallGrade, source: "browser", screenshot,
        endpoints: scraped.endpoints,
        summary: {
          totalEndpoints: scraped.endpoints.length,
          allGrades: [...new Set(scraped.endpoints.map(e => e.grade))].join(", ") || overallGrade,
          anyWarnings: false,
          anyExceptional: false,
        },
      };
    } catch (err) {
      throw new Error(`Browser scrape failed: ${err.message}`);
    } finally {
      await tab.close();
    }
  }

  // ── Screenshot via a fresh tab (used when API returned the result) ─────────
  // Uses fromCache=on so SSL Labs loads the already-finished result instead
  // of triggering a new scan.  Waits up to 3 min using the same SSL_PAGE_STATE_FN
  // shared with runSSLLabsBrowser() to confirm the page is fully rendered
  // before capturing — the "Please wait" check prevents shooting mid-scan.
  async function screenshotViaFreshTab() {
    const tab = await newTab();
    try {
      // fromCache=on  → load the cached (already-done) result, no new scan
      // maxAge=1      → accept a result up to 1 hour old (just finished moments ago)
      await tab.goto(
        `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}&fromCache=on&maxAge=1`,
        { waitUntil: "domcontentloaded", timeout: 60000 }
      );

      // When the API returns an early (partial) grade, the full page may still
      // be scanning.  Give up to 10 min for all servers to complete so the
      // screenshot captures the full report with every server's grade visible.
      const deadline = Date.now() + 10 * 60 * 1000;
      let isReady = false;

      while (Date.now() < deadline) {
        await wait(3000);

        const state = await tab.evaluate(SSL_PAGE_STATE_FN).catch(() => "pending");

        // Any terminal state (ready / error / nossl / etc.) means we're done waiting
        if (state !== "pending") { isReady = true; break; }

        // Reload to prod the SPA if still showing progress
        try { await tab.reload({ waitUntil: "domcontentloaded", timeout: 30000 }); } catch (_) {}
      }

      // Final paint settle before capture
      await wait(2000);
      await tab.evaluate(() => window.scrollTo(0, 0));
      await tab.screenshot({ path: screenshotPath, fullPage: true });
      return "ssl.png";

    } catch (_) { return null; }
    finally { await tab.close(); }
  }

  // ════════════════════════════════════════════════════════════════════════════
  // MAIN FLOW
  // ════════════════════════════════════════════════════════════════════════════
  try {

    // ── Step 1: Try cache (instant if recently scanned) ───────────────────────
    let cached = await sslFetch(
      `${ENDPOINTS.SSL_LABS_API}?host=${domain}&fromCache=on&maxAge=24&all=done`,
      "cache", 3
    );

    if (cached && cached.status === "READY") {
      const data = parseReady(cached);
      const screenshot = await screenshotViaFreshTab();
      return {
        status: "SUCCESS", data,
        error: null, errorCode: null,
        url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
        screenshot,
      };
    }

    // ── Step 2: API polling + browser scraper in parallel ─────────────────────
    //
    // pollApiUntilReady now resolves as soon as ANY endpoint has a grade —
    // it no longer waits for all servers to finish.  This means we can write
    // the CSV grade immediately without blocking on the full scan completion.
    //
    // The screenshot always needs the full page (all servers done, no banner),
    // so screenshotViaFreshTab() has its own independent wait of up to 10 min.
    //
    // Strategy:
    //   • API returns early grade  → write CSV now, fire screenshot in background
    //   • Browser scraper wins     → it already waited for full completion,
    //                                so its screenshot is already the full page
    const result = await Promise.any([

      // Path A: API polling — resolves early once any endpoint has a grade
      pollApiUntilReady(8 * 60 * 1000).then(data => {
        if (!data) throw new Error("API polling returned no result");
        return { source: "api", data };
      }),

      // Path B: Browser scraper (up to 10 min) — waits for full page
      runSSLLabsBrowser().then(browserData => {
        return { source: "browser", data: browserData };
      }),

    ]);

    if (result.source === "api") {
      // ✅ API won — we have the grade (possibly early/partial).
      // Return the grade immediately, capture screenshot in background
      const screenshotPromise = screenshotViaFreshTab().catch(() => null);
      
      // Don't await the screenshot - let it run in background
      screenshotPromise.then(screenshot => {
        // Update the screenshot file when ready
        console.log(`[ssl-labs] Screenshot captured for ${domain}: ${screenshot || 'failed'}`);
      });
      
      // Return immediately with grade
      return {
        status: "SUCCESS",
        data: result.data,
        error: null, errorCode: null,
        url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
        screenshot: null,  // Initially null, will be updated later when file exists
      };
    } else {
      // Browser scraper won — it already waited for the full page, screenshot included
      return {
        status: "SUCCESS",
        data: result.data,
        error: null, errorCode: null,
        url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
        screenshot: result.data.screenshot || null,
      };
    }

  } catch (err) {
    const msg = err instanceof AggregateError
      ? err.errors.map(e => e.message).join(" | ")
      : err.message;

    return {
      status: "FAILED",
      data: { overallGrade: "N/A", error: msg },
      error: msg,
      errorCode: getErrorCode({ error: msg }),
      url: `${ENDPOINTS.SSL_LABS_WEB}?d=${domain}`,
      screenshot: null,
    };
  }
}

function formatDuration(ms) {
  const totalSec = Math.round(ms / 1000);
  const mins = Math.floor(totalSec / 60);
  const secs = totalSec % 60;
  return mins === 0 ? `${secs} sec` : `${mins} min ${secs} sec`;
}

module.exports = { runSSLLabs };