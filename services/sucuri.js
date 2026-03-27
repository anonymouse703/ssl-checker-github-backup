const { httpGet } = require('../utils/http');
const { getErrorCode } = require('../utils/error-codes');
const { ENDPOINTS } = require('../config/constants');
const path = require('path');

async function runSucuri(domain, context) {
  const { wait, newTab, paths } = context;
  const start = Date.now();

  try {
    const raw = await httpGet(`${ENDPOINTS.SUCURI_API}/?scan=${domain}`);

    if (!raw || raw.error)
      throw new Error(raw?.error || "No response");

    const scan = raw.scan || {};
    const site = scan.site || {};
    const blacklist = scan.blacklist || {};
    const malware = scan.malware || {};

    const isMalware =
      malware.malware_list?.length > 0 || malware.injected_spam?.length > 0;

    const isBlacklisted = Object.values(blacklist).some(
      (v) => v === true || v?.flagged === true,
    );

    const blacklistProviders = Object.entries(blacklist).map(
      ([provider, status]) => ({
        provider,
        flagged: status === true || status?.flagged === true || false,
      })
    );

    const blacklistedCount = blacklistProviders.filter((p) => p.flagged).length;

    const data = {
      overallStatus: isMalware ? "INFECTED" : isBlacklisted ? "BLACKLISTED" : "CLEAN",
      malware: {
        detected: isMalware,
        status: isMalware ? "Malware Found" : "No Malware Found",
        malwareList: malware.malware_list || [],
        injectedSpam: malware.injected_spam || [],
        iFrames: malware.iframes || [],
        links: malware.links || [],
        totalFound: (malware.malware_list?.length || 0) + (malware.injected_spam?.length || 0),
      },
      blacklist: {
        isBlacklisted,
        status: isBlacklisted
          ? ` Blacklisted on ${blacklistedCount} provider(s)`
          : " Not Blacklisted",
        totalProviders: blacklistProviders.length,
        blacklistedOn: blacklistedCount,
        providers: blacklistProviders,
      },
      site: {
        domain: site.domain || domain,
        ip: site.ip || null,
        cms: site.cms || null,
        server: site.server || null,
        php: site.php || null,
      },
      recommendations: (() => {
        const recs = raw.recommendations || {};
        const flat = [];
        Object.entries(recs).forEach(([category, items]) => {
          Object.entries(items).forEach(([key, details]) => {
            flat.push({ category, issue: key, details: typeof details === "object" ? details : {} });
          });
        });
        return flat;
      })(),
    };

    // ── Screenshot: open one tab to the results page and capture ─────────
    // The page loads fast because Sucuri caches the scan result.
    let screenshot = null;
    const tab = await newTab();
    try {
      await tab.goto(`https://sitecheck.sucuri.net/results/${domain}`, {
        waitUntil: "networkidle2",
        timeout: 60000,
      });
      // Wait for content — 15s max, then shoot whatever is there
      const deadline = Date.now() + 15000;
      while (Date.now() < deadline) {
        const found = await tab.$('.scan-results, .site-status, h2').catch(() => null);
        if (found) break;
        await wait(500);
      }
      await wait(1500);
      await tab.evaluate(() => window.scrollTo(0, 0));
      await tab.screenshot({
        path: path.join(paths.imagesDir, 'sucuri.png'),
        fullPage: true,
      });
      screenshot = 'sucuri.png';
    } catch (_) {
      // screenshot failed — non-fatal
    } finally {
      await tab.close();
    }

    return {
      status: "SUCCESS",
      data,
      error: null,
      errorCode: null,
      url: `https://sitecheck.sucuri.net/results/${domain}`,
      screenshot,
    };

  } catch (err) {
    console.error(`   🛡️ ${err.message} - using fallback data`);

    return {
      status: "SUCCESS",
      data: {
        overallStatus: "UNKNOWN",
        malware: { status: "⚠️ Scan Failed" },
        blacklist: { status: "⚠️ Scan Failed" },
        site: {},
        recommendations: [],
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
      url: `https://sitecheck.sucuri.net/results/${domain}`,
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

module.exports = { runSucuri };