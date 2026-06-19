'use strict';

const { getErrorCode } = require('../utils/error-codes');
const path = require('path');
const {
  setFixedViewport,
  captureWithRetry,
  wait,
} = require('../utils/screenshot');

const DNS_VIEWPORT_WIDTH = 1080;
const DNS_VIEWPORT_HEIGHT = 935;
const DNS_TOP_CROP = 0;

async function runWhatsMyDNSOnce(domain, context) {
  const { newTab, paths } = context;

  const screenshotPath = path.join(paths.imagesDir, 'dns.png');
  const debugPath = path.join(paths.domainDir, 'dns-debug-notloaded.png');
  const tab = await newTab();

  async function setDnsViewport() {
    await setFixedViewport(tab, DNS_VIEWPORT_WIDTH, DNS_VIEWPORT_HEIGHT);
  }

  async function shootTab() {
    try {
      // Extra settle time — capture even when all results are X (failed)
      await wait(3000);

      await captureWithRetry(tab, screenshotPath, {
        width: DNS_VIEWPORT_WIDTH,
        left: 0,
        right: 0,
        top: DNS_TOP_CROP,
        bottom: 0,
        minBytes: 1000,
      }, 3);

      return 'dns.png';
    } catch (err) {
      console.error(`[dns] Screenshot failed: ${err.message}`);
      return null;
    }
  }

  async function waitThroughSecurityVerification(timeoutMs = 90000) {
    const start = Date.now();
    let reloads = 0;

    while (Date.now() - start < timeoutMs) {
      const state = await tab.evaluate(() => {
        const text = (document.body?.innerText || '').toLowerCase();
        const href = window.location.href || '';
        const hasInput = !!document.querySelector('input[type="text"], input[type="search"], input[name="domain"], input');
        const hasSearchButton = /search|lookup|check/i.test(document.body?.innerText || '');
        const verifying =
          text.includes('performing security verification') ||
          text.includes('security verification') ||
          text.includes('verifying') ||
          text.includes('checking if the site connection is secure') ||
          text.includes('cloudflare');
        return { href, hasInput, hasSearchButton, verifying };
      }).catch(() => ({ verifying: false, hasInput: false, hasSearchButton: false }));

      if (!state.verifying && (state.hasInput || state.hasSearchButton)) return true;

      if (state.verifying && reloads < 2 && Date.now() - start > (reloads + 1) * 25000) {
        reloads++;
        console.log(`[dns] Security verification still showing for ${domain}; reload ${reloads}/2`);
        await tab.reload({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
      }

      await wait(3000);
    }

    return false;
  }

  async function spinnerAppeared(timeoutMs = 12000) {
    try {
      await tab.waitForFunction(() => {
        const el = document.querySelector('svg[data-icon-loading]');
        return el && !el.classList.contains('hidden');
      }, { timeout: timeoutMs });
      return true;
    } catch (_) {
      return false;
    }
  }

  async function spinnerHidden(timeoutMs = 60000) {
    try {
      await tab.waitForFunction(() => {
        const el = document.querySelector('svg[data-icon-loading]');
        return !el || el.classList.contains('hidden');
      }, { timeout: timeoutMs });
      return true;
    } catch (_) {
      return false;
    }
  }

  async function hasRealDnsResults() {
    return await tab.evaluate(() => {
      const ipPattern = /\b\d{1,3}(?:\.\d{1,3}){3}\b/;
      const ipv6Pattern = /\b(?:[a-fA-F0-9]{1,4}:){2,7}[a-fA-F0-9]{1,4}\b/;
      const bodyText = document.body?.innerText || '';

      const resultSelectors = [
        '.dns-checker-result',
        'code.result',
        '.result-ip',
        '[data-id]',
        'td',
        'table tr'
      ];

      for (const sel of resultSelectors) {
        const nodes = document.querySelectorAll(sel);
        for (const node of nodes) {
          const text = node.innerText || '';
          if (ipPattern.test(text) || ipv6Pattern.test(text)) {
            return true;
          }
        }
      }

      const statusIcons = document.querySelectorAll(
        'svg[fill="#22cc22"], svg[fill="#cc2222"], .text-success, .text-danger, .status-ok, .status-fail'
      );
      // Lower threshold — even all-failed results (all X marks) should count as loaded
      if (statusIcons.length > 0) return true;

      if (/\b\d+\s*\/\s*\d+\b/.test(bodyText)) return true;

      return false;
    }).catch(() => false);
  }

  async function waitForDnsResults(timeoutMs = 60000) {
    const start = Date.now();

    while (Date.now() - start < timeoutMs) {
      const found = await tab.evaluate(() => {
        const ipPattern = /\b\d{1,3}(?:\.\d{1,3}){3}\b/;
        const ipv6Pattern = /\b(?:[a-fA-F0-9]{1,4}:){2,7}[a-fA-F0-9]{1,4}\b/;

        const loadingIcon = document.querySelector('svg[data-icon-loading]');
        const isSpinnerHidden = !loadingIcon || loadingIcon.classList.contains('hidden');

        if (!isSpinnerHidden) return false;

        const resultSelectors = [
          '.dns-checker-result',
          'code.result',
          '.result-ip',
          '[data-id]',
          'td',
          'table tr'
        ];

        for (const sel of resultSelectors) {
          const nodes = document.querySelectorAll(sel);
          for (const node of nodes) {
            const text = node.innerText || '';
            if (ipPattern.test(text) || ipv6Pattern.test(text)) {
              return true;
            }
          }
        }

        // Count status icons — even all-X results should trigger screenshot
        const statusIcons = document.querySelectorAll(
          'svg[fill="#22cc22"], svg[fill="#cc2222"], .text-success, .text-danger, .status-ok, .status-fail'
        );
        if (statusIcons.length > 0) return true;

        const bodyText = document.body?.innerText || '';
        if (/\b\d+\s*\/\s*\d+\b/.test(bodyText)) return true;

        return false;
      }).catch(() => false);

      if (found) return true;
      await wait(1500);
    }

    return false;
  }

  async function extractWhatsMyDnsData() {
    return await tab.evaluate(() => {
      const ipv4Pattern = /\b\d{1,3}(?:\.\d{1,3}){3}\b/g;
      const ipv6Pattern = /\b(?:[a-fA-F0-9]{1,4}:){2,7}[a-fA-F0-9]{1,4}\b/g;

      let rows = Array.from(document.querySelectorAll('[data-id]'));

      if (rows.length === 0) {
        const tables = document.querySelectorAll('table');
        for (const table of tables) {
          const tbody = table.querySelector('tbody');
          const tableRows = Array.from((tbody || table).querySelectorAll('tr'));
          if (tableRows.length > 3) {
            rows = tableRows;
            break;
          }
        }
      }

      let totalServers = 0;
      let propagated = 0;
      let failed = 0;

      for (const row of rows) {
        totalServers++;

        let status = 'UNKNOWN';

        const statusEl = row.querySelector('svg[fill], .status-ok, .status-fail, .text-success, .text-danger');
        if (statusEl) {
          const fill = statusEl.getAttribute?.('fill') || '';
          const cls = statusEl.classList?.toString() || '';

          if (fill === '#22cc22' || cls.includes('success') || cls.includes('ok')) {
            status = 'OK';
          } else if (fill === '#cc2222' || cls.includes('danger') || cls.includes('fail')) {
            status = 'FAIL';
          }
        }

        if (status === 'UNKNOWN') {
          const text = row.innerText || '';
          const hasIp = ipv4Pattern.test(text) || ipv6Pattern.test(text);
          if (hasIp) status = 'OK';
          else status = 'FAIL';
        }

        if (status === 'OK') propagated++;
        if (status === 'FAIL') failed++;
      }

      if (totalServers === 0) {
        const bodyText = document.body?.innerText || '';
        const m = bodyText.match(/(\d+)\s*\/\s*(\d+)/);
        if (m) {
          const ok = parseInt(m[1], 10);
          const total = parseInt(m[2], 10);
          return {
            totalServers: total,
            propagated: ok,
            failed: total - ok,
            propagationRate: total > 0 ? Math.round((ok / total) * 100) + '%' : '0%',
            note: 'Summary only'
          };
        }
      }

      return {
        totalServers,
        propagated,
        failed,
        propagationRate: totalServers > 0 ? Math.round((propagated / totalServers) * 100) + '%' : '0%',
        note: totalServers === 0 ? 'No records detected' : ''
      };
    });
  }

  async function runPrimary() {
    const targetUrl = `https://www.whatsmydns.net/#A/${domain}`;

    await setDnsViewport();

    await tab.goto(targetUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 30000
    });

    await waitThroughSecurityVerification(90000).catch(() => false);
    await tab.evaluate(() => window.scrollTo(0, 0)).catch(() => {});
    await wait(2500);

    await spinnerAppeared(8000).catch(() => {});
    await spinnerHidden(60000).catch(() => {});

    let loaded = await hasRealDnsResults();

    if (!loaded) {
      loaded = await waitForDnsResults(60000);
    }

    if (!loaded) {
      // Even if not "loaded", take a debug screenshot and continue
      console.log(`[dns] Results not fully detected for ${domain} — capturing anyway`);
      await captureWithRetry(tab, debugPath, {
        width: DNS_VIEWPORT_WIDTH,
        left: 0,
        right: 0,
        top: DNS_TOP_CROP,
        bottom: 0,
      }, 2).catch(() => {});
    }

    // Extra settle before scroll + screenshot
    await wait(3000);

    // Scroll to results area
    await tab.evaluate(() => {
      const candidates = Array.from(document.querySelectorAll('input, table, [data-id], .dns-checker-result'));
      const target =
        candidates.find(el => {
          const text = (el.innerText || el.value || '').toLowerCase();
          return text.includes('.') || text.includes('search') || el.matches('table, [data-id], .dns-checker-result');
        }) || document.querySelector('input');

      if (target) {
        target.scrollIntoView({ block: 'start', inline: 'nearest' });
        window.scrollBy(0, -120);
      } else {
        window.scrollTo(0, 0);
      }
    }).catch(() => {});

    await wait(1200);

    const data = await extractWhatsMyDnsData();
    const currentUrl = await tab.evaluate(() => window.location.href).catch(() => targetUrl);

    // Always attempt screenshot regardless of load status
    const screenshot = await shootTab();

    return {
      status: 'SUCCESS',
      data,
      error: null,
      errorCode: null,
      url: currentUrl || targetUrl,
      screenshot
    };
  }

  async function runFallback() {
    const fallbackUrl = `https://dnschecker.org/#A/${domain}`;

    await setDnsViewport();

    await tab.goto(fallbackUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 30000
    });

    await tab.evaluate(() => window.scrollTo(0, 0)).catch(() => {});
    await wait(8000);

    const altData = await tab.evaluate(() => {
      const rows = Array.from(document.querySelectorAll('table tr'));
      let propagated = 0;
      let failed = 0;
      let totalServers = 0;

      for (const row of rows) {
        const text = row.innerText || '';
        if (!text.trim()) continue;

        totalServers++;

        let status = 'UNKNOWN';
        if (row.querySelector('.text-success')) status = 'OK';
        else if (row.querySelector('.text-danger')) status = 'FAIL';

        if (status === 'OK') propagated++;
        else if (status === 'FAIL') failed++;
      }

      return {
        totalServers,
        propagated,
        failed,
        propagationRate: totalServers > 0 ? Math.round((propagated / totalServers) * 100) + '%' : '0%',
        note: 'Fallback source'
      };
    }).catch(() => ({
      totalServers: 0,
      propagated: 0,
      failed: 0,
      propagationRate: '0%',
      note: 'Fallback parse failed'
    }));

    const screenshot = await shootTab();

    return {
      status: altData.totalServers > 0 ? 'SUCCESS' : 'ERROR',
      data: altData,
      error: altData.totalServers > 0 ? null : 'Unable to load DNS results',
      errorCode: altData.totalServers > 0 ? null : getErrorCode('DNS_LOAD_FAILED'),
      url: fallbackUrl,
      screenshot
    };
  }

  try {
    const primary = await runPrimary();

    // Primary page can sometimes screenshot the input page before the result table is ready.
    // Accept it only when real resolver rows were parsed. Otherwise use fallback source.
    if (primary && primary.data && Number(primary.data.totalServers || 0) > 0) {
      return primary;
    }

    return await runFallback();
  } catch (error) {
    try {
      return await runFallback();
    } catch (fallbackError) {
      return {
        status: 'ERROR',
        data: null,
        error: fallbackError.message || error.message || 'DNS capture failed',
        errorCode: getErrorCode('DNS_CAPTURE_FAILED'),
        url: null,
        screenshot: null
      };
    }
  } finally {
    await tab.close().catch(() => {});
  }
}


async function runWhatsMyDNS(domain, context) {
  const maxRetries = parseInt(process.env.WHATSMYDNS_MAX_RETRIES || '5', 10);
  let lastResult = null;
  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      console.log(`[dns] Attempt ${attempt}/${maxRetries} for ${domain}`);
      const result = await runWhatsMyDNSOnce(domain, context);
      lastResult = result;

      const totalServers = Number(result?.data?.totalServers || 0);

      if (result && result.screenshot && totalServers > 0) {
        return result;
      }

      lastError = new Error(
        result?.error ||
        `DNS returned incomplete data: totalServers=${totalServers}, screenshot=${result?.screenshot || 'none'}`
      );
      console.error(`[dns] Attempt ${attempt} incomplete for ${domain}: ${lastError.message}`);
    } catch (err) {
      lastError = err;
      console.error(`[dns] Attempt ${attempt} error for ${domain}: ${err.message}`);
    }

    if (attempt < maxRetries) {
      await wait(attempt * 10000);
    }
  }

  if (lastResult) {
    return {
      ...lastResult,
      status: lastResult.status || 'SUCCESS',
      error: lastResult.error || `DNS screenshot missing after ${maxRetries} attempts`,
      errorCode: lastResult.errorCode || getErrorCode({ error: 'DNS_SCREENSHOT_MISSING' }),
      screenshot: null,
    };
  }

  return {
    status: 'ERROR',
    data: null,
    error: lastError?.message || 'DNS capture failed',
    errorCode: getErrorCode({ error: lastError?.message || 'DNS_CAPTURE_FAILED' }),
    url: null,
    screenshot: null,
  };
}

module.exports = { runWhatsMyDNS };