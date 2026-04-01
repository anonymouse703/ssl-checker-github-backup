const { getErrorCode } = require('../utils/error-codes');
const path = require('path');

async function runWhatsMyDNS(domain, context) {
  const { wait, newTab, paths } = context;

  const screenshotPath = path.join(paths.imagesDir, 'dns.png');
  const debugPath = path.join(paths.domainDir, 'dns-debug-notloaded.png');
  const tab = await newTab();

  async function shootTab() {
    try {
      await tab.evaluate(() => window.scrollTo(0, 0));
      await wait(500);
      await tab.screenshot({
        path: screenshotPath,
        fullPage: false
      });
      return 'dns.png';
    } catch (e) {
      return null;
    }
  }

  async function spinnerAppeared(timeoutMs = 12000) {
    try {
      await tab.waitForFunction(() => {
        const el = document.querySelector('svg[data-icon-loading]');
        return el && !el.classList.contains('hidden');
      }, { timeout: timeoutMs });
      return true;
    } catch (e) {
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
    } catch (e) {
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
      if (statusIcons.length > 3) return true;

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
        const spinnerHidden = !loadingIcon || loadingIcon.classList.contains('hidden');

        if (!spinnerHidden) return false;

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
        if (statusIcons.length > 3) return true;

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

      // Process rows incrementally without storing all records
      let totalServers = 0;
      let propagated = 0;
      let failed = 0;

      for (const row of rows) {
        totalServers++;
        
        let status = 'UNKNOWN';
        
        // Check status from SVG or class
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
        
        // Fallback to IP address detection
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

    await tab.goto(targetUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 30000
    });

    await wait(2500);

    await spinnerAppeared(8000).catch(() => {});
    await spinnerHidden(60000).catch(() => {});

    let loaded = await hasRealDnsResults();

    if (!loaded) {
      loaded = await waitForDnsResults(60000);
    }

    if (!loaded) {
      await tab.screenshot({
        path: debugPath,
        fullPage: false
      }).catch(() => {});
    }

    await wait(1500);

    const data = await extractWhatsMyDnsData();
    const currentUrl = await tab.evaluate(() => window.location.href).catch(() => targetUrl);
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

    await tab.goto(fallbackUrl, {
      waitUntil: 'domcontentloaded',
      timeout: 30000
    });

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
        else {
          const hasIp = /\b\d{1,3}(?:\.\d{1,3}){3}\b/.test(text);
          if (hasIp) status = 'OK';
          else status = 'FAIL';
        }

        if (status === 'OK') propagated++;
        if (status === 'FAIL') failed++;
      }

      return {
        totalServers,
        propagated,
        failed,
        propagationRate: totalServers > 0 ? Math.round((propagated / totalServers) * 100) + '%' : '0%',
        note: totalServers === 0 ? 'No records detected on fallback' : ''
      };
    });

    const screenshot = await shootTab();

    return {
      status: 'SUCCESS',
      data: altData,
      error: null,
      errorCode: null,
      url: fallbackUrl,
      screenshot
    };
  }

  try {
    return await runPrimary();
  } catch (err) {
    try {
      return await runFallback();
    } catch (altErr) {
      return {
        status: 'SUCCESS',
        data: {
          totalServers: 0,
          propagated: '?',
          failed: '?',
          propagationRate: 'N/A',
          note: 'DNS check unavailable'
        },
        error: err.message || altErr.message,
        errorCode: getErrorCode({ error: err.message || altErr.message }),
        url: `https://www.whatsmydns.net/#A/${domain}`,
        screenshot: null
      };
    }
  } finally {
    try {
      await tab.close().catch(() => {});
    } catch (_) {}
  }
}

module.exports = {
  runWhatsMyDNS
};