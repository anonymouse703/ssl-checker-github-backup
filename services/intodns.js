const { getErrorCode } = require('../utils/error-codes');
const path = require('path');

async function runIntoDNS(domain, context) {
  const { newTab, paths, wait } = context;
  const screenshotPath = path.join(paths.imagesDir, 'intodns.png');
  const tab = await newTab();

  try {
    const url = `https://intodns.com/${domain}`;
    await tab.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

    // Wait for the main content (table or overall health)
    await tab.waitForSelector('.overall, .check', { timeout: 30000 }).catch(() => {});

    // Allow extra time for dynamic content
    await wait(2000);

    // Extract data from the page
    const data = await tab.evaluate(() => {
      // Helper to get text from an element if exists
      const getText = (selector, parent = document) => {
        const el = parent.querySelector(selector);
        return el ? el.innerText.trim() : '';
      };

      // Overall Health
      let overallHealth = 'UNKNOWN';
      const healthEl = document.querySelector('.overall span');
      if (healthEl) {
        overallHealth = healthEl.innerText.trim().toUpperCase();
      } else {
        const text = getText('.overall');
        const match = text.match(/Overall Health:\s*(GOOD|WARN|ERROR)/i);
        if (match) overallHealth = match[1].toUpperCase();
      }

      // Collect all check rows
      const rows = Array.from(document.querySelectorAll('tr.check'));
      let errorCount = 0;
      let warnCount = 0;
      let mxStatus = 'UNKNOWN';
      let nsCount = 0;
      let soaSerial = '';

      for (const row of rows) {
        const nameCell = row.querySelector('td.name');
        const statusCell = row.querySelector('td.status span');
        const detailsCell = row.querySelector('td.details');

        const name = nameCell ? nameCell.innerText.trim() : '';
        const statusText = statusCell ? statusCell.innerText.trim().toUpperCase() : '';
        const details = detailsCell ? detailsCell.innerText.trim() : '';

        // Count errors/warnings
        if (statusText === 'ERROR') errorCount++;
        if (statusText === 'WARNING') warnCount++;

        // MX Status
        if (name === 'MX Records' || name === 'Mail Servers') {
          if (statusText === 'ERROR' && details.toLowerCase().includes('no mail configured')) {
            mxStatus = 'NO_MAIL';
          } else if (statusText === 'ERROR') {
            mxStatus = 'ERROR';
          } else if (statusText === 'WARNING') {
            mxStatus = 'WARNING';
          } else if (statusText === 'OK') {
            mxStatus = 'OK';
          } else {
            mxStatus = statusText || 'UNKNOWN';
          }
          // Optionally store the details for debugging
          // but we don't need them in the main fields.
        }

        // NS Count
        if (name === 'NS Records') {
          // Count lines that contain an IP or hostname pattern
          const lines = details.split('\n').filter(l => l.trim());
          nsCount = lines.length;
        }

        // SOA Serial
        if (name === 'SOA Record') {
          const match = details.match(/Serial:\s*(\d+)/i);
          if (match) soaSerial = match[1];
        }
      }

      // If no MX row found, maybe domain has no MX
      if (mxStatus === 'UNKNOWN' && errorCount === 0 && warnCount === 0) {
        mxStatus = 'NO_MAIL'; // fallback
      }

      return {
        overallHealth,
        errorCount,
        warnCount,
        mxStatus,
        nsCount,
        soaSerial,
      };
    });

    // Take screenshot
    await tab.evaluate(() => window.scrollTo(0, 0));
    await wait(500);
    await tab.screenshot({ path: screenshotPath, fullPage: true });

    return {
      status: 'SUCCESS',
      data,
      error: null,
      errorCode: null,
      url: `https://intodns.com/${domain}`,
      screenshot: 'intodns.png',
    };
  } catch (err) {
    // Fallback: try to take a screenshot anyway if possible
    let screenshot = null;
    try {
      await tab.evaluate(() => window.scrollTo(0, 0));
      await wait(500);
      await tab.screenshot({ path: screenshotPath, fullPage: true });
      screenshot = 'intodns.png';
    } catch (_) {}

    return {
      status: 'FAILED',
      data: {
        overallHealth: 'N/A',
        errorCount: 0,
        warnCount: 0,
        mxStatus: 'UNKNOWN',
        nsCount: 0,
        soaSerial: '',
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
      url: `https://intodns.com/${domain}`,
      screenshot,
    };
  } finally {
    await tab.close();
  }
}

module.exports = { runIntoDNS };