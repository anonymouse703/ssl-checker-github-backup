const { getErrorCode } = require('../utils/error-codes');
const path = require('path');
const {
  setFixedViewport,
  captureViewportWidthFullHeight,
  wait,
} = require('../utils/screenshot');

const INTODNS_VIEWPORT_WIDTH = 995;
const INTODNS_VIEWPORT_HEIGHT = 900;

async function runIntoDNS(domain, context) {
  const { newTab, paths } = context;
  const screenshotPath = path.join(paths.imagesDir, 'intodns.png');
  const tab = await newTab();

  const resultUrl = `https://intodns.com/${domain}`;

  async function waitForIntoDNSResults(page) {
    const maxAttempts = 8;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await page.waitForFunction(() => {
          const bodyText = document.body ? document.body.innerText : '';
          if (/Something wrong happend\. Please refresh the page!/i.test(bodyText)) {
            return false;
          }

          const rows = Array.from(document.querySelectorAll('table tr'));
          if (rows.length < 12) return false;

          const text = rows.map(r => (r.innerText || '').replace(/\s+/g, ' ').trim()).join('\n');

          return (
            /Parent/i.test(text) &&
            /NS/i.test(text) &&
            /SOA/i.test(text) &&
            (/TLD Parent Check/i.test(text) || /Nameservers A records/i.test(text)) &&
            (/NS records from your nameservers/i.test(text) || /Recursive Queries/i.test(text)) &&
            (/SOA record/i.test(text) || /SOA Serial/i.test(text))
          );
        }, { timeout: 10000 });

        return true;
      } catch (_) {
        if (attempt < maxAttempts) {
          await wait(2500);
          await page.reload({ waitUntil: 'domcontentloaded', timeout: 60000 }).catch(() => {});
        }
      }
    }

    return false;
  }

  try {
    await setFixedViewport(tab, INTODNS_VIEWPORT_WIDTH, INTODNS_VIEWPORT_HEIGHT);

    await tab.goto(resultUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

    const ready = await waitForIntoDNSResults(tab);
    if (!ready) {
      throw new Error('IntoDNS results did not fully load in time.');
    }

    await wait(2200);

    const data = await tab.evaluate(() => {
      const clean = (v) => (v || '').replace(/\s+/g, ' ').trim();

      const rows = Array.from(document.querySelectorAll('table tr'))
        .map((row) => {
          const cells = Array.from(row.querySelectorAll('td, th')).map(td => clean(td.innerText));
          return { cells, text: clean(row.innerText) };
        })
        .filter(r => r.cells.length > 0 || r.text);

      let overallHealth = 'UNKNOWN';
      let errorCount = 0;
      let warnCount = 0;
      let mxStatus = 'UNKNOWN';
      let nsCount = 0;
      let soaSerial = '';

      const fullText = clean(document.body ? document.body.innerText : '');

      if (/Overall Health:\s*GOOD/i.test(fullText)) overallHealth = 'GOOD';
      else if (/Overall Health:\s*WARN/i.test(fullText)) overallHealth = 'WARN';
      else if (/Overall Health:\s*ERROR/i.test(fullText)) overallHealth = 'ERROR';

      for (const row of rows) {
        const text = row.text;
        const lower = text.toLowerCase();

        if (/warning/i.test(text) || /warn/i.test(text)) warnCount++;
        if (/error/i.test(text) || /critical/i.test(text)) errorCount++;

        if (/mx records/i.test(text) || /mail servers/i.test(text)) {
          if (/no mail/i.test(lower) || /no mx/i.test(lower)) mxStatus = 'NO_MAIL';
          else if (/error/i.test(text)) mxStatus = 'ERROR';
          else if (/warning|warn/i.test(text)) mxStatus = 'WARNING';
          else mxStatus = 'OK';
        }

        if (/NS records from your nameservers/i.test(text) || /nameservers listed at the parent ns are/i.test(lower)) {
          const ips = text.match(/\b(?:\d{1,3}\.){3}\d{1,3}\b/g) || [];
          const hosts = text.match(/\b[a-z0-9.-]+\.[a-z]{2,}\b/gi) || [];
          const count = Math.max(ips.length, hosts.length);
          if (count > nsCount) nsCount = count;
        }

        if (/SOA/i.test(text) && /serial/i.test(lower)) {
          const m = text.match(/serial[:\s]+(\d{3,})/i);
          if (m) soaSerial = m[1];
        }
      }

      if (overallHealth === 'UNKNOWN') {
        if (errorCount > 0) overallHealth = 'ERROR';
        else if (warnCount > 0) overallHealth = 'WARN';
        else overallHealth = 'GOOD';
      }

      if (mxStatus === 'UNKNOWN') {
        if (/no mail configured|no mx records|no mail servers/i.test(fullText.toLowerCase())) {
          mxStatus = 'NO_MAIL';
        } else {
          mxStatus = 'OK';
        }
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

    await captureViewportWidthFullHeight(tab, screenshotPath, {
      width: INTODNS_VIEWPORT_WIDTH,
      left: 0,
      right: 0,
      top: 0,
      bottom: 0,
    });

    return {
      status: 'SUCCESS',
      data,
      error: null,
      errorCode: null,
      url: resultUrl,
      screenshot: 'intodns.png',
    };
  } catch (err) {
    let screenshot = null;

    try {
      await captureViewportWidthFullHeight(tab, screenshotPath, {
        width: INTODNS_VIEWPORT_WIDTH,
      });
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
      url: resultUrl,
      screenshot,
    };
  } finally {
    await tab.close().catch(() => {});
  }
}

module.exports = { runIntoDNS };