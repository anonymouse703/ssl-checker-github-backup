'use strict';

const { getErrorCode } = require('../utils/error-codes');
const { createNewPage } = require('../utils/browser');
const fs   = require('fs');
const path = require('path');

const USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

async function runPingdom(domain, context) {
  const { newTab, paths, wait } = context;
  const debugDir = paths.domainDir;
  let tab = null;

  try {
    tab = await newTab();
    await tab.setViewport({ width: 1280, height: 900 });
    await tab.setUserAgent(USER_AGENT);
    tab.setDefaultNavigationTimeout(300000);
    tab.setDefaultTimeout(300000);

    // Don't set up custom request interception - the one in createNewPage is sufficient
    // Just use the page as-is with default filtering from browser.js

    await tab.goto('https://tools.pingdom.com/', {
      waitUntil: 'networkidle2',
      timeout: 60000,
    });

    await wait(3000);

    await tab.click('input[placeholder="www.example.com"]');
    await tab.keyboard.down('Control');
    await tab.keyboard.press('A');
    await tab.keyboard.up('Control');
    await tab.keyboard.press('Backspace');
    await tab.keyboard.type(`http://${domain}`, { delay: 80 });
    await tab.click('input[value="START TEST"]');

    let resultsFound = false;
    for (let i = 1; i <= 24; i++) {
      await wait(5000);
      const found = await tab.evaluate(() => {
        const text = document.body.innerText || '';
        return (
          text.includes('Performance grade') ||
          text.includes('DOWNLOAD HAR') ||
          text.includes('Load time') ||
          text.includes('Page size')
        );
      }).catch(() => false);

      if (found) { resultsFound = true; break; }

      if (i % 6 === 0) {
        await tab.screenshot({
          path: path.join(debugDir, `pingdom-progress-${i * 5}s.png`)
        }).catch(() => {});
      }
    }

    await wait(3000);

    await tab.screenshot({
      path: path.join(paths.imagesDir, 'pingdom.png'),
      fullPage: true,
    }).catch(() => {});

    const pageText = await tab.evaluate(() => document.body.innerText || '');

    let grade = 'N/A', score = 'N/A', pageSize = 'N/A',
        loadTime = 'N/A', requests = 'N/A';

    const lines = pageText.split('\n').map(l => l.trim()).filter(Boolean);

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.toLowerCase() === 'performance grade') {
        const next = lines[i + 1] || '';
        const m = next.match(/^([A-F][+-]?)(\d{1,3})$/i);
        if (m) { grade = m[1].toUpperCase(); score = m[2]; }
        continue;
      }
      if (line.toLowerCase() === 'page size') {
        const next = lines[i + 1] || '';
        const m = next.match(/^([\d.]+)\s*(KB|MB)$/i);
        if (m) pageSize = `${m[1]} ${m[2].toUpperCase()}`;
        continue;
      }
      if (line.toLowerCase() === 'load time') {
        const next = lines[i + 1] || '';
        const m = next.match(/^([\d.]+)\s*(ms|s)$/i);
        if (m) loadTime = `${m[1]} ${m[2]}`;
        continue;
      }
      if (line.toLowerCase() === 'requests') {
        const next = lines[i + 1] || '';
        if (/^\d+$/.test(next)) requests = next;
        continue;
      }
    }

    if (grade === 'N/A') {
      const m = pageText.match(/Performance\s+grade\s*([A-F][+-]?)(\d{1,3})/i);
      if (m) { grade = m[1].toUpperCase(); score = m[2]; }
    }
    if (loadTime === 'N/A') {
      const m = pageText.match(/Load\s+time\s*([\d.]+)\s*(ms|s)/i);
      if (m) loadTime = `${m[1]} ${m[2]}`;
    }
    if (pageSize === 'N/A') {
      const m = pageText.match(/Page\s+size\s*([\d.]+)\s*(KB|MB)/i);
      if (m) pageSize = `${m[1]} ${m[2].toUpperCase()}`;
    }
    if (requests === 'N/A') {
      const m = pageText.match(/Requests\s*\n?\s*(\d+)/i);
      if (m) requests = m[1];
    }

    return {
      status: 'SUCCESS',
      data: {
        performanceGrade: grade !== 'N/A' ? `${grade} ${score}`.trim() : 'N/A',
        gradeLetter:  grade,
        gradeNumber:  score !== 'N/A' ? parseInt(score) : null,
        loadTime,
        pageSize,
        requests,
      },
      error:      null,
      errorCode:  null,
      url:        tab.url(),
      screenshot: 'pingdom.png',
    };

  } catch (err) {
    try {
      if (tab) {
        await tab.screenshot({
          path: path.join(paths.imagesDir, 'pingdom.png'),
          fullPage: true,
        });
        const html = await tab.content();
        fs.writeFileSync(path.join(debugDir, 'pingdom-error.html'), html);
      }
    } catch (_) {}

    return {
      status: 'SUCCESS',
      data: {
        performanceGrade: 'N/A',
        gradeLetter:  'N/A',
        gradeNumber:  null,
        loadTime:     'N/A',
        pageSize:     'N/A',
        requests:     'N/A',
      },
      error:      err.message,
      errorCode:  getErrorCode({ error: err.message }),
      url:        `https://tools.pingdom.com/#${domain}`,
      screenshot: 'pingdom.png',
    };

  } finally {
    if (tab) await tab.close().catch(() => {});
  }
}

module.exports = { runPingdom };