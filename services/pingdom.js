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

    // Navigate to Pingdom
    await tab.goto('https://tools.pingdom.com/', {
      waitUntil: 'networkidle2',
      timeout: 60000,
    });

    await wait(3000);

    // Clear and enter domain
    await tab.click('input[placeholder="www.example.com"]');
    await tab.keyboard.down('Control');
    await tab.keyboard.press('A');
    await tab.keyboard.up('Control');
    await tab.keyboard.press('Backspace');
    await tab.keyboard.type(`http://${domain}`, { delay: 80 });
    
    // Click START TEST
    await tab.click('input[value="START TEST"]');

    console.log(`[pingdom] Waiting for test results for ${domain}...`);

    // Wait for test to complete - up to 3 minutes
    let testComplete = false;
    const maxWaitMs = 180000; // 3 minutes
    const startTime = Date.now();
    
    while (Date.now() - startTime < maxWaitMs && !testComplete) {
      await wait(5000);
      
      // Check if test is complete and has results
      const hasResults = await tab.evaluate(() => {
        const text = document.body.innerText || '';
        
        // Check for performance grade
        const hasGrade = text.includes('Performance grade') || 
                         text.includes('Grade') ||
                         /Performance\s+grade/i.test(text);
        
        // Check for load time
        const hasLoadTime = text.includes('Load time') || 
                           /Load\s+time/i.test(text);
        
        // Check for page size
        const hasPageSize = text.includes('Page size') ||
                           /Page\s+size/i.test(text);
        
        // Also check if DOWNLOAD HAR button appears (means test is complete)
        const hasDownloadHar = text.includes('DOWNLOAD HAR');
        
        return {
          hasResults: (hasGrade && hasLoadTime) || hasDownloadHar,
          text: text.substring(0, 500) // For debugging
        };
      }).catch(() => ({ hasResults: false, text: '' }));
      
      if (hasResults.hasResults) {
        testComplete = true;
        console.log(`[pingdom] Test completed for ${domain}`);
        break;
      }
      
      // Log progress every 30 seconds
      const elapsedSec = Math.round((Date.now() - startTime) / 1000);
      if (elapsedSec % 30 === 0 && elapsedSec > 0) {
        console.log(`[pingdom] Still waiting for results... (${elapsedSec}s)`);
      }
    }

    if (!testComplete) {
      console.log(`[pingdom] Test timeout for ${domain} after 3 minutes`);
    }

    // Wait a bit for final rendering
    await wait(3000);

    // Take screenshot
    await tab.screenshot({
      path: path.join(paths.imagesDir, 'pingdom.png'),
      fullPage: true,
    }).catch(() => {});

    // Extract results
    const pageText = await tab.evaluate(() => document.body.innerText || '');

    let grade = 'N/A', score = 'N/A', pageSize = 'N/A',
        loadTime = 'N/A', requests = 'N/A';

    // Parse performance grade
    const gradeMatch = pageText.match(/Performance\s+grade\s*([A-F][+-]?)(\d{1,3})?/i);
    if (gradeMatch) {
      grade = gradeMatch[1].toUpperCase();
      score = gradeMatch[2] || 'N/A';
    }
    
    // Alternative format
    if (grade === 'N/A') {
      const altMatch = pageText.match(/Grade\s*([A-F][+-]?)/i);
      if (altMatch) grade = altMatch[1].toUpperCase();
    }

    // Parse load time
    const loadMatch = pageText.match(/Load\s+time\s*([\d.]+)\s*(ms|s)/i);
    if (loadMatch) loadTime = `${loadMatch[1]} ${loadMatch[2]}`;

    // Parse page size
    const sizeMatch = pageText.match(/Page\s+size\s*([\d.]+)\s*(KB|MB)/i);
    if (sizeMatch) pageSize = `${sizeMatch[1]} ${sizeMatch[2].toUpperCase()}`;

    // Parse requests
    const reqMatch = pageText.match(/Requests\s*\n?\s*(\d+)/i);
    if (reqMatch) requests = reqMatch[1];

    return {
      status: 'SUCCESS',
      data: {
        performanceGrade: grade !== 'N/A' ? `${grade} ${score}`.trim() : 'N/A',
        gradeLetter: grade,
        gradeNumber: score !== 'N/A' ? parseInt(score) : null,
        loadTime,
        pageSize,
        requests,
      },
      error: null,
      errorCode: null,
      url: tab.url(),
      screenshot: 'pingdom.png',
    };

  } catch (err) {
    console.error(`[pingdom] Error for ${domain}:`, err.message);
    
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
        gradeLetter: 'N/A',
        gradeNumber: null,
        loadTime: 'N/A',
        pageSize: 'N/A',
        requests: 'N/A',
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
      url: `https://tools.pingdom.com/#${domain}`,
      screenshot: 'pingdom.png',
    };

  } finally {
    if (tab) await tab.close().catch(() => {});
  }
}

module.exports = { runPingdom };