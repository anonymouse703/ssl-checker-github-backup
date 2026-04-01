/**
 * utils/browser.js
 * Puppeteer browser management utilities
 * Fixed to prevent duplicate request interception errors
 */

'use strict';

const puppeteer = require('puppeteer');
const { resolveChromePath } = require('./chrome-path');

const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36';

async function launchBrowser() {
  const executablePath = resolveChromePath();

  if (!executablePath) {
    throw new Error(
      'Chrome executable not found. Set PUPPETEER_EXECUTABLE_PATH in .env or install Chrome.'
    );
  }

  console.log(`[browser] launching Chrome from: ${executablePath}`);

  return await puppeteer.launch({
    headless: true,
    executablePath,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-gpu',
      '--window-size=1280,900',
      '--disable-extensions',
      '--disable-background-networking',
      '--disable-sync',
      '--disable-translate',
      '--disable-default-apps',
      '--mute-audio',
      '--no-first-run',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding',
      '--js-flags=--max-old-space-size=128',
      '--disable-features=TranslateUI,BlinkGenPropertyTrees',
      // '--single-process',
    ],
    protocolTimeout: 600000,
    timeout: 600000
  });
}

async function createNewPage(browser) {
  const page = await browser.newPage();

  await page.setViewport({
    width: 1280,
    height: 900,
    deviceScaleFactor: 1
  });

  await page.setUserAgent(USER_AGENT);
  page.setDefaultNavigationTimeout(600000);
  page.setDefaultTimeout(600000);

  // Enable request interception ONCE
  await page.setRequestInterception(true);
  
  // Set a single request handler that blocks unnecessary resources
  page.on('request', (request) => {
    const url = request.url().toLowerCase();
    const resourceType = request.resourceType();

    // Block media files and tracking scripts
    if (
      resourceType === 'media' ||
      resourceType === 'font' ||
      url.includes('google-analytics.com') ||
      url.includes('googletagmanager.com') ||
      url.includes('doubleclick.net') ||
      url.includes('facebook.net') ||
      url.includes('clarity.ms') ||
      url.includes('hotjar.com')
    ) {
      request.abort();
      return;
    }

    // Allow all other requests
    request.continue();
  });

  return page;
}


async function createNewPageWithCustomInterception(browser, customFilter) {
  const page = await browser.newPage();

  await page.setViewport({
    width: 1280,
    height: 900,
    deviceScaleFactor: 1
  });

  await page.setUserAgent(USER_AGENT);
  page.setDefaultNavigationTimeout(600000);
  page.setDefaultTimeout(600000);

  // Enable request interception
  await page.setRequestInterception(true);
  
  // Set a single request handler with custom filtering
  page.on('request', (request) => {
    const url = request.url().toLowerCase();
    const resourceType = request.resourceType();

    // Default blocking
    if (
      resourceType === 'media' ||
      resourceType === 'font' ||
      url.includes('google-analytics.com') ||
      url.includes('googletagmanager.com') ||
      url.includes('doubleclick.net')
    ) {
      request.abort();
      return;
    }

    // Apply custom filter if provided
    if (customFilter && customFilter(request)) {
      request.abort();
      return;
    }

    request.continue();
  });

  return page;
}


module.exports = {
  launchBrowser,
  createNewPage,
  createNewPageWithCustomInterception
};