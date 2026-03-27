'use strict';

const fs = require('fs');
const path = require('path');

function exists(p) {
  try {
    return !!p && fs.existsSync(p);
  } catch (_) {
    return false;
  }
}

function findNewestBundledChrome(baseDir) {
  if (!exists(baseDir)) return null;

  const entries = fs.readdirSync(baseDir, { withFileTypes: true })
    .filter(d => d.isDirectory() && d.name.startsWith('linux-'))
    .map(d => d.name)
    .sort((a, b) => b.localeCompare(a, undefined, { numeric: true }));

  for (const dir of entries) {
    const candidate = path.join(baseDir, dir, 'chrome-linux64', 'chrome');
    if (exists(candidate)) return candidate;
  }

  return null;
}

function resolveChromePath() {
  const explicit =
    process.env.PUPPETEER_EXECUTABLE_PATH ||
    process.env.CHROME_BIN ||
    process.env.CHROME_PATH;

  if (exists(explicit)) return explicit;

  const bundledProjectChrome = findNewestBundledChrome(
    '/usr/local/ind_leads/ssl-checker-tool/chrome'
  );
  if (bundledProjectChrome) return bundledProjectChrome;

  const bundledCacheChrome = findNewestBundledChrome(
    '/opt/puppeteer-cache/chrome'
  );
  if (bundledCacheChrome) return bundledCacheChrome;

  const commonSystemPaths = [
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium-browser',
    '/usr/bin/chromium'
  ];

  for (const p of commonSystemPaths) {
    if (exists(p)) return p;
  }

  return null;
}

module.exports = { resolveChromePath };