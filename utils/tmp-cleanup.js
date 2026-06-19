/**
 * utils/tmp-cleanup.js
 * Cleans Puppeteer/Chrome temporary profile folders AND stale old Chrome
 * binary cache versions safely.
 */
'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');

const TMP_DIR = os.tmpdir();

// IMPORTANT: Puppeteer's OWN internal default temp-profile prefix is
// "puppeteer_dev_chrome_profile-" (used whenever a launch falls back to
// Puppeteer's built-in temp dir logic instead of our managed userDataDir).
// Our own managed dirs use "puppeteer_dev_profile-". We must recognize BOTH
// prefixes, or any profile created via the internal fallback path is
// invisible to cleanup forever.
const PROFILE_PREFIXES = [
  'puppeteer_dev_profile-',        // our own managed userDataDir prefix
  'puppeteer_dev_chrome_profile-', // Puppeteer's internal default prefix
  '.org.chromium.',
];

function nowMs() {
  return Date.now();
}

function isProfileName(name) {
  return PROFILE_PREFIXES.some((prefix) => String(name || '').startsWith(prefix));
}

function isSafeTmpProfileDir(dirPath) {
  if (!dirPath || typeof dirPath !== 'string') return false;

  const resolved = path.resolve(dirPath);
  const tmpResolved = path.resolve(TMP_DIR);
  const parent = path.dirname(resolved);
  const name = path.basename(resolved);

  return parent === tmpResolved && isProfileName(name);
}

function createManagedUserDataDir() {
  return fs.mkdtempSync(path.join(TMP_DIR, 'puppeteer_dev_profile-'));
}

function getActiveBrowserProfileDirs() {
  const active = new Set();

  let procEntries = [];
  try {
    procEntries = fs.readdirSync('/proc', { withFileTypes: true });
  } catch (_) {
    return active;
  }

  for (const entry of procEntries) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;

    try {
      const cmdline = fs.readFileSync(`/proc/${entry.name}/cmdline`, 'utf8').replace(/\0/g, ' ');
      if (!cmdline || !cmdline.includes('--user-data-dir=')) continue;

      const matches = cmdline.match(/--user-data-dir=([^\s]+)/g) || [];
      for (const match of matches) {
        const dir = match.replace(/^--user-data-dir=/, '').trim();
        if (isSafeTmpProfileDir(dir)) active.add(path.resolve(dir));
      }
    } catch (_) {
      // Process may exit while reading /proc. Ignore.
    }
  }

  return active;
}

function removeBrowserProfileDir(dirPath, options = {}) {
  const { force = false } = options;

  if (!isSafeTmpProfileDir(dirPath)) {
    return { removed: false, skipped: true, reason: 'unsafe path', path: dirPath };
  }

  const resolved = path.resolve(dirPath);

  if (!force) {
    const active = getActiveBrowserProfileDirs();
    if (active.has(resolved)) {
      return { removed: false, skipped: true, reason: 'active browser profile', path: resolved };
    }
  }

  try {
    fs.rmSync(resolved, { recursive: true, force: true, maxRetries: 3, retryDelay: 250 });
    return { removed: true, skipped: false, path: resolved };
  } catch (err) {
    return { removed: false, skipped: false, error: err.message, path: resolved };
  }
}

function cleanupStaleBrowserProfiles(options = {}) {
  const minAgeMs = Number.isFinite(Number(options.minAgeMs))
    ? Number(options.minAgeMs)
    : Number(process.env.PUPPETEER_PROFILE_CLEANUP_MINUTES || 30) * 60 * 1000;

  const active = getActiveBrowserProfileDirs();
  const cutoff = nowMs() - minAgeMs;

  const result = {
    tmpDir: TMP_DIR,
    checked: 0,
    removed: 0,
    skippedActive: 0,
    skippedYoung: 0,
    errors: [],
  };

  let entries = [];
  try {
    entries = fs.readdirSync(TMP_DIR, { withFileTypes: true });
  } catch (err) {
    result.errors.push(`Cannot read ${TMP_DIR}: ${err.message}`);
    return result;
  }

  for (const entry of entries) {
    if (!entry.isDirectory() || !isProfileName(entry.name)) continue;

    const fullPath = path.join(TMP_DIR, entry.name);
    if (!isSafeTmpProfileDir(fullPath)) continue;

    result.checked++;

    const resolved = path.resolve(fullPath);
    if (active.has(resolved)) {
      result.skippedActive++;
      continue;
    }

    let stat;
    try {
      stat = fs.statSync(resolved);
    } catch (_) {
      continue;
    }

    if (stat.mtimeMs > cutoff) {
      result.skippedYoung++;
      continue;
    }

    const removed = removeBrowserProfileDir(resolved, { force: true });
    if (removed.removed) {
      result.removed++;
    } else if (removed.error) {
      result.errors.push(`${resolved}: ${removed.error}`);
    }
  }

  return result;
}

// ── Old Chrome BINARY cache cleanup ──────────────────────────────────────────
//
// Every time Puppeteer auto-installs/updates Chrome, it leaves the PREVIOUS
// version's binary sitting in place (each one is 250-260MB). Over months of
// updates this silently eats multiple GB of disk per VM across these base
// directories:
//   - /usr/local/ind_leads/ssl-checker-tool/chrome
//   - /opt/puppeteer-cache/chrome
//   - ~/.cache/puppeteer/chrome   (commonly /root/.cache/puppeteer/chrome)
//
// chrome-path.js's resolveChromePath() always picks the NEWEST "linux-*"
// version under each base dir, so anything OLDER is dead weight that can be
// safely removed — unless a Chrome process happens to still be running from
// that exact path (defensive check below).

const CHROME_BINARY_BASE_DIRS = [
  process.env.CHROME_CACHE_DIR_1 || '/usr/local/ind_leads/ssl-checker-tool/chrome',
  process.env.CHROME_CACHE_DIR_2 || '/opt/puppeteer-cache/chrome',
  process.env.CHROME_CACHE_DIR_3 || path.join(os.homedir(), '.cache', 'puppeteer', 'chrome'),
];

function listVersionDirs(baseDir) {
  try {
    return fs.readdirSync(baseDir, { withFileTypes: true })
      .filter((d) => d.isDirectory() && d.name.startsWith('linux-'))
      .map((d) => d.name)
      .sort((a, b) => b.localeCompare(a, undefined, { numeric: true })); // newest first
  } catch (_) {
    return [];
  }
}

/**
 * Returns the set of Chrome executable paths currently running, resolved via
 * /proc/<pid>/exe, so we never delete a binary an active process still needs.
 */
function getActiveChromeExecutablePaths() {
  const active = new Set();

  let procEntries = [];
  try {
    procEntries = fs.readdirSync('/proc', { withFileTypes: true });
  } catch (_) {
    return active;
  }

  for (const entry of procEntries) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;
    try {
      const exePath = fs.readlinkSync(`/proc/${entry.name}/exe`);
      if (exePath && exePath.includes('/chrome')) {
        active.add(path.resolve(exePath));
      }
    } catch (_) {
      // Process may not have a readable exe symlink (permissions, exited, etc).
    }
  }

  return active;
}

/**
 * For each known Chrome binary cache base dir, keep ONLY the newest
 * "linux-*" version folder and remove all older ones.
 *
 * @returns {{ checked: number, removed: string[], skippedActive: string[], skippedNewest: string[], freedApproxMB: number, errors: string[] }}
 */
function cleanupOldChromeBinaries(options = {}) {
  const baseDirs = options.baseDirs || CHROME_BINARY_BASE_DIRS;
  const activeExePaths = getActiveChromeExecutablePaths();

  const result = {
    checked: 0,
    removed: [],
    skippedActive: [],
    skippedNewest: [],
    freedApproxMB: 0,
    errors: [],
  };

  for (const baseDir of baseDirs) {
    const versions = listVersionDirs(baseDir);
    if (versions.length === 0) continue;

    const newest = versions[0];

    for (const version of versions) {
      result.checked++;
      const versionPath = path.join(baseDir, version);

      if (version === newest) {
        result.skippedNewest.push(versionPath);
        continue;
      }

      const chromeBinary = path.join(versionPath, 'chrome-linux64', 'chrome');
      if (activeExePaths.has(path.resolve(chromeBinary))) {
        result.skippedActive.push(versionPath);
        continue;
      }

      // Estimate size before removing, for visibility in logs/reports.
      let approxMB = 0;
      try {
        const stat = fs.statSync(chromeBinary);
        approxMB = Math.round(stat.size / (1024 * 1024));
      } catch (_) {}

      try {
        fs.rmSync(versionPath, { recursive: true, force: true, maxRetries: 3, retryDelay: 250 });
        result.removed.push(versionPath);
        result.freedApproxMB += approxMB;
      } catch (err) {
        result.errors.push(`${versionPath}: ${err.message}`);
      }
    }
  }

  return result;
}

module.exports = {
  TMP_DIR,
  createManagedUserDataDir,
  removeBrowserProfileDir,
  cleanupStaleBrowserProfiles,
  getActiveBrowserProfileDirs,
  cleanupOldChromeBinaries,
  getActiveChromeExecutablePaths,
  CHROME_BINARY_BASE_DIRS,
};