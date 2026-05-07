// utils/scan-usage.js
'use strict';

/**
 * Persists scan usage counters to disk so the /scan-capacity endpoint
 * can enforce minutes_gap, domains_per_hour, and domains_per_day limits
 * across restarts and concurrent requests.
 *
 * Storage: OUTPUT_DIR/scan_usage.json
 *
 * Schema:
 * {
 *   last_scan_at:       ISO string | null   — timestamp of the most recent scan start
 *   hour_window_start:  ISO string | null   — when the current 60-min window began
 *   hour_count:         number              — domains started in this 60-min window
 *   day_date:           "YYYY-MM-DD" | null — the local calendar date of day_count
 *   day_count:          number              — domains started today
 * }
 */

const fs   = require('fs');
const path = require('path');

const OUTPUT_DIR  = process.env.OUTPUT_DIR || '/home/ind/ind_leads_inputs';
const USAGE_FILE  = path.join(OUTPUT_DIR, 'scan_usage.json');

// ── File helpers ──────────────────────────────────────────────────────────────

function ensureDir(dir) {
  try { fs.mkdirSync(dir, { recursive: true }); } catch (_) {}
}

function readRaw() {
  try {
    return JSON.parse(fs.readFileSync(USAGE_FILE, 'utf8'));
  } catch (_) {
    return {};
  }
}

function writeRaw(data) {
  ensureDir(OUTPUT_DIR);
  fs.writeFileSync(USAGE_FILE, JSON.stringify(data, null, 2), 'utf8');
}

// ── Date helpers ──────────────────────────────────────────────────────────────

function todayYmd() {
  const d = new Date();
  return (
    d.getFullYear() +
    '-' + String(d.getMonth() + 1).padStart(2, '0') +
    '-' + String(d.getDate()).padStart(2, '0')
  );
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Read current usage, auto-resetting the hourly window and daily counter
 * when their periods have expired.
 *
 * @returns {{
 *   last_scan_at:      string|null,
 *   hour_window_start: string|null,
 *   hour_count:        number,
 *   day_date:          string,
 *   day_count:         number
 * }}
 */
function getUsage() {
  const data  = readRaw();
  const now   = Date.now();
  const today = todayYmd();

  // ── Hourly window ──────────────────────────────────────────────────────────
  let hourCount        = Number(data.hour_count) || 0;
  let hourWindowStart  = data.hour_window_start  || null;

  if (hourWindowStart) {
    const windowAgeMs = now - new Date(hourWindowStart).getTime();
    if (windowAgeMs >= 60 * 60 * 1000) {
      // Window expired — reset
      hourCount       = 0;
      hourWindowStart = null;
    }
  } else {
    hourCount = 0;
  }

  // ── Daily counter ──────────────────────────────────────────────────────────
  let dayCount = Number(data.day_count) || 0;
  if (data.day_date !== today) {
    dayCount = 0;  // New day — reset
  }

  return {
    last_scan_at:      data.last_scan_at || null,
    hour_window_start: hourWindowStart,
    hour_count:        hourCount,
    day_date:          today,
    day_count:         dayCount,
  };
}

/**
 * Record that `domainCount` domains were just submitted for scanning.
 * Call this immediately after spawning the child process in /scan or /multi-scan.
 *
 * @param {number} domainCount
 * @returns {object} updated usage snapshot
 */
function recordScan(domainCount = 1) {
  const count = Math.max(1, parseInt(domainCount, 10) || 1);
  const usage = getUsage();
  const now   = new Date();

  usage.last_scan_at = now.toISOString();
  usage.day_count    = (usage.day_count || 0) + count;

  // Start the hourly window on first scan of the window
  if (!usage.hour_window_start) {
    usage.hour_window_start = now.toISOString();
  }
  usage.hour_count = (usage.hour_count || 0) + count;

  writeRaw(usage);

  console.log(
    `[scan-usage] Recorded ${count} domain(s). ` +
    `Hour: ${usage.hour_count}, Day: ${usage.day_count}, Last: ${usage.last_scan_at}`
  );

  return usage;
}

/**
 * How many minutes remain before the minutes_gap is satisfied.
 * Returns 0 if the gap has already elapsed or there was no previous scan.
 *
 * @param {number} minutesGap
 * @param {string|null} lastScanAt
 * @returns {number}  minutes remaining (ceil), 0 = OK to scan
 */
function minutesUntilNextAllowed(minutesGap, lastScanAt) {
  if (!minutesGap || !lastScanAt) return 0;
  const elapsedMs = Date.now() - new Date(lastScanAt).getTime();
  const gapMs     = minutesGap * 60 * 1000;
  if (elapsedMs >= gapMs) return 0;
  return Math.ceil((gapMs - elapsedMs) / 60000);
}

module.exports = { getUsage, recordScan, minutesUntilNextAllowed };