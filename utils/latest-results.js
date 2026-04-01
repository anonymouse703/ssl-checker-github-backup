"use strict";

const fs = require("fs");
const path = require("path");
const { SUMMARY_FIELDS } = require("../config/constants");
const { csvEscape, parseCSVRow } = require("./csv-writer");

const OUTPUT_DIR = process.env.OUTPUT_DIR || "/home/ind/ind_leads_inputs";
const LATEST_CSV = path.join(OUTPUT_DIR, "latest.csv");
const LATEST_JSON = path.join(OUTPUT_DIR, "latest.json");

// File locking for latest files
const LOCK_TIMEOUT_MS = 30000;
const LOCK_RETRY_MS = 200;
const STALE_LOCK_MS = 60000;

function lockPath(filePath) {
  return filePath + ".lock";
}

async function acquireLock(filePath) {
  const lock = lockPath(filePath);
  const deadline = Date.now() + LOCK_TIMEOUT_MS;

  while (Date.now() < deadline) {
    try {
      fs.writeFileSync(lock, String(process.pid), { flag: "wx" });
      return true;
    } catch (e) {
      if (e.code !== "EEXIST") throw e;

      try {
        const stat = fs.statSync(lock);
        if (Date.now() - stat.mtimeMs > STALE_LOCK_MS) {
          fs.unlinkSync(lock);
          continue;
        }
      } catch (_) {}

      await new Promise((r) => setTimeout(r, LOCK_RETRY_MS));
    }
  }

  return false;
}

function releaseLock(filePath) {
  try {
    fs.unlinkSync(lockPath(filePath));
  } catch (_) {}
}

function ensureOutputDir() {
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }
}

/**
 * Read existing latest CSV and return as array of objects
 */
function readLatestCSV() {
  if (!fs.existsSync(LATEST_CSV)) {
    return [];
  }

  try {
    const content = fs.readFileSync(LATEST_CSV, "utf8");
    const lines = content.split("\n").filter(l => l.trim());
    
    if (lines.length < 2) {
      return [];
    }

    const headers = parseCSVRow(lines[0]);
    const rows = [];

    for (let i = 1; i < lines.length; i++) {
      const cols = parseCSVRow(lines[i]);
      if (cols.length === headers.length) {
        const row = {};
        headers.forEach((h, idx) => {
          row[h] = cols[idx] || "";
        });
        rows.push(row);
      }
    }

    return rows;
  } catch (err) {
    console.error(`[latest-results] Error reading latest.csv: ${err.message}`);
    return [];
  }
}

/**
 * Read existing latest JSON
 */
function readLatestJSON() {
  if (!fs.existsSync(LATEST_JSON)) {
    return {};
  }

  try {
    return JSON.parse(fs.readFileSync(LATEST_JSON, "utf8"));
  } catch (err) {
    console.error(`[latest-results] Error reading latest.json: ${err.message}`);
    return {};
  }
}

/**
 * Update or append domain data in latest files
 */
async function updateLatestResults(domainData) {
  ensureOutputDir();

  const domain = String(domainData.Domain || "").trim().toLowerCase();
  if (!domain) {
    console.error("[latest-results] Cannot update latest: missing domain");
    return false;
  }

  console.log(`[latest-results] Updating latest files for domain: ${domain}`);

  const locked = await acquireLock(LATEST_CSV);
  if (!locked) {
    console.error(`[latest-results] Failed to acquire lock for latest files`);
    return false;
  }

  try {
    const existingRows = readLatestCSV();
    const existingIndex = existingRows.findIndex(
      row => String(row.Domain || "").trim().toLowerCase() === domain
    );

    if (existingIndex >= 0) {
      existingRows[existingIndex] = { ...existingRows[existingIndex], ...domainData };
      console.log(`[latest-results] Updated existing entry for ${domain}`);
    } else {
      existingRows.push(domainData);
      console.log(`[latest-results] Added new entry for ${domain}`);
    }

    // Write updated CSV
    const headers = SUMMARY_FIELDS;
    const csvLines = [headers.join(",")];

    for (const row of existingRows) {
      const line = headers.map(h => csvEscape(row[h] || "")).join(",");
      csvLines.push(line);
    }

    fs.writeFileSync(LATEST_CSV, csvLines.join("\n") + "\n", "utf8");
    console.log(`[latest-results] Updated latest.csv (${existingRows.length} total domains)`);

    // Update JSON
    const jsonData = {};
    for (const row of existingRows) {
      const key = String(row.Domain || "").trim().toLowerCase();
      if (key) {
        jsonData[key] = row;
      }
    }

    jsonData._metadata = {
      last_updated: new Date().toISOString(),
      total_domains: existingRows.length,
      version: "1.0"
    };

    fs.writeFileSync(LATEST_JSON, JSON.stringify(jsonData, null, 2), "utf8");
    console.log(`[latest-results] Updated latest.json (${existingRows.length} total domains)`);

    return true;
  } catch (err) {
    console.error(`[latest-results] Error updating latest files: ${err.message}`);
    return false;
  } finally {
    releaseLock(LATEST_CSV);
  }
}

function getDomainFromLatest(domain) {
  const jsonData = readLatestJSON();
  const key = String(domain || "").trim().toLowerCase();
  return jsonData[key] || null;
}

function getAllLatestDomains() {
  const jsonData = readLatestJSON();
  const domains = [];
  for (const key in jsonData) {
    if (key !== "_metadata") {
      domains.push(jsonData[key]);
    }
  }
  return domains;
}

module.exports = {
  updateLatestResults,
  getDomainFromLatest,
  getAllLatestDomains,
  LATEST_CSV,
  LATEST_JSON
};