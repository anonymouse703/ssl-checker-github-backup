// utils/csv-writer.js
"use strict";

const fs = require("fs");
const path = require("path");

const CSV_FIELDS = [
  "Domain",
  "Run_At",
  "Total_Time",
  "SSL_Status",
  "SSL_Error",
  "SSL_ErrorCode",
  "SSL_Grade",
  "SSL_Endpoints",
  "SSL_AllGrades",
  "SSL_URL",
  "SSL_Screenshot",
  "Sucuri_Status",
  "Sucuri_Error",
  "Sucuri_ErrorCode",
  "Sucuri_Overall",
  "Sucuri_Malware",
  "Sucuri_Blacklist",
  "Sucuri_URL",
  "Sucuri_Screenshot",
  "PageSpeed_Status",
  "PageSpeed_Error",
  "PageSpeed_ErrorCode",
  "PageSpeed_Performance",
  "PageSpeed_Accessibility",
  "PageSpeed_BestPractices",
  "PageSpeed_SEO",
  "PageSpeed_LCP",
  "PageSpeed_CLS",
  "PageSpeed_TBT",
  "PageSpeed_TTFB",
  "PageSpeed_FCP",
  "PageSpeed_URL",
  "PageSpeed_Screenshot",
  "Pingdom_Status",
  "Pingdom_Error",
  "Pingdom_ErrorCode",
  "Pingdom_Grade",
  "Pingdom_GradeLetter",
  "Pingdom_GradeNumber",
  "Pingdom_LoadTime",
  "Pingdom_PageSize",
  "Pingdom_Requests",
  "Pingdom_URL",
  "Pingdom_Screenshot",
  "DNS_Status",
  "DNS_Error",
  "DNS_ErrorCode",
  "DNS_Propagation",
  "DNS_TotalServers",
  "DNS_Propagated",
  "DNS_Failed",
  "DNS_PropagationRate",
  "DNS_URL",
  "DNS_Screenshot",
  "PageRank_Status",
  "PageRank_Error",
  "PageRank_ErrorCode",
  "PageRank_Integer",
  "PageRank_Decimal",
  "PageRank_Rank",
  "PageRank_URL",
  "Server_SPF_Status",
  "Server_SPF_Value",
  "Server_DMARC_Status",
  "Server_DMARC_Value",
  "Server_DKIM_Status",
  "Server_DKIM_Value",
  "Server_DomainBlacklist_Status",
  "Server_DomainBlacklist_Value",
  "Server_MX_Status",
  "Server_MX_Value",
  "Server_RBL_Status",
  "Server_RBL_Value",
  "Server_IP_Address",
  "Server_BrokenLinks_Status",
  "Server_BrokenLinks_Value",
  "Server_HTTP_Status",
  "Server_HTTP_Code",
  "Server_SSL_Status",
  "Server_SSL_Value",
  "IntoDNS_OverallHealth",
  "IntoDNS_ErrorCount",
  "IntoDNS_WarnCount",
  "IntoDNS_MX_Status",
  "IntoDNS_NS_Count",
  "IntoDNS_SOA_Serial",
  "IntoDNS_URL",
  "IntoDNS_Screenshot",
];

const SUMMARY_FIELDS = [
  "Domain",
  "Run_At",
  "Total_Time",
  "SSL_Status",
  "SSL_Grade",
  "SSL_Endpoints",
  "SSL_AllGrades",
  "SSL_URL",
  "SSL_Screenshot",
  "Sucuri_Status",
  "Sucuri_Overall",
  "Sucuri_Malware",
  "Sucuri_Blacklist",
  "Sucuri_URL",
  "Sucuri_Screenshot",
  "PageSpeed_Status",
  "PageSpeed_Performance",
  "PageSpeed_Accessibility",
  "PageSpeed_BestPractices",
  "PageSpeed_SEO",
  "PageSpeed_LCP",
  "PageSpeed_CLS",
  "PageSpeed_TBT",
  "PageSpeed_TTFB",
  "PageSpeed_FCP",
  "PageSpeed_URL",
  "PageSpeed_Screenshot",
  "Pingdom_Status",
  "Pingdom_Grade",
  "Pingdom_GradeLetter",
  "Pingdom_GradeNumber",
  "Pingdom_LoadTime",
  "Pingdom_PageSize",
  "Pingdom_Requests",
  "Pingdom_URL",
  "Pingdom_Screenshot",
  "DNS_Status",
  "DNS_Propagation",
  "DNS_TotalServers",
  "DNS_Propagated",
  "DNS_PropagationRate",
  "DNS_URL",
  "DNS_Screenshot",
  "PageRank_Status",
  "PageRank_Integer",
  "PageRank_Decimal",
  "PageRank_Rank",
  "PageRank_URL",
  "Server_SPF_Status",
  "Server_DMARC_Status",
  "Server_DKIM_Status",
  "Server_DomainBlacklist_Status",
  "Server_MX_Status",
  "Server_RBL_Status",
  "Server_IP_Address",
  "Server_BrokenLinks_Status",
  "Server_HTTP_Status",
  "Server_SSL_Status",
  "IntoDNS_OverallHealth",
  "IntoDNS_ErrorCount",
  "IntoDNS_WarnCount",
  "IntoDNS_MX_Status",
  "IntoDNS_NS_Count",
  "IntoDNS_SOA_Serial",
  "IntoDNS_URL",
  "IntoDNS_Screenshot",
];

function csvEscape(value) {
  if (value === null || value === undefined) return "";
  const str = String(value);
  if (
    str.includes(",") ||
    str.includes('"') ||
    str.includes("\n") ||
    str.includes("\r")
  ) {
    return '"' + str.replace(/"/g, '""') + '"';
  }
  return str;
}

function parseCSVRow(row) {
  const cols = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < row.length; i++) {
    const ch = row[i];
    if (inQuotes) {
      if (ch === '"') {
        if (row[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else {
      if (ch === '"') {
        inQuotes = true;
      } else if (ch === ",") {
        cols.push(cur);
        cur = "";
      } else {
        cur += ch;
      }
    }
  }

  cols.push(cur);
  return cols;
}

// File locking — prevents concurrent domain processes corrupting the same CSV
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

function ensureParentDir(filePath) {
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
}

function normalizeHeader(fields) {
  return fields.join(",");
}

function buildRow(fields, data) {
  return fields.map((f) => csvEscape(data[f])).join(",");
}

function replaceOrAppendRow(existingContent, fields, data) {
  const headerLine = normalizeHeader(fields);
  const rowLine = buildRow(fields, data);
  const incomingDomain = String(data.Domain || "").trim().toLowerCase();

  if (!existingContent || !existingContent.trim()) {
    return headerLine + "\n" + rowLine + "\n";
  }

  const lines = existingContent
    .split("\n")
    .map((l) => l.replace(/\r/g, ""))
    .filter((l) => l.trim() !== "");

  let header = lines[0] || headerLine;
  const rows = lines.slice(1);

  if (header !== headerLine) {
    // Rewrite using the expected header for consistency
    header = headerLine;
  }

  const headerCols = parseCSVRow(header);
  const domainIdx = headerCols.indexOf("Domain");

  const filteredRows =
    domainIdx === -1
      ? rows
      : rows.filter((row) => {
          if (!row.trim()) return false;
          const cols = parseCSVRow(row);
          return String(cols[domainIdx] || "").trim().toLowerCase() !== incomingDomain;
        });

  return [header, ...filteredRows, rowLine].join("\n") + "\n";
}

async function writeCSVRowToFile(csvFilePath, data, fields, label) {
  console.log(`[csv-writer] ===== Writing CSV: ${label} =====`);
  console.log(`[csv-writer] File path: ${csvFilePath}`);
  console.log(`[csv-writer] Domain: ${data.Domain}`);

  ensureParentDir(csvFilePath);

  const tmpPath = csvFilePath + ".tmp";

  console.log(`[csv-writer] Acquiring lock for: ${csvFilePath}`);
  const locked = await acquireLock(csvFilePath);
  if (!locked) {
    throw new Error(`Failed to acquire lock for: ${csvFilePath}`);
  }

  try {
    const existing = fs.existsSync(csvFilePath)
      ? fs.readFileSync(csvFilePath, "utf8")
      : "";

    const newContent = replaceOrAppendRow(existing, fields, data);

    console.log(`[csv-writer] Writing temp file: ${tmpPath}`);
    fs.writeFileSync(tmpPath, newContent, "utf8");

    // Atomic replace on same filesystem
    fs.renameSync(tmpPath, csvFilePath);

    const stats = fs.statSync(csvFilePath);
    console.log(`[csv-writer] ✅ Successfully wrote CSV: ${label}`);
    console.log(`[csv-writer] File size: ${stats.size} bytes`);
  } catch (err) {
    console.error(`[csv-writer] ❌ Could not save ${label}: ${err.message}`);
    console.error(`[csv-writer] 💡 Close the file in Excel / FileMaker and re-run.`);
    console.error(`[csv-writer] 💾 Temp file (if present): ${tmpPath}`);
    throw err;
  } finally {
    try {
      if (fs.existsSync(tmpPath)) {
        fs.unlinkSync(tmpPath);
      }
    } catch (_) {}
    releaseLock(csvFilePath);
  }
}

function writeDomainCSV(csvFilePath, data) {
  return writeCSVRowToFile(
    csvFilePath,
    data,
    CSV_FIELDS,
    `${data.Domain}_results.csv`
  );
}

function writeSummaryCSV(summaryPath, data) {
  return writeCSVRowToFile(summaryPath, data, SUMMARY_FIELDS, "summary.csv");
}

module.exports = {
  csvEscape,
  parseCSVRow,
  writeDomainCSV,
  writeSummaryCSV,
};