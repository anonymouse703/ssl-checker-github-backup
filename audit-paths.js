'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const SCAN_ROOT = process.env.SCAN_ROOT || '/home/ind';
const INPUTS_DIR = process.env.OUTPUT_DIR || '/home/ind/ind_leads_inputs';
const TOOL_DIR = process.env.TOOL_DIR || '/usr/local/ind_leads/ssl-checker-tool';

/**
 * Resolve (or create) the batch output folder.
 * Creates a unique folder name with date + timestamp + random hex
 * Format: YYYY-MM-DD_timestamp_random
 * Example: 2026-03-28_1743214567890_a3f9
 */
function resolveBatchPath(existingPath) {
  if (existingPath) {
    console.log(`[audit-paths] Using existing batch path: ${existingPath}`);
    return existingPath;
  }

  const now = new Date();
  const year = String(now.getFullYear());
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');

  const dateStr = `${year}-${month}-${day}`;
  const tsMs = Date.now();
  const randHex = crypto.randomBytes(2).toString('hex');
  const folder = `${dateStr}_${tsMs}_${randHex}`;

  const batchPath = path.join(SCAN_ROOT, folder);
  console.log(`[audit-paths] Created new batch path: ${batchPath}`);

  return batchPath;
}

/**
 * Extract the date portion from a batch root folder name.
 * Works for format: 2026-03-20_timestamp_random
 */
function dateFromBatchRoot(batchRoot) {
  const base = path.basename(batchRoot);
  const m = base.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : base;
}

/**
 * Paths for one domain inside one batch.
 *
 * Result structure:
 *   batchRoot/
 *     summary.csv                 <-- batch summary
 *     _batch_stats.json
 *     example.com/
 *       example.com_results.csv   <-- full per-domain CSV
 *       summary.csv               <-- per-domain summary (same as domain CSV)
 *       images/                   <-- screenshots folder
 *         ssl.png
 *         intodns.png
 *         pagespeed.png
 *         pingdom.png
 *         sucuri.png
 */
function domainPaths(batchRoot, domain) {
  const domainDir    = path.join(batchRoot, domain);
  const imagesDir    = path.join(domainDir, 'images');
  const csvPath      = path.join(domainDir, `${domain}_results.csv`);
  const domainSummaryPath  = path.join(domainDir, 'summary.csv');
  const batchSummaryPath   = path.join(batchRoot, 'summary.csv');
  const statsPath          = path.join(batchRoot, '_batch_stats.json');

  return {
    domainDir,
    imagesDir,
    csvPath,
    domainSummaryPath,
    batchSummaryPath,
    statsPath,
    batchRoot,
  };
}

function ensureDomainDirs(paths) {
  const dirs = [
    paths.batchRoot,
    paths.domainDir,
    paths.imagesDir,
    INPUTS_DIR,
  ];

  dirs.forEach((dir) => {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
      console.log(`[audit-paths] Created directory: ${dir}`);
    }
  });
}

module.exports = {
  resolveBatchPath,
  domainPaths,
  ensureDomainDirs,
  dateFromBatchRoot,
  SCAN_ROOT,
  INPUTS_DIR,
  TOOL_DIR,
};