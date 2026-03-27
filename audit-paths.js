'use strict';

const fs   = require('fs');
const path = require('path');
const crypto = require('crypto');

/** Root folder where dated scan folders are created. */
const SCAN_ROOT = '/home/ind';

/** Folder where the merged summary CSV is written. */
const INPUTS_DIR = '/home/ind/ind_leads_inputs';

/** Tool install location. */
const TOOL_DIR = '/usr/local/ind_leads/ssl-checker-tool';

/**
 * Resolve (or create) the batch output folder.
 *
 * Old behaviour: date + incrementing suffix (2026-03-20, 2026-03-20-2, …)
 *   ⚠ Race condition: two PCs reading the FS at the same instant both pick -2.
 *
 * New behaviour: date + millisecond timestamp + 4-char random hex suffix.
 *   e.g. 2026-03-20_1711900123456_a3f9
 *   Collision probability ≈ 0 even with 15 concurrent PCs.
 *
 * If existingPath is provided (passed via SCAN_BATCH_PATH env var from
 * multi-audit.js or api-server.js) it is returned unchanged — all child
 * processes in a batch share the same pre-resolved path.
 */
function resolveBatchPath(existingPath) {
  if (existingPath) return existingPath;

  const now   = new Date();
  const year  = String(now.getFullYear());
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day   = String(now.getDate()).padStart(2, '0');

  const dateStr  = `${year}-${month}-${day}`;
  const tsMs     = Date.now();
  const randHex  = crypto.randomBytes(2).toString('hex'); // 4 chars, e.g. "a3f9"
  const folder   = `${dateStr}_${tsMs}_${randHex}`;

  return path.join(SCAN_ROOT, folder);
}

/**
 * Extract the date portion from a batch root folder name.
 * Works for both old format (2026-03-20) and new (2026-03-20_ts_rand).
 */
function dateFromBatchRoot(batchRoot) {
  const base = path.basename(batchRoot);
  const m    = base.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : base;
}

/**
 * Return all well-known paths for a domain inside a batch folder.
 *
 * summaryPath — the per-batch summary CSV written by csv-writer.js.
 *   One summary per batch folder (not one global file) so concurrent
 *   batches from different PCs each get their own summary.
 */
function domainPaths(batchRoot, domain) {
  const domainDir  = path.join(batchRoot, domain);
  const imagesDir  = domainDir;
  const csvPath    = path.join(domainDir, `${domain}_results.csv`);
  const dateStr    = dateFromBatchRoot(batchRoot);
  // summary lives inside the batch folder — not in INPUTS_DIR — so two
  // concurrent batches never write to the same summary.csv.
  const summaryPath = path.join(batchRoot, 'summary.csv');

  return { domainDir, imagesDir, csvPath, summaryPath, batchRoot };
}

function ensureDomainDirs(paths) {
  [paths.domainDir, INPUTS_DIR].forEach((dir) => {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
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