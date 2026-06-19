"use strict";

const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

function getCommandTimeoutMs() {
  const raw = process.env.IMAGE_SYNC_COMMAND_TIMEOUT_MS || process.env.IMAGE_SYNC_TIMEOUT_MS || "120000";
  const n = parseInt(raw, 10);
  return Number.isFinite(n) && n > 0 ? n : 120000;
}

function runCommand(command, args = [], options = {}) {
  return new Promise((resolve, reject) => {
    const timeoutMs = options.timeoutMs || getCommandTimeoutMs();
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";
    let settled = false;

    const finish = (err, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(killTimer);
      if (err) reject(err);
      else resolve(value);
    };

    const killTimer = setTimeout(() => {
      try { child.kill("SIGTERM"); } catch (_) {}
      setTimeout(() => {
        try { child.kill("SIGKILL"); } catch (_) {}
      }, 3000);
      finish(new Error(`${command} timed out after ${Math.round(timeoutMs / 1000)}s`));
    }, timeoutMs);

    child.stdout.on("data", (d) => { stdout += String(d); });
    child.stderr.on("data", (d) => { stderr += String(d); });

    child.on("error", (err) => finish(err));

    child.on("close", (code) => {
      if (settled) return;
      if (code === 0) {
        finish(null, { stdout, stderr });
      } else {
        finish(new Error(`${command} exited with code ${code}: ${stderr || stdout}`));
      }
    });
  });
}

function normalizeDomain(domain) {
  return String(domain || "")
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/.*$/, "")
    .trim()
    .toLowerCase();
}

function listLocalImageFiles(imagesDir) {
  if (!fs.existsSync(imagesDir)) return [];
  return fs.readdirSync(imagesDir)
    .filter((f) => [".png", ".jpg", ".jpeg", ".webp"].includes(path.extname(f).toLowerCase()))
    .sort();
}

function shellSingleQuote(value) {
  return String(value).replace(/'/g, `'\\''`);
}

async function runRemoteSSH(sshCmd, remoteUser, remoteHost, remoteCommand) {
  const wrapped = `${sshCmd} ${remoteUser}@${remoteHost} "${remoteCommand}"`;
  return runCommand("bash", ["-lc", wrapped]);
}

async function syncDomainImages(domainDir, domain) {
  const enabled = String(process.env.IMAGE_SYNC_ENABLED || "false").toLowerCase() === "true";

  if (!enabled) {
    return { skipped: true, reason: "IMAGE_SYNC_ENABLED is false" };
  }

  // Guard against undefined/null paths so image sync cannot crash the scanner.
  // index.js passes the per-domain directory; if an older audit-paths.js build
  // does not provide domainDir, we skip cleanly instead of throwing
  // ERR_INVALID_ARG_TYPE: The "path" argument must be of type string.
  if (!domainDir || typeof domainDir !== "string") {
    return {
      skipped: true,
      reason: `Missing local domain directory for image sync: ${String(domainDir)}`,
      domain: normalizeDomain(domain),
    };
  }

  const resolvedDomainDir = path.resolve(domainDir);
  const imagesDir = path.basename(resolvedDomainDir) === "images"
    ? resolvedDomainDir
    : path.join(resolvedDomainDir, "images");

  if (!fs.existsSync(imagesDir)) {
    console.log(`[sync-images] No images folder found at: ${imagesDir}`);
    return { skipped: true, reason: `Missing images folder: ${imagesDir}` };
  }

  const remoteHost = process.env.IMAGE_SYNC_HOST;
  const remoteUser = process.env.IMAGE_SYNC_USER;
  const remoteBaseDir = process.env.IMAGE_SYNC_BASE_DIR || "/home/ta1/public_html/images";
  const remoteBaseUrl = process.env.IMAGE_SYNC_BASE_URL || "";
  const sshKeyPath = process.env.IMAGE_SYNC_SSH_KEY || "";
  const sshPort = process.env.IMAGE_SYNC_SSH_PORT || "22";
  const useSudoRsync = String(process.env.IMAGE_SYNC_SUDO || "false").toLowerCase() === "true";

  if (!remoteHost || !remoteUser) {
    return { skipped: true, reason: "Missing IMAGE_SYNC_HOST or IMAGE_SYNC_USER" };
  }

  const safeDomain = normalizeDomain(domain);

  if (!safeDomain) {
    return { skipped: true, reason: "Invalid domain value" };
  }

  const remoteDomainDir = `${remoteBaseDir}/${safeDomain}`;
  const quotedRemoteDomainDir = shellSingleQuote(remoteDomainDir);

  const localFiles = listLocalImageFiles(imagesDir);

  if (localFiles.length === 0) {
    console.log(`[sync-images] No image files found in: ${imagesDir}`);
    return {
      skipped: true,
      reason: `No image files found in ${imagesDir}`,
      remoteHost,
      remoteDomainDir,
      domain: safeDomain,
      baseUrl: remoteBaseUrl,
      imageCount: 0,
      localFiles: [],
      remoteFiles: [],
    };
  }

  // Build SSH command. BatchMode avoids password prompts that can hang a queue.
  const sshConnectTimeoutSec = parseInt(process.env.IMAGE_SYNC_CONNECT_TIMEOUT_SEC || "20", 10);
  const sshParts = [
    "ssh",
    "-p", sshPort,
    "-o", "BatchMode=yes",
    "-o", "StrictHostKeyChecking=no",
    "-o", `ConnectTimeout=${Number.isFinite(sshConnectTimeoutSec) && sshConnectTimeoutSec > 0 ? sshConnectTimeoutSec : 20}`,
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=2",
  ];
  if (sshKeyPath) sshParts.push("-i", sshKeyPath);
  const sshCmd = sshParts.join(" ");

  console.log(`[sync-images] Syncing ${imagesDir} -> ${remoteUser}@${remoteHost}:${remoteDomainDir}`);
  console.log(`[sync-images] Local files: ${localFiles.join(", ")}`);

  try {
    // Create remote domain folder
    const mkdirRemoteCommand = useSudoRsync
      ? `sudo mkdir -p '${quotedRemoteDomainDir}'`
      : `mkdir -p '${quotedRemoteDomainDir}'`;

    await runRemoteSSH(sshCmd, remoteUser, remoteHost, mkdirRemoteCommand);
    console.log(`[sync-images] Created remote directory: ${remoteDomainDir}`);

    // Sync fresh images.
    // --checksum : overwrite remote file if content differs (not just timestamp/size).
    // --delete   : remove remote files no longer present locally, so a re-scan fully
    //              replaces the previous set with no leftover screenshots from tools
    //              that weren't run this time (e.g. old pingdom.png when Pingdom was
    //              excluded from the re-scan).
    const rsyncPath = useSudoRsync ? `--rsync-path='sudo rsync'` : "";
    const rsyncIoTimeoutSec = parseInt(process.env.IMAGE_SYNC_RSYNC_IO_TIMEOUT_SEC || "60", 10);
    const rsyncTimeout = Number.isFinite(rsyncIoTimeoutSec) && rsyncIoTimeoutSec > 0 ? rsyncIoTimeoutSec : 60;
    const rsyncCmd = `rsync -avz --checksum --delete --timeout=${rsyncTimeout} ${rsyncPath} -e "${sshCmd}" "${imagesDir}/" "${remoteUser}@${remoteHost}:${remoteDomainDir}/"`;

    await runCommand("bash", ["-lc", rsyncCmd]);
    console.log(`[sync-images] ✅ Rsync complete for ${safeDomain}`);

    // Verify remote file list after sync
    const listRemoteCommand = useSudoRsync
      ? `sudo find '${quotedRemoteDomainDir}' -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | sort`
      : `find '${quotedRemoteDomainDir}' -mindepth 1 -maxdepth 1 -type f -printf '%f\n' | sort`;

    const listResult = await runRemoteSSH(sshCmd, remoteUser, remoteHost, listRemoteCommand);
    const remoteFiles = String(listResult.stdout || "")
      .split(/\r?\n/)
      .map((s) => s.trim())
      .filter(Boolean);

    console.log(`[sync-images] Remote files after sync: ${remoteFiles.join(", ")}`);

    // Delete local image files now that they are confirmed on the remote server.
    // Only delete files that actually made it across — if a file is missing from
    // remoteFiles for any reason, leave it in place so it can be re-synced.
    const deleted = [];
    const notDeleted = [];
    for (const localFile of localFiles) {
      if (remoteFiles.includes(localFile)) {
        try {
          fs.unlinkSync(path.join(imagesDir, localFile));
          deleted.push(localFile);
        } catch (delErr) {
          console.warn(`[sync-images] ⚠️  Could not delete local file ${localFile}: ${delErr.message}`);
          notDeleted.push(localFile);
        }
      } else {
        console.warn(`[sync-images] ⚠️  ${localFile} not found on remote — keeping local copy`);
        notDeleted.push(localFile);
      }
    }

    if (deleted.length > 0) {
      console.log(`[sync-images] 🗑️  Deleted ${deleted.length} local image(s): ${deleted.join(", ")}`);
    }
    if (notDeleted.length > 0) {
      console.warn(`[sync-images] ⚠️  Kept ${notDeleted.length} local image(s) not confirmed on remote: ${notDeleted.join(", ")}`);
    }

    return {
      success: true,
      remoteHost,
      remoteDomainDir,
      domain: safeDomain,
      baseUrl: remoteBaseUrl,
      imageCount: localFiles.length,
      localFiles,
      remoteFiles,
      deletedLocal: deleted,
      keptLocal: notDeleted,
    };
  } catch (err) {
    console.error(`[sync-images] ❌ Failed to sync images for ${safeDomain}:`, err.message);
    return {
      success: false,
      skipped: false,
      reason: err.message,
      error: err.message,
      remoteHost,
      remoteDomainDir,
      domain: safeDomain,
      baseUrl: remoteBaseUrl,
      imageCount: localFiles.length,
      localFiles,
      remoteFiles: [],
    };
  }
}

module.exports = { syncDomainImages };