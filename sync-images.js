"use strict";

const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

function runCommand(command, args = []) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });

    let stdout = "";
    let stderr = "";

    child.stdout.on("data", (d) => { stdout += String(d); });
    child.stderr.on("data", (d) => { stderr += String(d); });

    child.on("error", reject);

    child.on("close", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        reject(new Error(`${command} exited with code ${code}: ${stderr || stdout}`));
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
  const imagesDir = path.join(domainDir, "images");
  const enabled = String(process.env.IMAGE_SYNC_ENABLED || "false").toLowerCase() === "true";

  if (!enabled) {
    return { skipped: true, reason: "IMAGE_SYNC_ENABLED is false" };
  }

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

  // Build SSH command
  const sshParts = ["ssh", "-p", sshPort, "-o", "StrictHostKeyChecking=no"];
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

    // Sync fresh images
    // --checksum helps ensure overwrite happens even if timestamps/sizes are misleading.
    const rsyncPath = useSudoRsync ? `--rsync-path='sudo rsync'` : "";
    const rsyncCmd = `rsync -avz --checksum ${rsyncPath} -e "${sshCmd}" "${imagesDir}/" "${remoteUser}@${remoteHost}:${remoteDomainDir}/"`;

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

    return {
      success: true,
      remoteHost,
      remoteDomainDir,
      domain: safeDomain,
      baseUrl: remoteBaseUrl,
      imageCount: localFiles.length,
      localFiles,
      remoteFiles,
    };
  } catch (err) {
    console.error(`[sync-images] ❌ Failed to sync images for ${safeDomain}:`, err.message);
    throw err;
  }
}

module.exports = { syncDomainImages };