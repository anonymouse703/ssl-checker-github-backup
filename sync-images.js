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

async function syncDomainImages(domainDir, domain) {
  const imagesDir = path.join(domainDir, 'images');
  const enabled = String(process.env.IMAGE_SYNC_ENABLED || "false").toLowerCase() === "true";

  if (!enabled) {
    return { skipped: true, reason: "IMAGE_SYNC_ENABLED is false" };
  }

  if (!fs.existsSync(imagesDir)) {
    console.log(`[sync-images] No images folder found at: ${imagesDir}`);
    return { skipped: true, reason: `Missing images folder: ${imagesDir}` };
  }

  const remoteHost    = process.env.IMAGE_SYNC_HOST;
  const remoteUser    = process.env.IMAGE_SYNC_USER;
  const remoteBaseDir = process.env.IMAGE_SYNC_BASE_DIR || "/home/ta1/public_html/images";
  const sshKeyPath    = process.env.IMAGE_SYNC_SSH_KEY  || "";
  const sshPort       = process.env.IMAGE_SYNC_SSH_PORT || "22";
  const useSudoRsync  = String(process.env.IMAGE_SYNC_SUDO || "false").toLowerCase() === "true";

  if (!remoteHost || !remoteUser) {
    return { skipped: true, reason: "Missing IMAGE_SYNC_HOST or IMAGE_SYNC_USER" };
  }

  const remoteDomainDir = `${remoteBaseDir}/${domain}`;

  // Build SSH command cleanly without nested quote issues
  const sshParts = ['ssh', '-p', sshPort, '-o', 'StrictHostKeyChecking=no'];
  if (sshKeyPath) sshParts.push('-i', sshKeyPath);
  const sshCmd = sshParts.join(' ');

  console.log(`[sync-images] Syncing ${imagesDir} -> ${remoteUser}@${remoteHost}:${remoteDomainDir}`);

  try {
    // Create remote directory
    const mkdirArgs = useSudoRsync
      ? ['-c', `${sshCmd} ${remoteUser}@${remoteHost} "sudo mkdir -p '${remoteDomainDir}'"`]
      : ['-c', `${sshCmd} ${remoteUser}@${remoteHost} "mkdir -p '${remoteDomainDir}'"`];

    await runCommand("bash", mkdirArgs);
    console.log(`[sync-images] Created remote directory: ${remoteDomainDir}`);

    const imageFiles = fs.readdirSync(imagesDir).filter(f =>
      ['.png', '.jpg', '.jpeg', '.webp'].includes(path.extname(f).toLowerCase())
    );

    // Sync with so re-scans cleanly override old images
    const rsyncPath = useSudoRsync ? `--rsync-path='sudo rsync'` : '';
    const rsyncCmd = `rsync -avz  ${rsyncPath} -e '${sshCmd}' "${imagesDir}/" "${remoteUser}@${remoteHost}:${remoteDomainDir}/"`;

    await runCommand("bash", ["-c", rsyncCmd]);

    console.log(`[sync-images] ✅ Successfully synced images for ${domain}`);

    return {
      success: true,
      remoteHost,
      remoteDomainDir,
      domain,
      baseUrl: process.env.IMAGE_SYNC_BASE_URL,
      imageCount: imageFiles.length
    };
  } catch (err) {
    console.error(`[sync-images] ❌ Failed to sync images for ${domain}:`, err.message);
    throw err;
  }
}

module.exports = { syncDomainImages };