"use strict";

const fs = require("fs");
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
  const enabled = String(process.env.IMAGE_SYNC_ENABLED || "false").toLowerCase() === "true";
  if (!enabled) {
    return { skipped: true, reason: "IMAGE_SYNC_ENABLED is false" };
  }

  if (!fs.existsSync(domainDir)) {
    return { skipped: true, reason: `Missing folder: ${domainDir}` };
  }

  const remoteHost    = process.env.IMAGE_SYNC_HOST;
  const remoteUser    = process.env.IMAGE_SYNC_USER;
  const remoteBaseDir = process.env.IMAGE_SYNC_BASE_DIR || "/home/ind/images";
  const sshKeyPath    = process.env.IMAGE_SYNC_SSH_KEY  || "";
  const sshPort       = process.env.IMAGE_SYNC_SSH_PORT || "22";
  const useSudoRsync  = String(process.env.IMAGE_SYNC_SUDO || "false").toLowerCase() === "true";

  if (!remoteHost || !remoteUser) {
    return { skipped: true, reason: "Missing IMAGE_SYNC_HOST or IMAGE_SYNC_USER" };
  }

  const remoteDomainDir = `${remoteBaseDir}/${domain}`;

  // Build the SSH base command — includes port and optional key file
  const sshBase = sshKeyPath
    ? `ssh -i ${sshKeyPath} -p ${sshPort} -o StrictHostKeyChecking=no`
    : `ssh -p ${sshPort} -o StrictHostKeyChecking=no`;

  // sudo rsync flag — needed when remote user doesn't own the target directory
  const sudoFlag = useSudoRsync ? `--rsync-path="sudo rsync"` : "";

  // Create the remote domain folder first (also via sudo if needed)
  const mkdirCmd = useSudoRsync
    ? `${sshBase} ${remoteUser}@${remoteHost} "sudo mkdir -p '${remoteDomainDir}'"`
    : `${sshBase} ${remoteUser}@${remoteHost} "mkdir -p '${remoteDomainDir}'"`;

  await runCommand("bash", ["-lc", mkdirCmd]);

  // Rsync only image files (png, jpg, jpeg, webp)
  await runCommand("bash", [
    "-lc",
    `rsync -avz ${sudoFlag} --include='*/' --include='*.png' --include='*.jpg' --include='*.jpeg' --include='*.webp' --exclude='*' -e "${sshBase}" "${domainDir}/" "${remoteUser}@${remoteHost}:${remoteDomainDir}/"`
  ]);

  return {
    success: true,
    remoteHost,
    remoteDomainDir,
    domain,
  };
}

module.exports = { syncDomainImages };