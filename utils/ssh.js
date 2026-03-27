/**
 * ssh.js
 * No longer uses SSH or bash.
 * Kept as a thin wrapper so existing imports don't break.
 * All real logic has moved to server-checks.js using pure Node.js DNS/net/tls.
 */

const { exec } = require('child_process');
const util = require('util');
const execPromise = util.promisify(exec);

async function runSSHCommand(command) {
  try {
    const { stdout, stderr } = await execPromise(command, { timeout: 30000 });
    return { success: true, output: stdout.trim(), error: stderr.trim() };
  } catch (error) {
    return { success: false, output: error.stdout?.trim() || '', error: error.message };
  }
}

async function runMultipleSSHCommands(commands) {
  const results = {};
  for (const [key, command] of Object.entries(commands)) {
    const result = await runSSHCommand(command);
    results[key] = {
      ...result,
      status: result.success && result.output && result.output !== 'Missing' && result.output !== '' ? 'OK' : 'Missing',
      raw: result.output,
    };
  }
  return results;
}

module.exports = { runSSHCommand, runMultipleSSHCommands };