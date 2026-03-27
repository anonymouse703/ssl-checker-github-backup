const fs = require('fs');
const path = require('path');

const { TOOL_DIR } = require('../audit-paths');

function loadEnv() {
  const envPath = path.join(TOOL_DIR, '.env');

  if (fs.existsSync(envPath)) {
    fs.readFileSync(envPath, 'utf8')
      .split('\n')
      .forEach((line) => {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith('#')) return;

        const [key, ...rest] = trimmed.split('=');
        if (key && rest.length) {
          process.env[key.trim()] = rest.join('=').trim();
        }
      });
  }

  const PAGESPEED_API_KEY = process.env.PAGESPEED_API_KEY || '';
  const PAGERANK_API_KEY = process.env.PAGERANK_API_KEY || '';

  return { PAGESPEED_API_KEY, PAGERANK_API_KEY };
}

module.exports = { loadEnv };