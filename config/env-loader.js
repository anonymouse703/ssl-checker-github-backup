const fs = require('fs');
const path = require('path');

function loadEnv() {
  // CRITICAL FIX: Define default BEFORE using process.env
  const DEFAULT_TOOL_DIR = '/usr/local/ind_leads/ssl-checker-tool';
  
  // Use existing TOOL_DIR from env if available, otherwise use default
  // This prevents the undefined error
  const toolDir = process.env.TOOL_DIR || DEFAULT_TOOL_DIR;
  
  // Now safely use toolDir (guaranteed to be a string)
  const envPath = path.join(toolDir, '.env');

  console.log(`[env-loader] Looking for .env at: ${envPath}`);

  if (fs.existsSync(envPath)) {
    console.log(`[env-loader] Loading .env from: ${envPath}`);
    const envContent = fs.readFileSync(envPath, 'utf8');
    envContent.split('\n').forEach((line) => {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) return;

      const [key, ...rest] = trimmed.split('=');
      if (key && rest.length) {
        const value = rest.join('=').trim();
        // Remove quotes if present
        const cleanValue = value.replace(/^["']|["']$/g, '');
        process.env[key.trim()] = cleanValue;
      }
    });
  } else {
    console.log(`[env-loader] .env file not found at: ${envPath}`);
  }

  // Set defaults if not in .env (using the same default)
  if (!process.env.PORT) process.env.PORT = '3000';
  if (!process.env.TOOL_DIR) process.env.TOOL_DIR = DEFAULT_TOOL_DIR;
  if (!process.env.OUTPUT_DIR) process.env.OUTPUT_DIR = '/home/ind/ind_leads_inputs';
  if (!process.env.SCAN_ROOT) process.env.SCAN_ROOT = '/home/ind';
  if (!process.env.MAX_ACTIVE_RESERVED_DOMAINS) process.env.MAX_ACTIVE_RESERVED_DOMAINS = '6';

  console.log(`[env-loader] Configuration loaded:`);
  console.log(`[env-loader]   PORT: ${process.env.PORT}`);
  console.log(`[env-loader]   TOOL_DIR: ${process.env.TOOL_DIR}`);
  console.log(`[env-loader]   OUTPUT_DIR: ${process.env.OUTPUT_DIR}`);
  console.log(`[env-loader]   SCAN_ROOT: ${process.env.SCAN_ROOT}`);

  return {
    PAGESPEED_API_KEY: process.env.PAGESPEED_API_KEY || '',
    PAGERANK_API_KEY: process.env.PAGERANK_API_KEY || ''
  };
}

module.exports = { loadEnv };