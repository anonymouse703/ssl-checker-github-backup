// utils/site-checker.js
'use strict';

const https = require('https');
const http = require('http');
const dns = require('dns');

/**
 * Dead domain error codes — these mean the site is truly gone/unreachable.
 * Connection refused, DNS not found, network timeout, etc.
 */
const DEAD_ERROR_CODES = new Set([
  'ENOTFOUND',        // DNS resolution failed — domain doesn't exist or has no DNS
  'ECONNREFUSED',     // Port 80/443 closed — no web server running
  'ECONNRESET',       // Connection reset mid-handshake
  'ETIMEDOUT',        // TCP connection timed out
  'ENETUNREACH',      // Network unreachable
  'EHOSTUNREACH',     // Host unreachable
  'EAI_AGAIN',        // DNS temporary failure
  'ESERVFAIL',        // DNS server failure
]);

/**
 * SSL error codes that still mean the site IS alive (just has cert issues).
 * We should still scan these.
 */
const SSL_BUT_ALIVE_CODES = new Set([
  'DEPTH_ZERO_SELF_SIGNED_CERT',
  'SELF_SIGNED_CERT_IN_CHAIN',
  'UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'ERR_TLS_CERT_ALTNAME_INVALID',
  'CERT_HAS_EXPIRED',
  'UNABLE_TO_GET_ISSUER_CERT_LOCALLY',
]);

/**
 * First do a DNS lookup — fastest way to detect dead domains.
 */
function dnsLookup(domain, timeoutMs = 5000) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      resolve({ ok: false, error: 'DNS timeout', code: 'ETIMEDOUT' });
    }, timeoutMs);

    dns.lookup(domain, (err, address) => {
      clearTimeout(timer);
      if (err) {
        resolve({ ok: false, error: err.message, code: err.code, ip: null });
      } else {
        resolve({ ok: true, error: null, code: null, ip: address });
      }
    });
  });
}

/**
 * Try an HTTP GET to the domain and return reachability info.
 */
function httpProbe(url, timeoutMs = 12000) {
  return new Promise((resolve) => {
    const lib = url.startsWith('https') ? https : http;

    let settled = false;
    function done(result) {
      if (settled) return;
      settled = true;
      resolve(result);
    }

    try {
      const req = lib.get(
        url,
        {
          timeout: timeoutMs,
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; SiteChecker/1.0)',
            'Accept': 'text/html,application/xhtml+xml,*/*',
          },
          rejectUnauthorized: false, // ignore SSL cert errors — we just want to know if server responds
        },
        (res) => {
          // Drain response so socket is released
          res.resume();
          const status = res.statusCode;
          req.destroy();
          done({ alive: true, status, error: null, code: null, url });
        }
      );

      req.on('timeout', () => {
        req.destroy();
        done({ alive: false, status: null, error: 'Connection timed out', code: 'ETIMEDOUT', url });
      });

      req.on('error', (err) => {
        const code = err.code || '';

        if (SSL_BUT_ALIVE_CODES.has(code) || code.startsWith('ERR_TLS') || code.startsWith('ERR_SSL')) {
          // SSL error but server IS responding
          done({ alive: true, status: null, error: err.message, code, url, sslIssue: true });
        } else if (DEAD_ERROR_CODES.has(code)) {
          done({ alive: false, status: null, error: err.message, code, url });
        } else {
          // Unknown error — treat as dead to be safe
          done({ alive: false, status: null, error: err.message, code, url });
        }
      });

    } catch (err) {
      done({ alive: false, status: null, error: err.message, code: err.code || 'UNKNOWN', url });
    }
  });
}

/**
 * Main entry point.
 *
 * Strategy:
 *  1. DNS lookup — if it fails, domain is dead. Fast (< 5s).
 *  2. HTTPS probe — if server responds (any status), domain is alive.
 *  3. HTTP probe fallback — in case HTTPS fails but HTTP works.
 *
 * Returns:
 *  { alive: boolean, status: number|null, error: string|null, ip: string|null, reason: string }
 */
async function checkSiteReachable(domain, timeoutMs = 15000) {
  console.log(`[site-checker] Checking reachability: ${domain}`);

  // ── Step 1: DNS lookup ────────────────────────────────────────────────────
  const dnsResult = await dnsLookup(domain, 5000);

  if (!dnsResult.ok) {
    const reason = dnsResult.code === 'ENOTFOUND'
      ? `DNS not found — domain does not exist or has no DNS records`
      : `DNS lookup failed: ${dnsResult.error}`;

    console.log(`[site-checker] ❌ DEAD (DNS) ${domain}: ${reason}`);
    return {
      alive: false,
      status: null,
      ip: null,
      error: dnsResult.error,
      errorCode: dnsResult.code,
      reason,
      checkedUrl: `https://${domain}`,
    };
  }

  console.log(`[site-checker] DNS OK — ${domain} resolves to ${dnsResult.ip}`);

  // ── Step 2: HTTPS probe ───────────────────────────────────────────────────
  const probeTimeoutMs = Math.max(timeoutMs - 5000, 8000);
  const httpsResult = await httpProbe(`https://${domain}`, probeTimeoutMs);

  if (httpsResult.alive) {
    const msg = httpsResult.sslIssue
      ? `alive with SSL issue (${httpsResult.code}) — status ${httpsResult.status || 'unknown'}`
      : `alive — HTTP ${httpsResult.status}`;
    console.log(`[site-checker] ✅ ${domain} ${msg}`);
    return {
      alive: true,
      status: httpsResult.status,
      ip: dnsResult.ip,
      error: httpsResult.sslIssue ? httpsResult.error : null,
      errorCode: httpsResult.sslIssue ? httpsResult.code : null,
      reason: msg,
      checkedUrl: httpsResult.url,
      sslIssue: httpsResult.sslIssue || false,
    };
  }

  // ── Step 3: HTTP fallback ─────────────────────────────────────────────────
  console.log(`[site-checker] HTTPS failed (${httpsResult.code}), trying HTTP...`);
  const httpResult = await httpProbe(`http://${domain}`, probeTimeoutMs);

  if (httpResult.alive) {
    console.log(`[site-checker] ✅ ${domain} alive on HTTP — status ${httpResult.status}`);
    return {
      alive: true,
      status: httpResult.status,
      ip: dnsResult.ip,
      error: null,
      errorCode: null,
      reason: `alive on HTTP — status ${httpResult.status}`,
      checkedUrl: httpResult.url,
    };
  }

  // ── Both failed ───────────────────────────────────────────────────────────
  const finalError = httpsResult.error || httpResult.error;
  const finalCode = httpsResult.code || httpResult.code;
  const reason = `Server not responding on HTTPS or HTTP — ${finalError}`;
  console.log(`[site-checker] ❌ DEAD ${domain}: ${reason}`);

  return {
    alive: false,
    status: null,
    ip: dnsResult.ip,
    error: finalError,
    errorCode: finalCode,
    reason,
    checkedUrl: `https://${domain}`,
  };
}

module.exports = { checkSiteReachable };