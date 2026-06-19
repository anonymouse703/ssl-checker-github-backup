// utils/site-checker.js
'use strict';

const https = require('https');
const http = require('http');
const dns = require('dns');

/**
 * Fatal DNS errors: these usually mean the domain name itself is bad/missing.
 */
const FATAL_DNS_CODES = new Set([
  'ENOTFOUND',
  'ENODATA',
]);

/**
 * Temporary DNS/network errors: these should NOT make FileMaker mark the domain dead.
 * They can happen because of resolver limits, firewall/WAF behavior, network routing,
 * or temporary provider failure.
 */
const NON_FATAL_DNS_CODES = new Set([
  'EAI_AGAIN',
  'ESERVFAIL',
  'ETIMEOUT',
  'ETIMEDOUT',
  'ENETUNREACH',
  'EHOSTUNREACH',
]);

/**
 * Errors that can be caused by WAF/bot protection or scanner networking.
 * Treat these as uncertain/scanner-blocked unless another probe proves the site alive.
 */
const NON_FATAL_PROBE_CODES = new Set([
  'ECONNRESET',
  'ETIMEDOUT',
  'ETIMEOUT',
  'EAI_AGAIN',
  'ESERVFAIL',
  'ENETUNREACH',
  'EHOSTUNREACH',
  'ERR_SOCKET_CLOSED',
  'ERR_HTTP2_PROTOCOL_ERROR',
  'HPE_INVALID_HEADER_TOKEN',
  'HPE_INVALID_CONSTANT',
]);

/**
 * Errors that usually mean there is no web service listening.
 * Only treat as fatal after BOTH https and http fail this way.
 */
const FATAL_PROBE_CODES = new Set([
  'ECONNREFUSED',
]);

/**
 * SSL error codes that still mean the site IS alive.
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
 * HTTP statuses that prove the website/server exists even if the scanner is blocked.
 */
function isReachableStatus(status) {
  const n = Number(status || 0);
  if (!n) return false;
  if (n >= 200 && n < 400) return true;

  // Login/bot/WAF/rate-limit responses. These are NOT dead domains.
  return n === 401 || n === 403 || n === 405 || n === 406 || n === 407 || n === 408 || n === 409 || n === 412 || n === 418 || n === 429 || n === 500 || n === 502 || n === 503 || n === 504 || (n >= 520 && n <= 526);
}

function isWafLikeStatus(status) {
  const n = Number(status || 0);
  return n === 401 || n === 403 || n === 405 || n === 406 || n === 407 || n === 408 || n === 409 || n === 412 || n === 418 || n === 429 || n === 500 || n === 502 || n === 503 || n === 504 || (n >= 520 && n <= 526);
}

function dnsLookup(domain, timeoutMs = 5000) {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      resolve({ ok: false, error: 'DNS timeout', code: 'ETIMEDOUT', ip: null, fatal: false, uncertain: true });
    }, timeoutMs);

    dns.lookup(domain, (err, address) => {
      clearTimeout(timer);

      if (err) {
        const code = err.code || 'DNS_ERROR';
        resolve({
          ok: false,
          error: err.message,
          code,
          ip: null,
          fatal: FATAL_DNS_CODES.has(code),
          uncertain: NON_FATAL_DNS_CODES.has(code) || !FATAL_DNS_CODES.has(code),
        });
      } else {
        resolve({ ok: true, error: null, code: null, ip: address, fatal: false, uncertain: false });
      }
    });
  });
}

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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
          },
          rejectUnauthorized: false,
        },
        (res) => {
          res.resume();
          const status = res.statusCode;
          req.destroy();

          if (isReachableStatus(status)) {
            done({
              alive: true,
              status,
              error: null,
              code: null,
              url,
              fatal: false,
              scannerBlocked: isWafLikeStatus(status),
              reason: isWafLikeStatus(status)
                ? `server responded with WAF/bot-style HTTP ${status}`
                : `server responded with HTTP ${status}`,
            });
            return;
          }

          // 404/410 are real HTTP responses but normally not viable landing pages.
          // 5xx can be origin/WAF trouble; leave final decision to browser/homepage check.
          done({
            alive: false,
            status,
            error: `HTTP ${status}`,
            code: String(status || ''),
            url,
            fatal: status === 404 || status === 410 || status === 444,
            uncertain: status >= 500,
            scannerBlocked: status >= 500,
            reason: status >= 500
              ? `server returned HTTP ${status}; scanner/origin/WAF uncertain`
              : `server returned HTTP ${status}`,
          });
        }
      );

      req.on('timeout', () => {
        req.destroy();
        done({
          alive: false,
          status: null,
          error: 'Connection timed out',
          code: 'ETIMEDOUT',
          url,
          fatal: false,
          uncertain: true,
          scannerBlocked: true,
          reason: 'scanner timed out; do not mark domain dead without browser proof',
        });
      });

      req.on('error', (err) => {
        const code = err.code || 'UNKNOWN';

        if (SSL_BUT_ALIVE_CODES.has(code) || code.startsWith('ERR_TLS') || code.startsWith('ERR_SSL')) {
          done({
            alive: true,
            status: null,
            error: err.message,
            code,
            url,
            sslIssue: true,
            fatal: false,
            reason: `alive with SSL issue (${code})`,
          });
          return;
        }

        if (FATAL_PROBE_CODES.has(code)) {
          done({ alive: false, status: null, error: err.message, code, url, fatal: true, uncertain: false, reason: `fatal probe error: ${code}` });
          return;
        }

        // WAFs and bot filters often reset or stall automated requests. Do not call this dead.
        done({
          alive: false,
          status: null,
          error: err.message,
          code,
          url,
          fatal: false,
          uncertain: true,
          scannerBlocked: NON_FATAL_PROBE_CODES.has(code) || true,
          reason: `scanner probe failed (${code}); do not mark domain dead without browser proof`,
        });
      });
    } catch (err) {
      done({
        alive: false,
        status: null,
        error: err.message,
        code: err.code || 'UNKNOWN',
        url,
        fatal: false,
        uncertain: true,
        scannerBlocked: true,
        reason: `scanner exception; do not mark domain dead without browser proof`,
      });
    }
  });
}

async function checkSiteReachable(domain, timeoutMs = 15000) {
  console.log(`[site-checker] Checking reachability: ${domain}`);

  const dnsResult = await dnsLookup(domain, 5000);

  if (!dnsResult.ok) {
    if (dnsResult.fatal) {
      const reason = `DNS not found — domain does not exist or has no DNS records`;
      console.log(`[site-checker] ❌ DEAD (DNS) ${domain}: ${reason}`);
      return {
        alive: false,
        status: 'DNS_FAILED',
        statusCode: null,
        ip: null,
        error: dnsResult.error,
        errorCode: dnsResult.code,
        reason,
        checkedUrl: `https://${domain}`,
        fatal: true,
        uncertain: false,
        scannerBlocked: false,
      };
    }

    const reason = `DNS temporary/scanner failure: ${dnsResult.error}`;
    console.log(`[site-checker] ⚠️ UNCERTAIN (DNS) ${domain}: ${reason}`);
    return {
      alive: null,
      status: 'DNS_UNCERTAIN',
      statusCode: null,
      ip: null,
      error: dnsResult.error,
      errorCode: dnsResult.code,
      reason,
      checkedUrl: `https://${domain}`,
      fatal: false,
      uncertain: true,
      scannerBlocked: true,
    };
  }

  console.log(`[site-checker] DNS OK — ${domain} resolves to ${dnsResult.ip}`);

  const probeTimeoutMs = Math.max(timeoutMs - 5000, 8000);
  const httpsResult = await httpProbe(`https://${domain}`, probeTimeoutMs);

  if (httpsResult.alive) {
    const msg = httpsResult.reason || (httpsResult.sslIssue
      ? `alive with SSL issue (${httpsResult.code}) — status ${httpsResult.status || 'unknown'}`
      : `alive — HTTP ${httpsResult.status}`);

    console.log(`[site-checker] ✅ ${domain} ${msg}`);
    return {
      alive: true,
      status: httpsResult.scannerBlocked ? 'REACHABLE_WAF_BLOCKED' : 'REACHABLE',
      statusCode: httpsResult.status,
      ip: dnsResult.ip,
      error: httpsResult.sslIssue ? httpsResult.error : null,
      errorCode: httpsResult.sslIssue ? httpsResult.code : null,
      reason: msg,
      checkedUrl: httpsResult.url,
      sslIssue: httpsResult.sslIssue || false,
      fatal: false,
      uncertain: false,
      scannerBlocked: !!httpsResult.scannerBlocked,
    };
  }

  console.log(`[site-checker] HTTPS did not prove reachable (${httpsResult.code || httpsResult.status || 'no code'}), trying HTTP...`);
  const httpResult = await httpProbe(`http://${domain}`, probeTimeoutMs);

  if (httpResult.alive) {
    const msg = httpResult.reason || `alive on HTTP — status ${httpResult.status}`;
    console.log(`[site-checker] ✅ ${domain} ${msg}`);
    return {
      alive: true,
      status: httpResult.scannerBlocked ? 'REACHABLE_WAF_BLOCKED' : 'REACHABLE',
      statusCode: httpResult.status,
      ip: dnsResult.ip,
      error: null,
      errorCode: null,
      reason: msg,
      checkedUrl: httpResult.url,
      fatal: false,
      uncertain: false,
      scannerBlocked: !!httpResult.scannerBlocked,
    };
  }

  const errors = [httpsResult, httpResult];
  const bothFatalRefused = errors.every((r) => r && r.fatal === true && FATAL_PROBE_CODES.has(r.code));
  const hardHttp = errors.find((r) => r && r.fatal === true && ['404', '410', '444'].includes(String(r.code || r.status || '')));

  if (bothFatalRefused || hardHttp) {
    const final = hardHttp || httpsResult || httpResult;
    const reason = hardHttp
      ? `hard HTTP failure: ${final.code || final.status}`
      : `server refused both HTTPS and HTTP`;

    console.log(`[site-checker] ❌ DEAD ${domain}: ${reason}`);
    return {
      alive: false,
      status: 'UNREACHABLE',
      statusCode: final.status || null,
      ip: dnsResult.ip,
      error: final.error,
      errorCode: final.code || final.status || 'ERR_UNREACHABLE',
      reason,
      checkedUrl: `https://${domain}`,
      fatal: true,
      uncertain: false,
      scannerBlocked: false,
    };
  }

  const finalError = httpsResult.error || httpResult.error;
  const finalCode = httpsResult.code || httpResult.code || httpsResult.status || httpResult.status || 'SCANNER_UNCERTAIN';
  const reason = `scanner could not prove reachability (${finalCode}); do not mark domain dead without browser proof`;

  console.log(`[site-checker] ⚠️ UNCERTAIN ${domain}: ${reason}`);
  return {
    alive: null,
    status: 'SCAN_UNCERTAIN',
    statusCode: httpsResult.status || httpResult.status || null,
    ip: dnsResult.ip,
    error: finalError,
    errorCode: finalCode,
    reason,
    checkedUrl: `https://${domain}`,
    fatal: false,
    uncertain: true,
    scannerBlocked: true,
  };
}

module.exports = { checkSiteReachable };
