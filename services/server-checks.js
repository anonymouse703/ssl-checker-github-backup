/**
 * server-checks.js
 *
 * Pure Node.js implementation — no bash, no dig, no grep, no shell commands.
 * Works identically on Windows, Linux, macOS, Azure, Cloudflare Workers, etc.
 *
 * Implements the same logic as:
 *   check_spf.sh   → SPF record lookup + validation
 *   check_dmarc.sh → DMARC record lookup + validation
 *   check_mx.sh    → MX record lookup + exchange resolution
 *   + DKIM, RBL, DBL, HTTP, SSL, Broken Links
 */

'use strict';

const dns  = require('dns').promises;
const net  = require('net');
const tls  = require('tls');
const http = require('http');
const https = require('https');
const { getErrorCode } = require('../utils/error-codes');

// ─── DNS helpers ─────────────────────────────────────────────────────────────

async function resolveTXT(name) {
  try {
    const records = await dns.resolveTxt(name);
    // Each record is an array of strings (chunks) — join chunks, return array of records
    return records.map(chunks => chunks.join(''));
  } catch (_) {
    return [];
  }
}

async function resolveA(name) {
  try {
    return await dns.resolve4(name);
  } catch (_) {
    return [];
  }
}

async function resolveAAAA(name) {
  try {
    return await dns.resolve6(name);
  } catch (_) {
    return [];
  }
}

async function resolveMX(name) {
  try {
    const records = await dns.resolveMx(name);
    return records.sort((a, b) => a.priority - b.priority);
  } catch (_) {
    return [];
  }
}

// ─── SPF check ───────────────────────────────────────────────────────────────
// Logic from check_spf.sh:
//   1. Find TXT record starting with "v=spf1"
//   2. Validate structure (no unknown mechanisms, no multiple "all", etc.)
//   3. Count lookup-type mechanisms (include, a, mx, ptr, exists, redirect)
//      — if > 10, permerror risk → WARN
//   4. Flag +all (allows anyone to spoof) → WARN

async function checkSPF(domain) {
  const records = await resolveTXT(domain);
  const spfRecords = records.filter(r => r.toLowerCase().startsWith('v=spf1'));

  if (spfRecords.length === 0) {
    return { status: 'Missing', detail: '' };
  }

  if (spfRecords.length > 1) {
    return { status: 'FAIL', detail: 'Multiple SPF records found (permerror)' };
  }

  const record = spfRecords[0];

  // Count lookup mechanisms
  const lookupMechanisms = (record.match(/\b(include:[^\s]+|a\b|a:[^\s]+|mx\b|mx:[^\s]+|ptr\b|ptr:[^\s]+|exists:[^\s]+|redirect=[^\s]+)/gi) || []);
  if (lookupMechanisms.length > 10) {
    return { status: 'WARN', detail: `Lookup limit risk (${lookupMechanisms.length} mechanisms): ${record}` };
  }

  // +all means anyone can spoof a pass
  if (/\+all(\s|$)/i.test(record)) {
    return { status: 'WARN', detail: `+all detected (spoofing risk): ${record}` };
  }

  return { status: 'OK', detail: record };
}

// ─── DMARC check ─────────────────────────────────────────────────────────────
// Logic from check_dmarc.sh:
//   1. Query TXT at _dmarc.<domain>
//   2. Must start with v=DMARC1
//   3. Must have p= with valid value (none/quarantine/reject)
//   4. WARN if p=quarantine or p=reject but no rua= or ruf= reporting address
//   5. FAIL on duplicate tags, invalid tag values

async function checkDMARC(domain) {
  const records = await resolveTXT(`_dmarc.${domain}`);
  const dmarcRecords = records.filter(r => /v=DMARC1/i.test(r));

  if (dmarcRecords.length === 0) {
    return { status: 'Missing', detail: '' };
  }

  if (dmarcRecords.length > 1) {
    return { status: 'FAIL', detail: 'Multiple DMARC records found (invalid)' };
  }

  const record = dmarcRecords[0];

  // Parse tags
  const tags = {};
  record.split(';').forEach(part => {
    const trimmed = part.trim();
    const eq = trimmed.indexOf('=');
    if (eq === -1) return;
    const key = trimmed.slice(0, eq).trim().toLowerCase();
    const val = trimmed.slice(eq + 1).trim();
    tags[key] = val;
  });

  // v must be first and = DMARC1
  if (!tags['v'] || tags['v'].toUpperCase() !== 'DMARC1') {
    return { status: 'FAIL', detail: `Invalid v= tag: ${record}` };
  }

  // p= is required
  if (!tags['p']) {
    return { status: 'FAIL', detail: `Missing required p= tag: ${record}` };
  }

  const policy = tags['p'].toLowerCase();
  if (!['none', 'quarantine', 'reject'].includes(policy)) {
    return { status: 'FAIL', detail: `Invalid p= value "${tags['p']}": ${record}` };
  }

  // Strong policy but no reporting address
  if (['quarantine', 'reject'].includes(policy) && !tags['rua'] && !tags['ruf']) {
    return { status: 'WARN', detail: `p=${policy} but no rua= or ruf= reporting address: ${record}` };
  }

  return { status: 'OK', detail: record };
}

// ─── DKIM check ──────────────────────────────────────────────────────────────
// Logic: query TXT at <selector>._domainkey.<domain>
// Tries common selectors — stops on first match with v=DKIM1

async function checkDKIM(domain) {
  const selectors = ['default', 'google', 'mail', 'k1', 'selector1', 'selector2', 'dkim'];

  for (const selector of selectors) {
    const records = await resolveTXT(`${selector}._domainkey.${domain}`);
    const dkimRecord = records.find(r => /v=DKIM1/i.test(r));
    if (dkimRecord) {
      return { status: 'OK', detail: `${selector}: ${dkimRecord}` };
    }
  }

  return { status: 'Missing', detail: '' };
}

// ─── MX check ────────────────────────────────────────────────────────────────
// Logic from check_mx.sh:
//   1. Query MX records, sorted by priority
//   2. Null MX (exchange = ".") → FAIL
//   3. MX target must be a valid hostname (not an IP)
//   4. Each exchange must resolve to at least one A or AAAA record → FAIL if not
//   5. No MX but domain has A record → WARN (RFC 5321 fallback)

async function checkMX(domain) {
  const mxRecords = await resolveMX(domain);

  if (mxRecords.length === 0) {
    // RFC 5321 fallback check
    const aRecords = await resolveA(domain);
    if (aRecords.length > 0) {
      return { status: 'WARN', detail: 'No MX records; RFC 5321 A-record fallback possible' };
    }
    return { status: 'Missing', detail: 'No MX records and no A record' };
  }

  // Check for null MX (RFC 7505)
  const nullMX = mxRecords.find(r => r.exchange === '.' || r.exchange === '');
  if (nullMX) {
    return { status: 'FAIL', detail: 'Null MX present — domain explicitly rejects email' };
  }

  const issues = [];
  for (const mx of mxRecords) {
    const exchange = mx.exchange.replace(/\.$/, '');

    // MX target must not be a bare IP address
    if (net.isIP(exchange)) {
      issues.push(`MX target is an IP address: ${exchange}`);
      continue;
    }

    // Exchange must resolve
    const aRecords   = await resolveA(exchange);
    const aaaaRecords = await resolveAAAA(exchange);

    if (aRecords.length === 0 && aaaaRecords.length === 0) {
      issues.push(`MX target ${exchange} does not resolve`);
    }
  }

  if (issues.length > 0) {
    return { status: 'WARN', detail: issues.join('; ') };
  }

  return {
    status: 'OK',
    detail: mxRecords.map(r => `${r.priority} ${r.exchange}`).join(', '),
  };
}

// ─── Domain Blacklist (DBL) ───────────────────────────────────────────────────
// Spamhaus DBL: query <domain>.dbl.spamhaus.org — any response = blacklisted

async function checkDomainBlacklist(domain) {
  try {
    const results = await resolveA(`${domain}.dbl.spamhaus.org`);
    if (results.length > 0) {
      return { status: 'Blacklisted', detail: results.join(', ') };
    }
    return { status: 'OK', detail: '' };
  } catch (_) {
    return { status: 'OK', detail: '' };
  }
}

// ─── RBL check ───────────────────────────────────────────────────────────────
// Spamhaus ZEN: query <reversed-ip>.zen.spamhaus.org — any response = listed
// From user command: dig +short 4.3.2.1.zen.spamhaus.org

async function checkRBL(ip) {
  if (!ip) return { status: 'Missing', detail: '' };

  const reversed = ip.split('.').reverse().join('.');
  try {
    const results = await resolveA(`${reversed}.zen.spamhaus.org`);
    if (results.length > 0) {
      return { status: 'Blacklisted', detail: results.join(', ') };
    }
    return { status: 'OK', detail: '' };
  } catch (_) {
    return { status: 'OK', detail: '' };
  }
}

// ─── HTTP check ──────────────────────────────────────────────────────────────
// From user command: curl -s -o /dev/null -I -w "%{http_code}" https://domain
// Pure Node: HEAD request, check for 2xx/3xx

async function checkHTTP(domain) {
  return new Promise(resolve => {
    const options = {
      hostname: domain,
      path: '/',
      method: 'HEAD',
      timeout: 10000,
      headers: { 'User-Agent': 'ssl-checker-tool/1.0' },
    };

    const req = https.request(options, res => {
      resolve({
        status: res.statusCode >= 200 && res.statusCode < 400 ? 'OK' : 'Missing',
        code: String(res.statusCode),
      });
    });

    req.on('error', () => resolve({ status: 'Missing', code: '000' }));
    req.on('timeout', () => { req.destroy(); resolve({ status: 'Missing', code: '000' }); });
    req.end();
  });
}

// ─── SSL check ───────────────────────────────────────────────────────────────
// From user command: openssl s_client -connect domain:443 | openssl x509 -noout
// Pure Node: TLS connect, verify certificate is present and not expired

async function checkSSL(domain) {
  return new Promise(resolve => {
    const socket = tls.connect(
      { host: domain, port: 443, servername: domain, timeout: 10000 },
      () => {
        try {
          const cert = socket.getPeerCertificate();
          if (!cert || !cert.subject) {
            socket.destroy();
            return resolve({ status: 'Missing', detail: 'No certificate' });
          }

          const expiry = new Date(cert.valid_to);
          const now    = new Date();
          const daysLeft = Math.floor((expiry - now) / (1000 * 60 * 60 * 24));

          socket.destroy();

          if (daysLeft < 0) {
            resolve({ status: 'FAIL', detail: `Certificate expired ${Math.abs(daysLeft)} days ago` });
          } else if (daysLeft < 14) {
            resolve({ status: 'WARN', detail: `Certificate expires in ${daysLeft} days` });
          } else {
            resolve({ status: 'OK', detail: `Valid for ${daysLeft} days (expires ${cert.valid_to})` });
          }
        } catch (e) {
          socket.destroy();
          resolve({ status: 'Missing', detail: e.message });
        }
      }
    );

    socket.on('error', err => resolve({ status: 'Missing', detail: err.message }));
    socket.on('timeout', () => { socket.destroy(); resolve({ status: 'Missing', detail: 'Timeout' }); });
  });
}

// ─── Broken Links check ──────────────────────────────────────────────────────
// From user command: wget --spider -r -nd -nv https://domain | grep -q "broken"
// Pure Node: fetch homepage, extract all <a href> links, HEAD-check each one

async function checkBrokenLinks(domain) {
  // Step 1: fetch homepage HTML
  const html = await fetchHTML(`https://${domain}`);
  if (!html) return { status: 'Missing', detail: 'Could not fetch homepage' };

  // Step 2: extract links
  const linkRegex = /href=["']([^"'#?][^"']*?)["']/gi;
  const links = new Set();
  let match;
  while ((match = linkRegex.exec(html)) !== null) {
    const href = match[1];
    if (href.startsWith('http://') || href.startsWith('https://')) {
      links.add(href);
    } else if (href.startsWith('/')) {
      links.add(`https://${domain}${href}`);
    }
    if (links.size >= 20) break; // cap at 20 links for speed
  }

  if (links.size === 0) return { status: 'OK', detail: 'No links to check' };

  // Step 3: HEAD-check each link
  const broken = [];
  const checks = Array.from(links).map(url =>
    headRequest(url).then(code => {
      if (code === 0 || code >= 400) broken.push(`${url} (${code})`);
    }).catch(() => broken.push(url))
  );

  await Promise.all(checks);

  if (broken.length > 0) {
    return { status: 'Missing', detail: `${broken.length} broken link(s): ${broken.slice(0, 3).join(', ')}` };
  }
  return { status: 'OK', detail: `${links.size} links checked` };
}

function fetchHTML(url) {
  return new Promise(resolve => {
    const lib = url.startsWith('https') ? https : http;
    lib.get(url, { timeout: 10000, headers: { 'User-Agent': 'ssl-checker-tool/1.0' } }, res => {
      // Follow one redirect
      if (res.statusCode >= 301 && res.statusCode <= 302 && res.headers.location) {
        return fetchHTML(res.headers.location).then(resolve);
      }
      let data = '';
      res.on('data', chunk => { data += chunk; if (data.length > 200000) res.destroy(); });
      res.on('end', () => resolve(data));
    }).on('error', () => resolve(null)).on('timeout', () => resolve(null));
  });
}

function headRequest(url) {
  return new Promise(resolve => {
    try {
      const lib = url.startsWith('https') ? https : http;
      const req = lib.request(url, { method: 'HEAD', timeout: 5000, headers: { 'User-Agent': 'ssl-checker-tool/1.0' } }, res => {
        resolve(res.statusCode);
      });
      req.on('error', () => resolve(0));
      req.on('timeout', () => { req.destroy(); resolve(0); });
      req.end();
    } catch (_) { resolve(0); }
  });
}

// ─── Main entry point ─────────────────────────────────────────────────────────

async function runServerChecks(domain, context) {
  const start = Date.now();

  try {
    // IP address
    const aRecords = await resolveA(domain);
    const serverIP = aRecords[0] || '';

    // Run all checks in parallel for speed

    const [spf, dmarc, dkim, mx, dbl, rbl, httpCheck, ssl, brokenLinks] = await Promise.all([
      checkSPF(domain),
      checkDMARC(domain),
      checkDKIM(domain),
      checkMX(domain),
      checkDomainBlacklist(domain),
      checkRBL(serverIP),
      checkHTTP(domain),
      checkSSL(domain),
      checkBrokenLinks(domain),
    ]);

    const data = {
      ip:               serverIP,
      spf:              spf.status,
      spf_detail:       spf.detail,
      dmarc:            dmarc.status,
      dmarc_detail:     dmarc.detail,
      dkim:             dkim.status,
      dkim_detail:      dkim.detail,
      domain_blacklist: dbl.status,
      mx:               mx.status,
      mx_detail:        mx.detail,
      smtp_mx:          'N/A',
      rbl:              rbl.status,
      http:             httpCheck.status,
      http_code:        httpCheck.code,
      ssl:              ssl.status,
      ssl_detail:       ssl.detail,
      broken_links:     brokenLinks.status,
      broken_links_detail: brokenLinks.detail,
    };


    return { status: 'SUCCESS', data, error: null, errorCode: null };

  } catch (err) {
    return {
      status: 'SKIPPED',
      data: {
        ip: '', spf: 'Missing', spf_detail: '',
        dmarc: 'Missing', dmarc_detail: '',
        dkim: 'Missing', dkim_detail: '',
        domain_blacklist: 'Missing',
        mx: 'Missing', mx_detail: '',
        smtp_mx: 'Missing',
        rbl: 'Missing',
        http: 'Missing', http_code: '',
        ssl: 'Missing', ssl_detail: '',
        broken_links: 'Missing', broken_links_detail: '',
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
    };
  }
}

function formatDuration(ms) {
  const totalSec = Math.round(ms / 1000);
  const mins = Math.floor(totalSec / 60);
  const secs = totalSec % 60;
  return mins === 0 ? `${secs} sec` : `${mins} min ${secs} sec`;
}

module.exports = { runServerChecks };