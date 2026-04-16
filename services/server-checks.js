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

async function checkSPF(domain) {
  try {
    const records = await resolveTXT(domain);
    const spfRecords = records.filter(r => r.toLowerCase().startsWith('v=spf1'));

    if (spfRecords.length === 0) {
      return { status: 'Missing', detail: '' };
    }

    if (spfRecords.length > 1) {
      return { status: 'FAIL', detail: 'Multiple SPF records found (permerror)' };
    }

    const record = spfRecords[0];
    const lookupMechanisms = (record.match(/\b(include:[^\s]+|a\b|a:[^\s]+|mx\b|mx:[^\s]+|ptr\b|ptr:[^\s]+|exists:[^\s]+|redirect=[^\s]+)/gi) || []);

    if (lookupMechanisms.length > 10) {
      return { status: 'WARN', detail: `Lookup limit risk (${lookupMechanisms.length} mechanisms)` };
    }

    if (/\+all(\s|$)/i.test(record)) {
      return { status: 'WARN', detail: '+all detected (spoofing risk)' };
    }

    return { status: 'OK', detail: record };
  } catch (err) {
    return { status: 'Error', detail: err.message };
  }
}

// ─── DMARC check ─────────────────────────────────────────────────────────────

async function checkDMARC(domain) {
  try {
    const records = await resolveTXT(`_dmarc.${domain}`);
    const dmarcRecords = records.filter(r => /v=DMARC1/i.test(r));

    if (dmarcRecords.length === 0) {
      return { status: 'Missing', detail: '' };
    }

    if (dmarcRecords.length > 1) {
      return { status: 'FAIL', detail: 'Multiple DMARC records found' };
    }

    const record = dmarcRecords[0];
    const tags = {};
    record.split(';').forEach(part => {
      const trimmed = part.trim();
      const eq = trimmed.indexOf('=');
      if (eq === -1) return;
      const key = trimmed.slice(0, eq).trim().toLowerCase();
      const val = trimmed.slice(eq + 1).trim();
      tags[key] = val;
    });

    if (!tags['v'] || tags['v'].toUpperCase() !== 'DMARC1') {
      return { status: 'FAIL', detail: 'Invalid v= tag' };
    }

    if (!tags['p']) {
      return { status: 'FAIL', detail: 'Missing required p= tag' };
    }

    const policy = tags['p'].toLowerCase();
    if (!['none', 'quarantine', 'reject'].includes(policy)) {
      return { status: 'FAIL', detail: `Invalid p= value "${tags['p']}"` };
    }

    if (['quarantine', 'reject'].includes(policy) && !tags['rua'] && !tags['ruf']) {
      return { status: 'WARN', detail: `p=${policy} but no reporting address` };
    }

    return { status: 'OK', detail: record };
  } catch (err) {
    return { status: 'Error', detail: err.message };
  }
}

// ─── DKIM check ──────────────────────────────────────────────────────────────

async function checkDKIM(domain) {
  try {
    const selectors = ['default', 'google', 'mail', 'k1', 'selector1', 'selector2', 'dkim'];

    for (const selector of selectors) {
      const records = await resolveTXT(`${selector}._domainkey.${domain}`);
      const dkimRecord = records.find(r => /v=DKIM1/i.test(r));
      if (dkimRecord) {
        return { status: 'OK', detail: `${selector}: ${dkimRecord.substring(0, 100)}...` };
      }
    }

    return { status: 'Missing', detail: '' };
  } catch (err) {
    return { status: 'Error', detail: err.message };
  }
}

// ─── MX check ────────────────────────────────────────────────────────────────

async function checkMX(domain) {
  try {
    const mxRecords = await resolveMX(domain);

    if (mxRecords.length === 0) {
      const aRecords = await resolveA(domain);
      if (aRecords.length > 0) {
        return { status: 'WARN', detail: 'No MX records; A-record fallback possible' };
      }
      return { status: 'Missing', detail: 'No MX records' };
    }

    const nullMX = mxRecords.find(r => r.exchange === '.' || r.exchange === '');
    if (nullMX) {
      return { status: 'FAIL', detail: 'Null MX present' };
    }

    const issues = [];
    for (const mx of mxRecords) {
      const exchange = mx.exchange.replace(/\.$/, '');
      if (net.isIP(exchange)) {
        issues.push(`MX target is IP: ${exchange}`);
        continue;
      }

      const aRecords = await resolveA(exchange);
      const aaaaRecords = await resolveAAAA(exchange);

      if (aRecords.length === 0 && aaaaRecords.length === 0) {
        issues.push(`${exchange} does not resolve`);
      }
    }

    if (issues.length > 0) {
      return { status: 'WARN', detail: issues.join('; ') };
    }

    return {
      status: 'OK',
      detail: mxRecords.map(r => `${r.priority} ${r.exchange}`).join(', '),
    };
  } catch (err) {
    return { status: 'Error', detail: err.message };
  }
}

// ─── Domain Blacklist (DBL) ─────────────────────────────────────────────────

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

async function checkHTTP(domain) {
  return new Promise((resolve) => {
    let request = null;
    const timeout = setTimeout(() => {
      if (request) {
        request.destroy();
      }
      resolve({ status: 'Timeout', code: '000' });
    }, 10000);

    if (typeof timeout.unref === 'function') timeout.unref();

    const options = {
      hostname: domain,
      path: '/',
      method: 'HEAD',
      timeout: 10000,
      headers: { 'User-Agent': 'ssl-checker-tool/1.0' },
    };

    request = https.request(options, (res) => {
      clearTimeout(timeout);
      resolve({
        status: res.statusCode >= 200 && res.statusCode < 400 ? 'OK' : 'Missing',
        code: String(res.statusCode),
      });
    });

    request.on('error', () => {
      clearTimeout(timeout);
      resolve({ status: 'Missing', code: '000' });
    });

    request.end();
  });
}

// ─── SSL check ───────────────────────────────────────────────────────────────

async function checkSSL(domain) {
  return new Promise((resolve) => {
    let socket = null;
    const timeout = setTimeout(() => {
      if (socket) {
        socket.destroy();
      }
      resolve({ status: 'Timeout', detail: 'Connection timeout' });
    }, 10000);

    if (typeof timeout.unref === 'function') timeout.unref();

    socket = tls.connect(
      { host: domain, port: 443, servername: domain, timeout: 10000 },
      () => {
        clearTimeout(timeout);
        try {
          const cert = socket.getPeerCertificate();
          if (!cert || !cert.subject) {
            socket.destroy();
            return resolve({ status: 'Missing', detail: 'No certificate' });
          }

          const expiry = new Date(cert.valid_to);
          const now = new Date();
          const daysLeft = Math.floor((expiry - now) / (1000 * 60 * 60 * 24));

          socket.destroy();

          if (daysLeft < 0) {
            resolve({ status: 'FAIL', detail: `Expired ${Math.abs(daysLeft)} days ago` });
          } else if (daysLeft < 14) {
            resolve({ status: 'WARN', detail: `Expires in ${daysLeft} days` });
          } else {
            resolve({ status: 'OK', detail: `Valid for ${daysLeft} days` });
          }
        } catch (e) {
          socket.destroy();
          resolve({ status: 'Missing', detail: e.message });
        }
      }
    );

    socket.on('error', (err) => {
      clearTimeout(timeout);
      resolve({ status: 'Missing', detail: err.message });
    });
  });
}

// ─── Broken Links check (with timeout) ───────────────────────────────────────

async function checkBrokenLinks(domain) {
  return new Promise(async (resolve) => {
    const timeout = setTimeout(() => {
      console.log(`[server-checks] Broken links check timed out for ${domain}`);
      resolve({ status: 'Skipped', detail: 'Check timed out (10s)' });
    }, 10000);

    if (typeof timeout.unref === 'function') timeout.unref();

    try {
      const html = await fetchHTMLWithTimeout(`https://${domain}`, 5000);
      if (!html) {
        clearTimeout(timeout);
        return resolve({ status: 'Skipped', detail: 'Could not fetch homepage' });
      }

      const linkRegex = /href=["']([^"'#?][^"']*?)["']/gi;
      const links = new Set();
      let match;
      let count = 0;
      while ((match = linkRegex.exec(html)) !== null && count < 10) {
        const href = match[1];
        if (href.startsWith('http://') || href.startsWith('https://')) {
          links.add(href);
          count++;
        } else if (href.startsWith('/')) {
          links.add(`https://${domain}${href}`);
          count++;
        }
      }

      if (links.size === 0) {
        clearTimeout(timeout);
        return resolve({ status: 'OK', detail: 'No links to check' });
      }

      const broken = [];
      const checks = Array.from(links).map(url =>
        headRequestWithTimeout(url, 3000).then(code => {
          if (code === 0 || code >= 400) {
            broken.push(url.substring(0, 50));
          }
        }).catch(() => broken.push(url.substring(0, 50)))
      );

      await Promise.all(checks);
      clearTimeout(timeout);

      if (broken.length > 0) {
        return resolve({
          status: 'Missing',
          detail: `${broken.length} broken link(s): ${broken.slice(0, 3).join(', ')}`
        });
      }
      return resolve({ status: 'OK', detail: `${links.size} links checked` });

    } catch (err) {
      clearTimeout(timeout);
      return resolve({ status: 'Skipped', detail: err.message });
    }
  });
}

function fetchHTMLWithTimeout(url, timeoutMs) {
  return new Promise(resolve => {
    let request = null;
    const timeout = setTimeout(() => {
      if (request) {
        request.destroy();
      }
      resolve(null);
    }, timeoutMs);

    if (typeof timeout.unref === 'function') timeout.unref();

    const lib = url.startsWith('https') ? https : http;
    request = lib.get(url, {
      timeout: timeoutMs,
      headers: { 'User-Agent': 'ssl-checker-tool/1.0' }
    }, (res) => {
      if (res.statusCode >= 301 && res.statusCode <= 302 && res.headers.location) {
        clearTimeout(timeout);
        return fetchHTMLWithTimeout(res.headers.location, timeoutMs).then(resolve);
      }

      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
        if (data.length > 100000) {
          res.destroy();
          clearTimeout(timeout);
          resolve(data);
        }
      });
      res.on('end', () => {
        clearTimeout(timeout);
        resolve(data);
      });
    });

    request.on('error', () => {
      clearTimeout(timeout);
      resolve(null);
    });
  });
}

function headRequestWithTimeout(url, timeoutMs) {
  return new Promise(resolve => {
    let request = null;
    const timeout = setTimeout(() => {
      if (request) {
        request.destroy();
      }
      resolve(0);
    }, timeoutMs);

    if (typeof timeout.unref === 'function') timeout.unref();

    try {
      const lib = url.startsWith('https') ? https : http;
      request = lib.request(url, {
        method: 'HEAD',
        timeout: timeoutMs,
        headers: { 'User-Agent': 'ssl-checker-tool/1.0' }
      }, (res) => {
        clearTimeout(timeout);
        resolve(res.statusCode);
      });
      request.on('error', () => {
        clearTimeout(timeout);
        resolve(0);
      });
      request.end();
    } catch (_) {
      clearTimeout(timeout);
      resolve(0);
    }
  });
}

// ─── Main entry point ─────────────────────────────────────────────────────────

async function runServerChecks(domain, context) {
  const start = Date.now();

  try {
    const aRecords = await resolveA(domain);
    const serverIP = aRecords[0] || '';

    const checkWithTimeout = (promise, name, timeoutMs = 10000) => {
      let timeoutId = null;

      const timeoutPromise = new Promise((resolve) => {
        timeoutId = setTimeout(() => {
          console.log(`[server-checks] ${name} timed out for ${domain}`);
          resolve({ status: 'Timeout', detail: 'Operation timed out' });
        }, timeoutMs);

        if (typeof timeoutId.unref === 'function') {
          timeoutId.unref();
        }
      });

      return Promise.race([promise, timeoutPromise]).finally(() => {
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
      });
    };

    const [spf, dmarc, dkim, mx, dbl, rbl, httpCheck, ssl, brokenLinks] = await Promise.all([
      checkWithTimeout(checkSPF(domain), 'SPF', 5000),
      checkWithTimeout(checkDMARC(domain), 'DMARC', 5000),
      checkWithTimeout(checkDKIM(domain), 'DKIM', 5000),
      checkWithTimeout(checkMX(domain), 'MX', 5000),
      checkWithTimeout(checkDomainBlacklist(domain), 'DBL', 5000),
      checkWithTimeout(checkRBL(serverIP), 'RBL', 5000),
      checkWithTimeout(checkHTTP(domain), 'HTTP', 10000),
      checkWithTimeout(checkSSL(domain), 'SSL', 10000),
      checkWithTimeout(checkBrokenLinks(domain), 'BrokenLinks', 15000),
    ]);

    const data = {
      ip: serverIP,
      spf: spf.status,
      spf_detail: spf.detail,
      dmarc: dmarc.status,
      dmarc_detail: dmarc.detail,
      dkim: dkim.status,
      dkim_detail: dkim.detail,
      domain_blacklist: dbl.status,
      mx: mx.status,
      mx_detail: mx.detail,
      smtp_mx: 'N/A',
      rbl: rbl.status,
      http: httpCheck.status,
      http_code: httpCheck.code,
      ssl: ssl.status,
      ssl_detail: ssl.detail,
      broken_links: brokenLinks.status,
      broken_links_detail: brokenLinks.detail,
    };

    return {
      status: 'SUCCESS',
      data,
      error: null,
      errorCode: null,
      duration_ms: Date.now() - start,
    };

  } catch (err) {
    console.error(`[server-checks] Error for ${domain}:`, err.message);
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
      duration_ms: Date.now() - start,
    };
  }
}

module.exports = { runServerChecks };