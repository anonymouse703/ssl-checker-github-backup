'use strict';

const { getErrorCode } = require('../utils/error-codes');
const fs = require('fs');
const path = require('path');
const {
  setFixedViewport,
  captureWithRetry,
  wait,
} = require('../utils/screenshot');

const USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

const PINGDOM_VIEWPORT_WIDTH = 1064;
const PINGDOM_VIEWPORT_HEIGHT = 900;
const WANTED_LOCATION = 'North America - USA - San Francisco';
const MAX_PINGDOM_RETRIES = 3;               // each tool gets 3 attempts before N/A fallback
const PINGDOM_SCREENSHOT_RETRIES = 3;

function pingdomNAResult(errorMessage) {
  return {
    status: 'FAILED',
    data: {
      performanceGrade: 'N/A',
      gradeLetter: 'N/A',
      gradeNumber: null,
      loadTime: 'N/A',
      pageSize: 'N/A',
      requests: 'N/A',
    },
    error: errorMessage || 'Pingdom failed after retries',
    errorCode: getErrorCode({ error: errorMessage || 'Pingdom failed after retries' }),
    url: 'https://tools.pingdom.com/',
    screenshot: '',
  };
}

async function waitForFonts(page, timeout = 15000) {
  try {
    await page.evaluate(async (t) => {
      if (!document.fonts) return;
      await Promise.race([
        document.fonts.ready,
        new Promise(resolve => setTimeout(resolve, t))
      ]);
    }, timeout);
  } catch (_) {}
}

async function fillPingdomUrl(page, domain) {
  const targetUrl = `https://${domain}`;
  const urlInputSelector = 'input[placeholder="www.example.com"], input[type="text"]';

  await page.waitForSelector(urlInputSelector, { timeout: 20000 });
  await page.click(urlInputSelector, { clickCount: 3 }).catch(() => {});
  await page.keyboard.down('Control').catch(() => {});
  await page.keyboard.press('A').catch(() => {});
  await page.keyboard.up('Control').catch(() => {});
  await page.keyboard.press('Backspace').catch(() => {});
  await page.type(urlInputSelector, targetUrl, { delay: 40 });
}

async function setPingdomLocation(page) {
  const wanted = WANTED_LOCATION;

  const normalize = (s) =>
    String(s || '')
      .replace(/\s+/g, ' ')
      .replace(/[^\x20-\x7E]/g, '')
      .trim()
      .toLowerCase();

  const dropdownFound = await page.evaluate(() => {
    const labels = Array.from(document.querySelectorAll('label, span, div, p'));
    const testFromLabel = labels.find(el => /test\s*from/i.test(el.textContent || ''));
    if (!testFromLabel) return false;

    const container = testFromLabel.closest('div, form, fieldset') || testFromLabel.parentElement;
    const select = container?.querySelector('app-select');
    if (!select) return false;

    select.click();
    return true;
  });

  if (!dropdownFound) {
    throw new Error('Could not find "Test from" dropdown');
  }

  await page.waitForSelector('app-select .options', { visible: true, timeout: 5000 });
  await wait(500);

  const optionSelector = 'app-select .options .option';
  await page.waitForSelector(optionSelector, { visible: true, timeout: 5000 });

  const options = await page.$$eval(optionSelector, els =>
    els.map(el => (el.textContent || '').trim())
  );

  const wantedIndex = options.findIndex(opt => normalize(opt) === normalize(wanted));
  if (wantedIndex === -1) {
    console.error('[pingdom] Available location options:', options);
    throw new Error(`Option "${wanted}" not found in dropdown`);
  }

  const optionElements = await page.$$(optionSelector);
  await optionElements[wantedIndex].click({ delay: 100 });

  await page.waitForFunction(
    () => !document.querySelector('app-select .options'),
    { timeout: 5000 }
  ).catch(() => {});

  await page.waitForFunction(
    (wantedText) => {
      const normalizeInner = (s) =>
        String(s || '')
          .replace(/\s+/g, ' ')
          .replace(/[^\x20-\x7E]/g, '')
          .trim()
          .toLowerCase();

      const appSelects = Array.from(document.querySelectorAll('app-select'));
      return appSelects.some(el => normalizeInner(el.textContent || '') === normalizeInner(wantedText));
    },
    { timeout: 10000 },
    wanted
  );
}

async function clickPingdomStartTest(page) {
  const locationVerified = await page.evaluate((wantedText) => {
    const normalize = (s) => String(s || '').replace(/\s+/g, ' ').trim().toLowerCase();
    const wantedNorm = normalize(wantedText);

    const appSelects = Array.from(document.querySelectorAll('app-select'));
    return appSelects.some((el) => normalize(el.textContent || '') === wantedNorm);
  }, WANTED_LOCATION);

  if (!locationVerified) {
    throw new Error(`Pingdom visible location is not "${WANTED_LOCATION}" before clicking START TEST.`);
  }

  const clicked = await page.evaluate(() => {
    const elements = Array.from(
      document.querySelectorAll('input[value="START TEST"], button, input[type="submit"]')
    );
    const btn = elements.find((el) =>
      /start test/i.test((el.value || el.innerText || '').trim())
    );
    if (btn) {
      btn.click();
      return true;
    }
    return false;
  });

  if (!clicked) {
    throw new Error('Could not click START TEST.');
  }
}

async function waitForPingdomResults(page, domain) {
  console.log(`[pingdom] Waiting for test results for ${domain}...`);

  const maxWaitMs = 180000; // increased to 3 minutes
  const startTime = Date.now();

  while (Date.now() - startTime < maxWaitMs) {
    await wait(5000);

    const state = await page.evaluate(() => {
      const rawText = document.body.innerText || '';

      const pingdomFailed =
        /something went wrong/i.test(rawText) ||
        /reached your limit/i.test(rawText) ||
        /try again in a bit/i.test(rawText) ||
        /whoops/i.test(rawText);

      const cutIdx = rawText.toLowerCase().indexOf('improve page performance');
      const text = cutIdx !== -1 ? rawText.substring(0, cutIdx) : rawText;

      const hasDownloadHar = /DOWNLOAD HAR/i.test(text);
      const hasGradeWithValue = /Performance\s+grade/i.test(text) && /\b[A-F][+-]?\s*\d{1,3}\b/.test(text);
      const hasLoadTimeWithValue = /Load\s+time/i.test(text) && /\b\d+(?:\.\d+)?\s*(ms|s)\b/i.test(text);
      const hasPageSizeWithValue = /Page\s+size/i.test(text) && /\b\d+(?:\.\d+)?\s*(KB|MB|GB)\b/i.test(text);

      return {
        pingdomFailed,
        hasResults: hasDownloadHar ||
          (hasGradeWithValue && hasLoadTimeWithValue) ||
          (hasGradeWithValue && hasPageSizeWithValue),
      };
    }).catch(() => ({ hasResults: false, pingdomFailed: false }));

    if (state.pingdomFailed) {
      throw new Error('Pingdom temporary error/limit page detected');
    }

    if (state.hasResults) {
      console.log(`[pingdom] Test completed for ${domain}`);
      return;
    }
  }

  throw new Error(`Pingdom test timeout for ${domain} after 3 minutes`);
}

function isPingdomComplete(data) {
  const d = normalizePingdomParsedData(data);
  return d.gradeLetter !== 'N/A' &&
         d.gradeNumber !== null &&
         d.loadTime !== 'N/A' &&
         d.pageSize !== 'N/A';
}

function isPingdomMinimal(data) {
  const d = normalizePingdomParsedData(data);
  return d.gradeLetter !== 'N/A' || d.gradeNumber !== null;
}

function parsePingdomText(pageText) {
  let textToParse = pageText;
  const improveIndex = (pageText || '').toLowerCase().indexOf('improve page performance');
  if (improveIndex !== -1) {
    textToParse = pageText.substring(0, improveIndex);
  }

  const raw = String(textToParse || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '\n')
    .replace(/\t/g, ' ');

  const clean = (v) => String(v || '').replace(/\s+/g, ' ').trim();
  const lines = raw
    .split('\n')
    .map(clean)
    .filter(Boolean);

  const compact = clean(raw);

  const isLabel = (v) => /^(Performance\s*grade|Page\s*size|Load\s*time|Requests|Your\s+Results|Download\s+HAR|Share\s+Result)$/i.test(clean(v));
  const scoreToLetter = (score) => {
    const n = Number(score);
    if (!Number.isFinite(n)) return 'N/A';
    if (n >= 90) return 'A';
    if (n >= 80) return 'B';
    if (n >= 70) return 'C';
    if (n >= 60) return 'D';
    return 'F';
  };
  const validScore = (v) => {
    const m = clean(v).match(/\b(100|[1-9]?\d)\b/);
    if (!m) return null;
    const n = Number(m[1]);
    return Number.isFinite(n) && n >= 0 && n <= 100 ? n : null;
  };

  let performanceGrade = 'N/A';
  let gradeLetter = 'N/A';
  let gradeNumber = null;
  let pageSize = 'N/A';
  let loadTime = 'N/A';
  let requests = 'N/A';

  function lineIndex(regex) {
    return lines.findIndex((line) => regex.test(line));
  }

  function nextLinesAfter(regex, count = 10) {
    const idx = lineIndex(regex);
    if (idx === -1) return [];
    return lines.slice(idx, idx + count + 1);
  }

  function parseGradeFromText(text) {
    const t = clean(text);
    let m = t.match(/Performance\s*grade\s*([A-F][+-]?)\s*(100|[1-9]?\d)?/i);
    if (m) {
      const letter = String(m[1]).toUpperCase();
      const score = m[2] ? validScore(m[2]) : null;
      return { letter, score };
    }

    m = t.match(/\b([A-F][+-]?)\b\s*(100|[1-9]?\d)\b/i);
    if (m) {
      const score = validScore(m[2]);
      if (score !== null) return { letter: String(m[1]).toUpperCase(), score };
    }

    return null;
  }

  const gradeWindow = nextLinesAfter(/Performance\s*grade/i, 12);
  if (gradeWindow.length) {
    const joined = gradeWindow.join(' ');
    let g = parseGradeFromText(joined);
    if (!g) {
      let foundLetter = null;
      let foundScore = null;
      for (const line of gradeWindow.slice(1)) {
        if (isLabel(line)) continue;
        const letterOnly = line.match(/^([A-F][+-]?)$/i);
        if (!foundLetter && letterOnly) {
          foundLetter = letterOnly[1].toUpperCase();
          continue;
        }
        const score = validScore(line);
        if (score !== null) {
          foundScore = score;
          break;
        }
      }
      if (foundLetter || foundScore !== null) {
        g = {
          letter: foundLetter || scoreToLetter(foundScore),
          score: foundScore,
        };
      }
    }
    if (g) {
      gradeLetter = g.letter || 'N/A';
      gradeNumber = g.score;
    }
  }

  const sizeWindow = nextLinesAfter(/Page\s*size/i, 8).join(' ');
  let m = sizeWindow.match(/([\d.]+)\s*(KB|MB|GB)\b/i) || compact.match(/Page\s*size\s*([\d.]+)\s*(KB|MB|GB)\b/i);
  if (m) pageSize = `${m[1]} ${String(m[2]).toUpperCase()}`;

  const loadWindow = nextLinesAfter(/Load\s*time/i, 8).join(' ');
  m = loadWindow.match(/([\d.]+)\s*(ms|s)\b/i) || compact.match(/Load\s*time\s*([\d.]+)\s*(ms|s)\b/i);
  if (m) loadTime = `${m[1]} ${String(m[2]).toLowerCase()}`;

  const requestWindowLines = nextLinesAfter(/\bRequests\b/i, 8);
  if (requestWindowLines.length) {
    const candidates = [];
    for (const line of requestWindowLines) {
      if (isLabel(line)) continue;
      const nums = Array.from(line.matchAll(/\b(\d{1,4})\b/g)).map(x => Number(x[1]));
      for (const n of nums) {
        if (Number.isFinite(n) && n > 0 && n <= 1000) candidates.push(n);
      }
    }
    if (candidates.length) requests = String(candidates[0]);
  }

  if (requests === 'N/A') {
    m = compact.match(/Requests\s*(\d{1,4})\b/i);
    if (m) {
      const n = Number(m[1]);
      if (Number.isFinite(n) && n > 0 && n <= 1000) requests = String(n);
    }
  }

  if (gradeLetter === 'N/A' || gradeNumber === null || pageSize === 'N/A' || loadTime === 'N/A' || requests === 'N/A') {
    const resultStart = lines.findIndex((line) => /Your\s+Results/i.test(line));
    const resultLines = resultStart >= 0 ? lines.slice(resultStart, resultStart + 40) : lines;
    const resultText = resultLines.join(' ');

    if (gradeLetter === 'N/A' || gradeNumber === null) {
      const g = parseGradeFromText(resultText);
      if (g) {
        gradeLetter = g.letter || gradeLetter;
        gradeNumber = g.score !== null ? g.score : gradeNumber;
      }
    }

    if (pageSize === 'N/A') {
      m = resultText.match(/([\d.]+)\s*(KB|MB|GB)\b/i);
      if (m) pageSize = `${m[1]} ${String(m[2]).toUpperCase()}`;
    }
    if (loadTime === 'N/A') {
      m = resultText.match(/([\d.]+)\s*(ms|s)\b/i);
      if (m) loadTime = `${m[1]} ${String(m[2]).toLowerCase()}`;
    }
    if (requests === 'N/A') {
      const reqMatch = resultText.match(/Requests\s*(\d{1,4})\b/i);
      if (reqMatch) {
        const n = Number(reqMatch[1]);
        if (n > 0 && n <= 1000) requests = String(n);
      }
    }
  }

  if (gradeLetter !== 'N/A' && gradeNumber === null) {
    performanceGrade = gradeLetter;
  } else if (gradeNumber !== null) {
    if (gradeLetter === 'N/A') gradeLetter = scoreToLetter(gradeNumber);
    performanceGrade = `${gradeLetter}${gradeNumber}`;
  }

  return {
    performanceGrade,
    gradeLetter,
    gradeNumber,
    loadTime,
    pageSize,
    requests,
  };
}

function pingdomEmptyData() {
  return {
    performanceGrade: 'N/A',
    gradeLetter: 'N/A',
    gradeNumber: null,
    loadTime: 'N/A',
    pageSize: 'N/A',
    requests: 'N/A',
  };
}

function isValuePresent(v) {
  return v !== null && v !== undefined && String(v).trim() !== '' && String(v).trim().toUpperCase() !== 'N/A';
}

function normalizeRequestCount(v) {
  if (!isValuePresent(v)) return 'N/A';
  const m = String(v).match(/\b(\d{1,4})\b/);
  if (!m) return 'N/A';
  const n = Number(m[1]);
  if (!Number.isFinite(n) || n <= 0 || n > 1000) return 'N/A';
  return String(n);
}

function normalizePingdomParsedData(data) {
  const d = data || {};
  const out = pingdomEmptyData();

  if (isValuePresent(d.gradeLetter)) {
    const m = String(d.gradeLetter).trim().toUpperCase().match(/^([A-F][+-]?)$/);
    if (m) out.gradeLetter = m[1];
  }

  if (d.gradeNumber !== null && d.gradeNumber !== undefined && d.gradeNumber !== '') {
    const m = String(d.gradeNumber).match(/\b(100|[1-9]?\d)\b/);
    if (m) {
      const n = Number(m[1]);
      if (Number.isFinite(n) && n >= 0 && n <= 100) out.gradeNumber = n;
    }
  }

  if ((out.gradeLetter === 'N/A' || out.gradeNumber === null) && isValuePresent(d.performanceGrade)) {
    const compact = String(d.performanceGrade).replace(/\s+/g, ' ').trim();
    const m = compact.match(/\b([A-F][+-]?)\s*(100|[1-9]?\d)\b/i);
    if (m) {
      out.gradeLetter = m[1].toUpperCase();
      out.gradeNumber = Number(m[2]);
    } else {
      const letterOnly = compact.match(/\b([A-F][+-]?)\b/i);
      if (letterOnly && out.gradeLetter === 'N/A') out.gradeLetter = letterOnly[1].toUpperCase();
    }
  }

  const scoreToLetter = (score) => {
    const n = Number(score);
    if (!Number.isFinite(n)) return 'N/A';
    if (n >= 90) return 'A';
    if (n >= 80) return 'B';
    if (n >= 70) return 'C';
    if (n >= 60) return 'D';
    return 'F';
  };

  if (out.gradeLetter === 'N/A' && out.gradeNumber !== null) {
    out.gradeLetter = scoreToLetter(out.gradeNumber);
  }

  if (out.gradeLetter !== 'N/A' && out.gradeNumber !== null) {
    out.performanceGrade = `${out.gradeLetter}${out.gradeNumber}`;
  } else if (out.gradeLetter !== 'N/A') {
    out.performanceGrade = out.gradeLetter;
  }

  if (isValuePresent(d.loadTime)) out.loadTime = String(d.loadTime).replace(/\s+/g, ' ').trim();
  if (isValuePresent(d.pageSize)) out.pageSize = String(d.pageSize).replace(/\s+/g, ' ').trim();

  const requests = normalizeRequestCount(d.requests);
  if (requests !== 'N/A') out.requests = requests;

  return out;
}

function hasPingdomGrade(data) {
  const d = normalizePingdomParsedData(data);
  return d.gradeLetter !== 'N/A' || d.gradeNumber !== null || d.performanceGrade !== 'N/A';
}

function mergePingdomData(preferred, fallback) {
  const a = normalizePingdomParsedData(preferred);
  const b = normalizePingdomParsedData(fallback);
  const out = pingdomEmptyData();

  if (hasPingdomGrade(a)) {
    out.performanceGrade = a.performanceGrade;
    out.gradeLetter = a.gradeLetter;
    out.gradeNumber = a.gradeNumber;
  } else if (hasPingdomGrade(b)) {
    out.performanceGrade = b.performanceGrade;
    out.gradeLetter = b.gradeLetter;
    out.gradeNumber = b.gradeNumber;
  }

  out.loadTime = a.loadTime !== 'N/A' ? a.loadTime : b.loadTime;
  out.pageSize = a.pageSize !== 'N/A' ? a.pageSize : b.pageSize;
  out.requests = a.requests !== 'N/A' ? a.requests : b.requests;

  return normalizePingdomParsedData(out);
}

function isPingdomAllNA(data) {
  const d = normalizePingdomParsedData(data);
  return d.gradeLetter === 'N/A' &&
    d.gradeNumber === null &&
    d.loadTime === 'N/A' &&
    d.pageSize === 'N/A' &&
    d.requests === 'N/A';
}

async function extractPingdomVisibleMetrics(page) {
  const extracted = await page.evaluate(() => {
    const clean = (v) => String(v || '').replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
    const isVisibleStyle = (el) => {
      if (!el) return false;
      const st = window.getComputedStyle(el);
      return st && st.display !== 'none' && st.visibility !== 'hidden' && Number(st.opacity || 1) !== 0;
    };

    const tokens = [];
    const walker = document.createTreeWalker(
      document.body,
      NodeFilter.SHOW_TEXT,
      {
        acceptNode(node) {
          const text = clean(node.nodeValue);
          if (!text) return NodeFilter.FILTER_REJECT;
          const parent = node.parentElement;
          if (!isVisibleStyle(parent)) return NodeFilter.FILTER_REJECT;
          return NodeFilter.FILTER_ACCEPT;
        }
      }
    );

    while (walker.nextNode()) {
      const node = walker.currentNode;
      const text = clean(node.nodeValue);
      const parent = node.parentElement;
      if (!text || !parent) continue;

      const range = document.createRange();
      range.selectNodeContents(node);
      const rect = range.getBoundingClientRect();
      range.detach?.();

      if (!rect || rect.width <= 0 || rect.height <= 0) continue;
      if (rect.bottom < 0 || rect.top > window.innerHeight + 2000) continue;

      const st = window.getComputedStyle(parent);
      const fontSize = parseFloat(st.fontSize || '0') || 0;

      tokens.push({
        text,
        x: rect.left,
        y: rect.top,
        cx: rect.left + rect.width / 2,
        cy: rect.top + rect.height / 2,
        w: rect.width,
        h: rect.height,
        fontSize,
        parentTag: parent.tagName,
        parentClasses: parent.className,
      });
    }

    const yourResultsToken = tokens.find(t => /your\s+results/i.test(t.text));
    const resultTop = yourResultsToken ? yourResultsToken.y : 0;
    const nextHeading = yourResultsToken
      ? tokens.find(t => t.y > resultTop + 20 && /^(performance|improve|grade|page\s+size|load\s+time|requests)/i.test(t.text) && t.fontSize >= 14)
      : null;
    const resultBottom = nextHeading ? nextHeading.y : (yourResultsToken ? yourResultsToken.y + 300 : Infinity);

    const tokensInResults = tokens.filter(t => t.y >= resultTop - 10 && t.y < resultBottom);

    function findLabel(labelRe) {
      return tokensInResults.find(t => labelRe.test(t.text)) || null;
    }

    function nearby(label, options = {}) {
      if (!label) return [];
      const maxDx = options.maxDx || 170;
      const maxDy = options.maxDy || 140;
      const minDy = options.minDy ?? -8;
      return tokensInResults
        .filter(t => t !== label)
        .filter(t => t.cy >= label.cy + minDy && t.cy <= label.cy + maxDy)
        .filter(t => Math.abs(t.cx - label.cx) <= maxDx)
        .sort((a, b) => {
          const belowA = a.cy >= label.cy ? 0 : 1;
          const belowB = b.cy >= label.cy ? 0 : 1;
          if (belowA !== belowB) return belowA - belowB;
          if (b.fontSize !== a.fontSize) return b.fontSize - a.fontSize;
          return Math.abs(a.cx - label.cx) + Math.abs(a.cy - label.cy) -
                 (Math.abs(b.cx - label.cx) + Math.abs(b.cy - label.cy));
        });
    }

    const performanceLabel = findLabel(/performance\s*grade/i);
    const pageSizeLabel = findLabel(/page\s*size/i);
    const loadTimeLabel = findLabel(/load\s*time/i);
    const requestsLabel = findLabel(/requests/i);

    function parseNumberUnit(cands, unitRe) {
      const text = cands.map(t => t.text).join(' ');
      let m = text.match(new RegExp('\\b([0-9]+(?:\\.[0-9]+)?)\\s*(' + unitRe + ')\\b', 'i'));
      if (m) return `${m[1]} ${m[2].toUpperCase()}`;
      for (let i = 0; i < cands.length - 1; i++) {
        const a = clean(cands[i].text);
        const b = clean(cands[i+1].text);
        if (/^[0-9]+(?:\.[0-9]+)?$/.test(a) && new RegExp('^(' + unitRe + ')$', 'i').test(b)) {
          return `${a} ${b.toUpperCase()}`;
        }
      }
      return 'N/A';
    }

    function parseLoadTime(cands) {
      const text = cands.map(t => t.text).join(' ');
      let m = text.match(/\b([0-9]+(?:\.[0-9]+)?)\s*(ms|s)\b/i);
      if (m) return `${m[1]} ${m[2].toLowerCase()}`;
      for (let i = 0; i < cands.length - 1; i++) {
        const a = clean(cands[i].text);
        const b = clean(cands[i+1].text);
        if (/^[0-9]+(?:\.[0-9]+)?$/.test(a) && /^(ms|s)$/i.test(b)) {
          return `${a} ${b.toLowerCase()}`;
        }
      }
      return 'N/A';
    }

    function parseRequests(cands) {
      const nums = [];
      for (const t of cands) {
        const txt = clean(t.text);
        const matches = Array.from(txt.matchAll(/\b([1-9][0-9]{0,3})\b/g)).map(m => Number(m[1]));
        for (const n of matches) {
          if (n > 0 && n <= 1000) {
            nums.push({ n, token: t });
          }
        }
      }
      if (!nums.length) return 'N/A';
      nums.sort((a, b) => {
        if (b.token.fontSize !== a.token.fontSize) return b.token.fontSize - a.token.fontSize;
        return a.token.y - b.token.y;
      });
      return String(nums[0].n);
    }

    function scoreToLetter(score) {
      const n = Number(score);
      if (!Number.isFinite(n)) return 'N/A';
      if (n >= 90) return 'A';
      if (n >= 80) return 'B';
      if (n >= 70) return 'C';
      if (n >= 60) return 'D';
      return 'F';
    }

    function parseGrade(cands) {
      const text = cands.map(t => t.text).join(' ');
      let m = text.match(/\b([A-F][+-]?)\s*(100|[1-9]?\d)\b/i);
      if (m) {
        return { gradeLetter: m[1].toUpperCase(), gradeNumber: Number(m[2]) };
      }

      let letterToken = cands.find(t => /^([A-F][+-]?)$/i.test(clean(t.text)));
      let scoreToken = cands.find(t => {
        const cleaned = clean(t.text);
        return /^(100|[1-9]?\d)$/.test(cleaned) && Number(cleaned) >= 0 && Number(cleaned) <= 100;
      });

      const score = scoreToken ? Number(clean(scoreToken.text)) : null;
      const letter = letterToken ? clean(letterToken.text).toUpperCase() : (score !== null ? scoreToLetter(score) : 'N/A');

      return { gradeLetter: letter, gradeNumber: score };
    }

    const gradeCands = nearby(performanceLabel, { maxDx: 140, maxDy: 130 });
    const sizeCands = nearby(pageSizeLabel, { maxDx: 150, maxDy: 130 });
    const loadCands = nearby(loadTimeLabel, { maxDx: 150, maxDy: 130 });
    const requestCands = nearby(requestsLabel, { maxDx: 150, maxDy: 130 });

    const grade = parseGrade(gradeCands);
    const pageSize = parseNumberUnit(sizeCands, 'KB|MB|GB');
    const loadTime = parseLoadTime(loadCands);
    let requests = parseRequests(requestCands);

    if (requests === 'N/A' && requestsLabel) {
      const broader = tokensInResults.filter(t => t.y > requestsLabel.y && t.y < requestsLabel.y + 100);
      const nums = [];
      for (const t of broader) {
        const m = t.text.match(/\b([1-9][0-9]{0,3})\b/);
        if (m && Number(m[1]) > 0 && Number(m[1]) <= 1000) nums.push(Number(m[1]));
      }
      if (nums.length) requests = String(nums[0]);
    }

    const performanceGrade = grade.gradeLetter !== 'N/A' && grade.gradeNumber !== null
      ? `${grade.gradeLetter}${grade.gradeNumber}`
      : (grade.gradeLetter !== 'N/A' ? grade.gradeLetter : 'N/A');

    return {
      performanceGrade,
      gradeLetter: grade.gradeLetter,
      gradeNumber: grade.gradeNumber,
      loadTime,
      pageSize,
      requests,
    };
  });

  return normalizePingdomParsedData(extracted);
}

// ---------- REPLACED: waits for stable complete OR returns best partial data ----------
async function waitForStableOrPartialPingdomMetrics(page, domain, timeoutMs = 150000) {
  const started = Date.now();
  let lastPartial = null;
  let lastComplete = null;
  let lastSignature = '';
  let stableCount = 0;

  while (Date.now() - started < timeoutMs) {
    await wait(3000);
    let parsed = await extractPingdomVisibleMetrics(page).catch(() => null);
    let normalized = normalizePingdomParsedData(parsed);

    // Fallback to text parser if DOM extraction returned all N/A
    if (isPingdomAllNA(normalized)) {
      const pageText = await page.evaluate(() => document.body.innerText || '').catch(() => '');
      if (pageText) {
        const textParsed = parsePingdomText(pageText);
        const textNormalized = normalizePingdomParsedData(textParsed);
        if (!isPingdomAllNA(textNormalized)) {
          normalized = textNormalized;
        }
      }
    }

    // Track best partial data (at least a grade)
    if (!isPingdomAllNA(normalized) && (normalized.gradeLetter !== 'N/A' || normalized.gradeNumber !== null)) {
      if (!lastPartial || (isPingdomComplete(normalized) && !isPingdomComplete(lastPartial))) {
        lastPartial = normalized;
      }
    }

    const complete = isPingdomComplete(normalized);
    if (complete) {
      const signature = [
        normalized.performanceGrade,
        normalized.loadTime,
        normalized.pageSize,
        normalized.requests,
      ].join('|');
      if (signature === lastSignature) {
        stableCount += 1;
        if (stableCount >= 2) {
          console.log(`[pingdom] Stable complete results for ${domain}`);
          return normalized;
        }
      } else {
        stableCount = 1;
        lastSignature = signature;
      }
      lastComplete = normalized;
    } else {
      stableCount = 0;
    }
  }

  // Timeout reached — return best data we have
  if (lastComplete) {
    console.warn(`[pingdom] Timeout for ${domain} — returning last complete result`);
    return lastComplete;
  }
  if (lastPartial && isPingdomMinimal(lastPartial)) {
    console.warn(`[pingdom] Timeout for ${domain} — returning best partial result (${JSON.stringify(lastPartial)})`);
    return lastPartial;
  }
  console.warn(`[pingdom] No usable results for ${domain} after ${timeoutMs}ms`);
  return null;
}

// ---------- Single attempt function (retryable) ----------
async function runPingdomOnce(domain, context, attempt) {
  const { newTab, paths } = context;
  const debugDir = paths.domainDir;
  const screenshotPath = path.join(paths.imagesDir, 'pingdom.png');
  const errorScreenshotPath = path.join(paths.imagesDir, `pingdom-error-attempt-${attempt}.png`);
  let tab = null;

  try {
    tab = await newTab();

    await tab.setViewport({
      width: PINGDOM_VIEWPORT_WIDTH,
      height: PINGDOM_VIEWPORT_HEIGHT,
      deviceScaleFactor: 1,
    });

    await tab.setUserAgent(USER_AGENT);
    tab.setDefaultNavigationTimeout(300000);
    tab.setDefaultTimeout(300000);

    const existingInterception = await tab.evaluate(() => true).then(() => false).catch(() => false);
    if (!existingInterception) {
      await tab.setRequestInterception(true).catch(() => {});
    }

    tab.on('request', (req) => {
      const url = req.url().toLowerCase();
      const type = req.resourceType();

      if (
        type === 'media' ||
        url.includes('google-analytics.com') ||
        url.includes('googletagmanager.com') ||
        url.includes('doubleclick.net') ||
        url.includes('hotjar.com') ||
        url.includes('clarity.ms')
      ) {
        req.abort().catch(() => {});
        return;
      }
      req.continue().catch(() => {});
    });

    await tab.goto('https://tools.pingdom.com/', {
      waitUntil: 'domcontentloaded',
      timeout: 90000,
    });

    await wait(5000);
    await waitForFonts(tab, 15000);
    await wait(2000);

    await fillPingdomUrl(tab, domain);
    await setPingdomLocation(tab);
    await wait(800);
    await clickPingdomStartTest(tab);
    await waitForPingdomResults(tab, domain);
    await waitForFonts(tab, 10000);
    await wait(3000);

    // ── SCREENSHOT FIRST ──────────────────────────────────────────────────
    const screenshotFile = await captureWithRetry(tab, screenshotPath, {
      width: PINGDOM_VIEWPORT_WIDTH,
      left: 0,
      right: 25,
      top: 0,
      bottom: 0,
      minBytes: 1000,
      settleBeforeCaptureMs: 2000,
      retryBaseDelayMs: 5000,
    }, PINGDOM_SCREENSHOT_RETRIES).catch((ssErr) => {
      console.warn(`[pingdom] Screenshot failed for ${domain} (attempt ${attempt}): ${ssErr.message}`);
      return '';
    });

    // ── WAIT FOR STABLE OR PARTIAL METRICS ────────────────────────────────
    let parsedMetrics = await waitForStableOrPartialPingdomMetrics(tab, domain, 150000);
    if (!parsedMetrics) {
      // ultimate fallback: try text parsing one more time
      const pageText = await tab.evaluate(() => document.body.innerText || '').catch(() => '');
      if (pageText) {
        parsedMetrics = parsePingdomText(pageText);
        if (isPingdomAllNA(parsedMetrics)) {
          throw new Error('Pingdom: no metrics could be extracted after all fallbacks');
        }
      } else {
        throw new Error('Pingdom: no metrics could be extracted after all fallbacks');
      }
    }

    await wait(1000);

    const finalVisibleParsed = await extractPingdomVisibleMetrics(tab).catch(() => null);
    const pageText = await tab.evaluate(() => document.body.innerText || '');
    const textParsed = parsePingdomText(pageText);

    let parsed = mergePingdomData(finalVisibleParsed, parsedMetrics);
    parsed = mergePingdomData(parsed, textParsed);

    // Final validation: we must have at least a grade (letter or number)
    if (parsed.gradeLetter === 'N/A' && parsed.gradeNumber === null) {
      throw new Error('Pingdom: grade completely missing after all extraction attempts');
    }

    if (!isPingdomComplete(parsed)) {
      const missing = [];
      if (parsed.gradeLetter === 'N/A') missing.push('gradeLetter');
      if (parsed.gradeNumber === null) missing.push('gradeNumber');
      if (parsed.loadTime === 'N/A') missing.push('loadTime');
      if (parsed.pageSize === 'N/A') missing.push('pageSize');
      if (parsed.requests === 'N/A') missing.push('requests');
      console.warn(`[pingdom] Partial data for ${domain} — missing: ${missing.join(', ')} (proceeding with available data)`);
    }

    // Additional sanity for request count
    if (parsed.requests !== 'N/A' && Number(parsed.requests) > 1000) {
      const textReq = normalizeRequestCount(textParsed.requests);
      if (textReq !== 'N/A') parsed.requests = textReq;
      else parsed.requests = 'N/A';
    }

    try {
      fs.writeFileSync(path.join(debugDir, `pingdom-rendered-text-attempt-${attempt}.txt`), pageText || '');
      fs.writeFileSync(path.join(debugDir, `pingdom-visible-parsed-attempt-${attempt}.json`), JSON.stringify({
        finalVisibleParsed,
        textParsed,
        finalParsed: parsed,
      }, null, 2));
      fs.writeFileSync(path.join(debugDir, `pingdom-parsed-attempt-${attempt}.json`), JSON.stringify(parsed, null, 2));
    } catch (_) {}

    return {
      status: 'SUCCESS',
      data: parsed,
      error: null,
      errorCode: null,
      url: tab.url(),
      screenshot: screenshotFile || '',
    };
  } catch (err) {
    console.error(`[pingdom] Attempt ${attempt} error for ${domain}:`, err.message);

    try {
      if (tab) {
        if (!fs.existsSync(screenshotPath)) {
          await captureWithRetry(tab, errorScreenshotPath, {
            width: PINGDOM_VIEWPORT_WIDTH,
            left: 0,
            right: 0,
            top: 0,
            bottom: 0,
            minBytes: 1000,
          }, 2).catch(() => {});

          try {
            if (fs.existsSync(errorScreenshotPath)) {
              fs.copyFileSync(errorScreenshotPath, screenshotPath);
            }
          } catch (_) {}
        }

        const html = await tab.content().catch(() => '');
        if (html) {
          fs.writeFileSync(path.join(debugDir, `pingdom-error-attempt-${attempt}.html`), html);
        }
      }
    } catch (_) {}

    const existingScreenshot = fs.existsSync(screenshotPath) ? path.basename(screenshotPath) : '';

    // Last-chance extraction: sometimes the Pingdom page visibly contains results,
    // but the service throws during a later stability/screenshot step. In that case,
    // do NOT throw away the visible metrics and import N/A into FileMaker.
    try {
      if (tab) {
        const visibleParsed = await extractPingdomVisibleMetrics(tab).catch(() => null);
        const pageText = await tab.evaluate(() => document.body.innerText || '').catch(() => '');
        const textParsed = pageText ? parsePingdomText(pageText) : null;
        let recovered = mergePingdomData(visibleParsed, textParsed);
        recovered = normalizePingdomParsedData(recovered);

        if (!isPingdomAllNA(recovered) && (recovered.gradeLetter !== 'N/A' || recovered.gradeNumber !== null)) {
          console.warn(`[pingdom] Recovered visible results for ${domain} after error: ${JSON.stringify(recovered)}`);
          try {
            fs.writeFileSync(path.join(debugDir, `pingdom-recovered-visible-attempt-${attempt}.json`), JSON.stringify(recovered, null, 2));
          } catch (_) {}
          return {
            status: 'SUCCESS',
            data: recovered,
            error: null,
            errorCode: null,
            url: tab.url ? tab.url() : 'https://tools.pingdom.com/',
            screenshot: existingScreenshot,
          };
        }
      }
    } catch (recoverErr) {
      console.warn(`[pingdom] Visible recovery failed for ${domain}: ${recoverErr.message}`);
    }

    return {
      status: 'FAILED',
      data: {
        performanceGrade: 'N/A',
        gradeLetter: 'N/A',
        gradeNumber: null,
        loadTime: 'N/A',
        pageSize: 'N/A',
        requests: 'N/A',
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
      url: 'https://tools.pingdom.com/',
      screenshot: existingScreenshot,
    };
  } finally {
    if (tab) await tab.close().catch(() => {});
  }
}

// ---------- Main exported function (with retry loop) ----------
async function runPingdom(domain, context) {
  let lastError = '';

  for (let attempt = 1; attempt <= MAX_PINGDOM_RETRIES; attempt++) {
    console.log(`[pingdom] Attempt ${attempt}/${MAX_PINGDOM_RETRIES} for ${domain}`);

    const result = await runPingdomOnce(domain, context, attempt);

    if (result && result.status === 'SUCCESS') {
      return result;
    }

    lastError = result && result.error ? result.error : 'Unknown Pingdom error';

    if (attempt < MAX_PINGDOM_RETRIES) {
      const retryDelay = attempt === 1 ? 45000 : 60000;
      console.log(`[pingdom] Retry in ${retryDelay / 1000}s for ${domain}: ${lastError}`);
      await wait(retryDelay);
    }
  }

  console.error(`[pingdom] Failed after ${MAX_PINGDOM_RETRIES} attempts for ${domain}: ${lastError}`);
  return pingdomNAResult(lastError);
}

module.exports = { runPingdom };