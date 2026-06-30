"use strict";

/**
 * scrape-contact.js
 *
 * Scrapes company contact information (email, phone, address, company name,
 * social links) from a domain's homepage footer and common contact pages.
 *
 * Techniques used (in priority order):
 *   1. JSON-LD structured data  (schema.org LocalBusiness / Organization)
 *   2. Contact page scrape      (/contact, /contact-us, /about, /about-us)
 *   3. Homepage footer scrape   (footer, .footer, #footer selectors)
 *   4. Whole-page regex sweep   (last resort — catches anything missed above)
 *
 * Integration:
 *   Same acquireBrowser / createNewPage pattern as the rest of index.js.
 *   Add "contact" to the tool list and call scrapeContact(context) inside
 *   the track() wrapper.
 *
 * Returns { Contact_Status, Contact_CompanyName, Contact_Email[],
 *           Contact_Phone[], Contact_Address, Contact_Socials{},
 *           Contact_Source, Contact_URL, Contact_Error }
 */

// ---------------------------------------------------------------------------
// Regex patterns
// ---------------------------------------------------------------------------

// Email — intentionally conservative to avoid false positives.
// Excludes image filenames (*.png), common assets, and placeholder text.
const EMAIL_RE =
  /\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,10}\b/g;

const EMAIL_BLACKLIST = new Set([
  "example.com",
  "yourdomain.com",
  "domain.com",
  "email.com",
  "company.com",
  "yoursite.com",
  "test.com",
  "sentry.io",
  "sentry-next.wixpress.com",
  "wixpress.com",
]);

// Phone — handles AU (+61), PH (+63), US (+1), UK (+44), and generic formats.
const PHONE_RE =
  /(?:\+?\d{1,3}[\s\-.]?)?(?:\(?\d{1,4}\)?[\s\-.]?)(?:\d[\s\-.]?){6,12}\d/g;

// Address keywords — used to score candidate strings.
const ADDRESS_KEYWORDS =
  /\b(street|st\.|avenue|ave\.?|road|rd\.?|boulevard|blvd\.?|lane|ln\.?|drive|dr\.?|court|ct\.?|place|pl\.?|way|suite|ste\.?|floor|fl\.?|unit|po\s*box|p\.o\.?\s*box|zip|postal)\b/i;

const PO_BOX_RE = /\b(p\.?o\.?\s*box|po\s*box)\s*\d+/i;

// Cheap noise filter for phone-number candidates.
const PHONE_MIN_DIGITS = 7;

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

function countDigits(s) {
  return (s.match(/\d/g) || []).length;
}

function cleanEmail(e) {
  return e.toLowerCase().trim().replace(/[.,;:]+$/, "");
}

function isValidEmail(e) {
  const domain = e.split("@")[1] || "";
  if (EMAIL_BLACKLIST.has(domain)) return false;
  if (/\.(png|jpg|jpeg|gif|webp|svg|css|js)$/i.test(e)) return false;
  if (e.length > 120) return false;
  return true;
}

function cleanPhone(p) {
  return p.trim().replace(/[\s\u00a0]+/g, " ");
}

function isValidPhone(p) {
  if (countDigits(p) < PHONE_MIN_DIGITS) return false;
  if (countDigits(p) > 15) return false;
  // Skip things that look like years or pure numbers with no separators
  if (/^\d{4}$/.test(p.trim())) return false;
  return true;
}

function dedupe(arr) {
  return [...new Set(arr.map((s) => s.trim()).filter(Boolean))];
}

function dedupeUrls(arr) {
  return [...new Set((arr || []).map((s) => String(s || "").trim()).filter(Boolean))];
}

function isContactLikeUrl(value) {
  const s = String(value || "").toLowerCase();
  return /\b(contact|contact-us|contacts|get-in-touch|reach-us|inquiry|enquiry|request-quote|quote|support)\b/.test(s) ||
    /\/(contact|contact-us|contacts|get-in-touch|reach-us|inquiry|enquiry|request-quote|quote|support)(\/|$|[?#])/.test(s);
}

function urlPathFromAbsolute(value) {
  try {
    const u = new URL(String(value || ""));
    return (u.pathname || "/").replace(/\/$/, "") || "/";
  } catch (_) {
    return "";
  }
}

function contactUrlPriority(value) {
  let path = "";
  try {
    path = new URL(String(value || "")).pathname.toLowerCase().replace(/\/$/, "") || "/";
  } catch (_) {
    path = String(value || "").toLowerCase();
  }

  if (path === "/contact") return 1;
  if (path === "/contact-us") return 2;
  if (path === "/contacts") return 3;
  if (path === "/get-in-touch") return 4;
  if (path === "/reach-us") return 5;
  if (/contact/.test(path)) return 6;
  if (/get-in-touch|reach-us|inquiry|enquiry|quote|support/.test(path)) return 7;
  return 50;
}

function scoreAddressCandidate(text) {
  let score = 0;
  if (ADDRESS_KEYWORDS.test(text)) score += 3;
  if (PO_BOX_RE.test(text)) score += 4;
  if (/\d{4,6}/.test(text)) score += 1; // postcode-like
  if (/,/.test(text)) score += 1;        // comma-separated components
  if (text.length > 10 && text.length < 200) score += 1;
  return score;
}

// ---------------------------------------------------------------------------
// JSON-LD parser — checks all <script type="application/ld+json"> blocks
// ---------------------------------------------------------------------------

function extractFromJsonLd(jsonLdArray) {
  const result = {
    companyName: "",
    emails: [],
    phones: [],
    address: "",
    socials: {},
  };

  if (!Array.isArray(jsonLdArray)) jsonLdArray = [];

  const types = [
    "LocalBusiness",
    "Organization",
    "Corporation",
    "Store",
    "Restaurant",
    "Hotel",
    "MedicalBusiness",
    "ProfessionalService",
  ];

  for (const raw of jsonLdArray) {
    let obj;
    try {
      obj = typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch (_) {
      continue;
    }

    // Handle @graph arrays (common in WordPress/Yoast setups)
    const candidates = Array.isArray(obj["@graph"])
      ? obj["@graph"]
      : [obj];

    for (const node of candidates) {
      const nodeType = String(node["@type"] || "");
      const isOrgLike = types.some((t) => nodeType.includes(t));
      if (!isOrgLike) continue;

      // Company name
      if (!result.companyName && node.name) {
        result.companyName = String(node.name).trim();
      }

      // Email
      if (node.email) {
        const emails = Array.isArray(node.email) ? node.email : [node.email];
        emails.forEach((e) => {
          const cleaned = cleanEmail(String(e));
          if (isValidEmail(cleaned)) result.emails.push(cleaned);
        });
      }

      // Phone
      if (node.telephone) {
        const phones = Array.isArray(node.telephone)
          ? node.telephone
          : [node.telephone];
        phones.forEach((p) => {
          const cleaned = cleanPhone(String(p));
          if (isValidPhone(cleaned)) result.phones.push(cleaned);
        });
      }

      // Address (PostalAddress schema)
      if (!result.address && node.address) {
        const addr = node.address;
        if (typeof addr === "string") {
          result.address = addr.trim();
        } else if (typeof addr === "object") {
          const parts = [
            addr.streetAddress,
            addr.addressLocality,
            addr.addressRegion,
            addr.postalCode,
            addr.addressCountry,
          ]
            .filter(Boolean)
            .map((s) => String(s).trim());
          result.address = parts.join(", ");
        }
      }

      // Social media
      if (node.sameAs) {
        const links = Array.isArray(node.sameAs) ? node.sameAs : [node.sameAs];
        links.forEach((url) => {
          const u = String(url).toLowerCase();
          if (u.includes("facebook.com"))  result.socials.facebook  = url;
          if (u.includes("twitter.com") || u.includes("x.com"))
                                            result.socials.twitter   = url;
          if (u.includes("linkedin.com"))   result.socials.linkedin  = url;
          if (u.includes("instagram.com"))  result.socials.instagram = url;
          if (u.includes("youtube.com"))    result.socials.youtube   = url;
          if (u.includes("tiktok.com"))     result.socials.tiktok    = url;
          if (u.includes("pinterest.com"))  result.socials.pinterest = url;
        });
      }
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// In-page scrape — runs inside page.evaluate()
// ---------------------------------------------------------------------------

const IN_PAGE_SCRAPER = /* js */ `
(function () {
  // ── helpers ────────────────────────────────────────────────────────────
  const EMAIL_RE   = /\\b[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,10}\\b/g;
  const PHONE_RE   = /(?:\\+?\\d{1,3}[\\s\\-.\\u00a0]?)?(?:\\(?\\d{1,4}\\)?[\\s\\-.\\u00a0]?)(?:\\d[\\s\\-.\\u00a0]?){6,12}\\d/g;
  const ADDR_KW    = /\\b(street|st\\.|avenue|ave\\.?|road|rd\\.?|boulevard|blvd\\.?|lane|drive|dr\\.?|court|ct\\.?|suite|ste\\.?|floor|p\\.?o\\.?\\s*box|po\\s*box)\\b/i;

  function getText(el) { return el ? (el.innerText || el.textContent || '') : ''; }

  // ── JSON-LD ────────────────────────────────────────────────────────────
  const jsonLdBlocks = [];
  document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
    try { jsonLdBlocks.push(JSON.parse(s.textContent)); } catch (_) {}
  });

  // ── Targeted zone text ─────────────────────────────────────────────────
  // Priority order: contact-page main, footer, header, full body
  const zones = [];
  const footerEl = document.querySelector(
    'footer, [class*="footer"], [id*="footer"], [role="contentinfo"]'
  );
  const contactEl = document.querySelector(
    '[class*="contact"], [id*="contact"], [class*="about"], [id*="about"]'
  );
  const headerEl = document.querySelector(
    'header, [class*="header"], [id*="header"]'
  );

  if (contactEl) zones.push(getText(contactEl));
  if (footerEl)  zones.push(getText(footerEl));
  if (headerEl)  zones.push(getText(headerEl));

  // Whole-page text as final fallback
  zones.push(document.body ? getText(document.body) : '');

  const combinedText = zones.join('\\n');

  // ── Email extraction ───────────────────────────────────────────────────
  // Also catch mailto: href values
  const emails = new Set();
  (combinedText.match(EMAIL_RE) || []).forEach(e => emails.add(e.toLowerCase()));
  document.querySelectorAll('a[href^="mailto:"]').forEach(a => {
    const e = (a.getAttribute('href') || '').replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase();
    if (e) emails.add(e);
  });

  // ── Phone extraction ───────────────────────────────────────────────────
  // Also catch tel: href values
  const phones = new Set();
  (combinedText.match(PHONE_RE) || []).forEach(p => phones.add(p.trim()));
  document.querySelectorAll('a[href^="tel:"]').forEach(a => {
    const p = (a.getAttribute('href') || '').replace(/^tel:/i, '').trim();
    if (p) phones.add(p);
  });

  // ── Address extraction ─────────────────────────────────────────────────
  // Look for lines that look like addresses in the footer/contact zone
  const addressCandidates = [];
  const lines = combinedText.split(/[\\n\\r]+/).map(l => l.trim()).filter(Boolean);
  lines.forEach(line => {
    if (ADDR_KW.test(line) && line.length > 8 && line.length < 200) {
      addressCandidates.push(line);
    }
  });

  // ── Social links ───────────────────────────────────────────────────────
  const socials = {};
  document.querySelectorAll('a[href]').forEach(a => {
    const href = (a.getAttribute('href') || '').toLowerCase();
    if (href.includes('facebook.com') && !socials.facebook)  socials.facebook  = a.href;
    if ((href.includes('twitter.com') || href.includes('x.com')) && !socials.twitter)
                                                               socials.twitter   = a.href;
    if (href.includes('linkedin.com') && !socials.linkedin)   socials.linkedin  = a.href;
    if (href.includes('instagram.com') && !socials.instagram) socials.instagram = a.href;
    if (href.includes('youtube.com') && !socials.youtube)     socials.youtube   = a.href;
    if (href.includes('tiktok.com') && !socials.tiktok)       socials.tiktok    = a.href;
  });

  // ── Contact form detection ────────────────────────────────────────────
  // We save the public page URL where a visitor can submit a message, not only
  // the technical <form action> endpoint. This is useful when no email/phone is public.
  function absUrl(value) {
    try { return new URL(value || window.location.href, window.location.href).href; }
    catch (_) { return ''; }
  }

  function cleanSmall(value) {
    return String(value || '').replace(/\s+/g, ' ').trim().slice(0, 240);
  }

  function scoreForm(form) {
    const action = (form.getAttribute('action') || '').toLowerCase();
    const idClass = ((form.id || '') + ' ' + (form.className || '')).toLowerCase();
    const pagePath = (window.location.pathname || '').toLowerCase();
    const text = cleanSmall(form.innerText || form.textContent || '').toLowerCase();
    const all = action + ' ' + idClass + ' ' + pagePath + ' ' + text;

    let score = 0;
    if (/contact|inquiry|enquiry|get[-_\s]?in[-_\s]?touch|quote|message|support/.test(all)) score += 4;
    if (/name/.test(all)) score += 1;
    if (/email|e-mail/.test(all)) score += 2;
    if (/phone|tel/.test(all)) score += 1;
    if (/message|comment|question/.test(all)) score += 3;
    if (form.querySelector('textarea')) score += 3;
    if (form.querySelector('input[type="email"], input[name*="email" i]')) score += 2;
    if (form.querySelector('input[type="text"], input[name*="name" i]')) score += 1;
    if (form.querySelector('input[type="submit"], button[type="submit"], button')) score += 1;

    // Avoid counting search/login/newsletter as a contact form unless it also
    // has contact-like fields.
    if (/search|login|sign[-_\s]?in|subscribe|newsletter|commentform/.test(all)) score -= 4;

    return score;
  }

  const contactForms = [];

  document.querySelectorAll('form').forEach(form => {
    const score = scoreForm(form);
    if (score < 3) return;

    const actionRaw = form.getAttribute('action') || '';
    const actionUrl = absUrl(actionRaw || window.location.href);

    contactForms.push({
      pageUrl: window.location.href,
      actionUrl,
      method: (form.getAttribute('method') || 'GET').toUpperCase(),
      score,
      id: form.id || '',
      className: cleanSmall(form.className || ''),
      type: 'form'
    });
  });

  // Some sites use embedded contact forms instead of native <form> tags.
  document.querySelectorAll('iframe[src]').forEach(frame => {
    const src = frame.getAttribute('src') || '';
    const haystack = (src + ' ' + (frame.title || '') + ' ' + (frame.name || '') + ' ' + window.location.pathname).toLowerCase();

    if (/contact|form|jotform|typeform|google.*form|gravity|wpforms|hubspot|formstack|cognitoforms|wufoo/.test(haystack)) {
      contactForms.push({
        pageUrl: window.location.href,
        actionUrl: absUrl(src),
        method: 'IFRAME',
        score: 5,
        id: frame.id || '',
        className: cleanSmall(frame.className || ''),
        type: 'iframe'
      });
    }
  });

  // ── Contact page link detection ───────────────────────────────────────
  const contactPageLinks = [];

  document.querySelectorAll('a[href]').forEach(a => {
    const hrefRaw = a.getAttribute('href') || '';
    const href = hrefRaw.toLowerCase();
    const text = cleanSmall(a.innerText || a.textContent || a.getAttribute('aria-label') || a.getAttribute('title') || '').toLowerCase();
    const haystack = href + ' ' + text;

    if (/mailto:|tel:|javascript:|#|facebook\.com|twitter\.com|instagram\.com|youtube\.com|linkedin\.com|tiktok\.com/.test(href)) {
      return;
    }

    if (/contact|contact-us|contacts|get[-_\s]?in[-_\s]?touch|reach[-_\s]?us|inquiry|enquiry|request[-_\s]?quote|quote|support/.test(haystack)) {
      const url = absUrl(hrefRaw);
      if (url) {
        contactPageLinks.push({
          url,
          text: cleanSmall(text),
          sourcePage: window.location.href
        });
      }
    }
  });

  // ── Company name fallback (OG / meta / title) ─────────────────────────
  const ogSiteName = (document.querySelector('meta[property="og:site_name"]') || {}).content || '';
  const metaAppName = (document.querySelector('meta[name="application-name"]') || {}).content || '';
  const h1Text = getText(document.querySelector('h1')).trim();
  const titleText = (document.title || '').trim().replace(/^home\s*[-|:]\s*/i, '').trim();
  const companyNameFallback = ogSiteName || metaAppName || titleText || h1Text || '';

  return {
    jsonLdBlocks,
    emails:             [...emails],
    phones:             [...phones],
    addressCandidates,
    socials,
    companyNameFallback,
    contactForms,
    contactPageLinks,
  };
})()
`;

// ---------------------------------------------------------------------------
// Navigation helper — tries HTTPS first, HTTP fallback
// ---------------------------------------------------------------------------

async function safeGoto(page, url, timeoutMs = 15000) {
  try {
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: timeoutMs,
    });
    return true;
  } catch (_) {
    return false;
  }
}

// ---------------------------------------------------------------------------
// Merge two partial result objects (JSON-LD takes priority over page scrape)
// ---------------------------------------------------------------------------

function emptyExtractedData() {
  return {
    companyName: "",
    emails: [],
    phones: [],
    address: "",
    socials: {},
  };
}

function emptyPageData() {
  return {
    jsonLdBlocks: [],
    emails: [],
    phones: [],
    addressCandidates: [],
    socials: {},
    companyNameFallback: "",
    contactForms: [],
    contactPageLinks: [],
  };
}

function normalizePageData(data) {
  if (!data || typeof data !== "object") return emptyPageData();

  return {
    jsonLdBlocks: Array.isArray(data.jsonLdBlocks) ? data.jsonLdBlocks : [],
    emails: Array.isArray(data.emails) ? data.emails : [],
    phones: Array.isArray(data.phones) ? data.phones : [],
    addressCandidates: Array.isArray(data.addressCandidates) ? data.addressCandidates : [],
    socials: data.socials && typeof data.socials === "object" ? data.socials : {},
    companyNameFallback: data.companyNameFallback || "",
    contactForms: Array.isArray(data.contactForms) ? data.contactForms : [],
    contactPageLinks: Array.isArray(data.contactPageLinks) ? data.contactPageLinks : [],
  };
}

async function evaluateContactPage(page, log, label) {
  try {
    const data = await page.evaluate(IN_PAGE_SCRAPER);
    return normalizePageData(data);
  } catch (err) {
    log(`In-page scrape failed on ${label}:`, err.message || String(err));
    return emptyPageData();
  }
}

function mergeResults(primary, secondary) {
  primary = primary || emptyExtractedData();
  secondary = secondary || emptyExtractedData();

  return {
    companyName: primary.companyName || secondary.companyName || "",
    emails:  dedupe([...(primary.emails || []),  ...(secondary.emails || [])]),
    phones:  dedupe([...(primary.phones || []),  ...(secondary.phones || [])]),
    address: primary.address || secondary.address || "",
    socials: { ...(secondary.socials || {}), ...(primary.socials || {}) },
  };
}

// ---------------------------------------------------------------------------
// Main export
// ---------------------------------------------------------------------------

/**
 * @param {object} opts
 * @param {string}   opts.domain          - e.g. "example.com"
 * @param {Function} opts.newTab          - () => Promise<Page>  (from createNewPage)
 * @param {number}   [opts.timeoutMs]     - per-page timeout (default 20 000 ms)
 * @param {boolean}  [opts.debug]         - log extra info
 */
async function scrapeContact(opts) {
  const {
    domain,
    newTab,
    timeoutMs = 20000,
    debug = false,
    startPath = "",
  } = opts;

  const log = debug ? console.log.bind(console, `[scrape-contact:${domain}]`) : () => {};

  const result = {
    Contact_Status:      "not_run",
    Contact_CompanyName: "",
    Contact_Email:       "",        // primary (first found)
    Contact_Emails:      [],        // all unique emails
    Contact_Phone:       "",        // primary (first found)
    Contact_Phones:      [],        // all unique phones
    Contact_Address:     "",
    Contact_ContactPageURL: "",    // best public contact page discovered
    Contact_DataURL:      "",      // page where the extracted data was found
    Contact_HasContactForm: 0,
    Contact_FormURL:     "",        // public page URL with contact form
    Contact_FormURLs:    [],        // all unique public pages with contact forms
    Contact_FormActionURL:  "",     // technical form action / iframe URL
    Contact_FormActionURLs: [],
    Contact_Facebook:    "",
    Contact_Twitter:     "",
    Contact_LinkedIn:    "",
    Contact_Instagram:   "",
    Contact_YouTube:     "",
    Contact_TikTok:      "",
    Contact_Source:      "",        // e.g. "jsonld+contact_page"
    Contact_URL:         "",        // best public contact/outreach URL, not just the data source
    Contact_Error:       "",
  };

  // Common contact-page paths to try (in order).
  // startPath lets the local tester preserve paths like https://site.com/contact/
  // and test that page first.
  const normalizePath = (p) => {
    let s = String(p || "").trim();
    if (!s || s === "/") return "";
    if (!s.startsWith("/")) s = "/" + s;
    return s.replace(/[?#].*$/, "").replace(/\/$/, "");
  };

  const preferredPath = normalizePath(startPath);
  const contactPaths = dedupe([
    preferredPath,
    "/contact",
    "/contact-us",
    "/contacts",
    "/about",
    "/about-us",
    "/reach-us",
    "/get-in-touch"
  ]).filter(Boolean);

  const baseUrls = [`https://${domain}`, `http://${domain}`];

  let page = null;
  const sources = [];
  let accumulated = { companyName: "", emails: [], phones: [], address: "", socials: {} };
  const contactFormPages = [];
  const contactFormActions = [];
  const discoveredContactPages = [];
  let bestContactPageUrl = "";
  let dataUrl = "";

  function rememberContactLinks(pageData) {
    const links = pageData && Array.isArray(pageData.contactPageLinks) ? pageData.contactPageLinks : [];
    for (const link of links) {
      if (link && link.url && isContactLikeUrl(link.url)) {
        discoveredContactPages.push(String(link.url));
      }
    }
  }

  function rememberContactForms(pageData, label) {
    const forms = pageData && Array.isArray(pageData.contactForms) ? pageData.contactForms : [];
    if (forms.length === 0) return;

    for (const form of forms) {
      if (form && form.pageUrl) contactFormPages.push(String(form.pageUrl));
      if (form && form.actionUrl) contactFormActions.push(String(form.actionUrl));
    }

    sources.push(`contact_form(${label})`);
  }

  try {
    page = await newTab();
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
    );

    // ── STEP 1: Homepage ────────────────────────────────────────────────
    let homepageReached = false;
    let workingBase = "";

    for (const base of baseUrls) {
      log(`Trying homepage: ${base}`);
      const ok = await safeGoto(page, base, timeoutMs);
      if (ok) {
        homepageReached = true;
        workingBase = base;
        log(`Homepage loaded: ${base}`);
        break;
      }
    }

    if (!homepageReached) {
      result.Contact_Status = "unreachable";
      result.Contact_Error  = "Could not load homepage";
      return result;
    }

    dataUrl = workingBase;

    // Small wait for any deferred JS to inject contact info into the DOM
    await new Promise((r) => setTimeout(r, 1500));

    const homePageData = await evaluateContactPage(page, log, "homepage");
    rememberContactLinks(homePageData);
    rememberContactForms(homePageData, "homepage");
    const homeJsonLd = extractFromJsonLd(homePageData.jsonLdBlocks || []);

    // Merge JSON-LD (highest quality) + page-scraped data
    const homeScraped = {
      companyName: homePageData.companyNameFallback || "",
      emails:  (homePageData.emails  || []).filter(isValidEmail).map(cleanEmail),
      phones:  (homePageData.phones  || []).filter(isValidPhone).map(cleanPhone),
      address: "",
      socials: homePageData.socials || {},
    };

    // Pick best address candidate from homepage
    const homeAddrCandidates = (homePageData.addressCandidates || []);
    if (homeAddrCandidates.length > 0) {
      homeAddrCandidates.sort((a, b) => scoreAddressCandidate(b) - scoreAddressCandidate(a));
      homeScraped.address = homeAddrCandidates[0];
    }

    accumulated = mergeResults(homeJsonLd, homeScraped);

    if (homeJsonLd.emails.length > 0 || homeJsonLd.address) {
      sources.push("jsonld");
    }
    sources.push("homepage");

    log("Homepage data:", accumulated);

    // ── STEP 2: Contact / About pages ───────────────────────────────────
    // Always try contact candidates so Contact_URL becomes the real outreach page
    // instead of the homepage when homepage already has phone/email.
    const contactUrls = dedupeUrls([
      ...discoveredContactPages,
      ...contactPaths.map((p) => `${workingBase}${p}`)
    ]).sort((a, b) => contactUrlPriority(a) - contactUrlPriority(b));

    if (contactUrls.length > 0) {
      for (const contactUrl of contactUrls) {
        const contactPath = urlPathFromAbsolute(contactUrl) || contactUrl;
        log(`Trying contact page: ${contactUrl}`);

        try {
          const ok = await safeGoto(page, contactUrl, timeoutMs);
          if (!ok) continue;

          // Verify it's a real page (not a redirect back to homepage)
          const currentUrl = page.url() || contactUrl;
          const finalUrl = currentUrl.toLowerCase().replace(/\/$/, "");
          const isContactPage = isContactLikeUrl(finalUrl);
          if (!isContactPage) {
            log(`${contactUrl} redirected away from a contact-like page — skipping`);
            continue;
          }

          if (!bestContactPageUrl) bestContactPageUrl = currentUrl;

          await new Promise((r) => setTimeout(r, 1000));

          const contactPageData = await evaluateContactPage(page, log, contactPath);
          rememberContactLinks(contactPageData);
          rememberContactForms(contactPageData, contactPath);
          const contactJsonLd = extractFromJsonLd(contactPageData.jsonLdBlocks || []);

          const contactScraped = {
            companyName: contactPageData.companyNameFallback || "",
            emails: (contactPageData.emails || []).filter(isValidEmail).map(cleanEmail),
            phones: (contactPageData.phones || []).filter(isValidPhone).map(cleanPhone),
            address: "",
            socials: contactPageData.socials || {},
          };

          const cpAddrCandidates = contactPageData.addressCandidates || [];
          if (cpAddrCandidates.length > 0) {
            cpAddrCandidates.sort((a, b) => scoreAddressCandidate(b) - scoreAddressCandidate(a));
            contactScraped.address = cpAddrCandidates[0];
          }

          const contactMerged = mergeResults(contactJsonLd, contactScraped);
          accumulated = mergeResults(contactMerged, accumulated);

          sources.push(`contact_page(${contactPath})`);

          if (
            contactMerged.emails.length > 0 ||
            contactMerged.phones.length > 0 ||
            contactMerged.address ||
            Object.keys(contactMerged.socials || {}).length > 0
          ) {
            dataUrl = currentUrl;
          }

          log(`Contact page data from ${contactPath}:`, contactMerged);

          // Stop visiting more pages if we now have what we need
          if (
            accumulated.emails.length > 0 &&
            accumulated.phones.length > 0 &&
            contactFormPages.length > 0
          ) break;

        } catch (err) {
          log(`Error on ${contactPath}:`, err.message);
        }
      }
    }

    // ── STEP 3: Build final result ──────────────────────────────────────
    const finalEmails = dedupe(accumulated.emails);
    const finalPhones = dedupe(accumulated.phones);
    const finalFormPages = dedupeUrls(contactFormPages);
    const finalFormActions = dedupeUrls(contactFormActions);

    result.Contact_Status      = "ok";
    result.Contact_CompanyName = accumulated.companyName || "";
    result.Contact_Email       = finalEmails[0] || "";
    result.Contact_Emails      = finalEmails;
    result.Contact_Phone       = finalPhones[0] || "";
    result.Contact_Phones      = finalPhones;
    result.Contact_Address     = accumulated.address || "";
    result.Contact_ContactPageURL = bestContactPageUrl || finalFormPages[0] || "";
    result.Contact_DataURL      = dataUrl || workingBase || "";
    result.Contact_HasContactForm = finalFormPages.length > 0 ? 1 : 0;
    result.Contact_FormURL     = finalFormPages[0] || "";
    result.Contact_FormURLs    = finalFormPages;
    result.Contact_FormActionURL  = finalFormActions[0] || "";
    result.Contact_FormActionURLs = finalFormActions;

    // Public URL to use for outreach. Prefer the page with a form, then the
    // best discovered contact page, then the page where data was found.
    result.Contact_URL =
      result.Contact_FormURL ||
      result.Contact_ContactPageURL ||
      result.Contact_DataURL ||
      workingBase ||
      "";

    result.Contact_Facebook    = accumulated.socials.facebook  || "";
    result.Contact_Twitter     = accumulated.socials.twitter   || "";
    result.Contact_LinkedIn    = accumulated.socials.linkedin  || "";
    result.Contact_Instagram   = accumulated.socials.instagram || "";
    result.Contact_YouTube     = accumulated.socials.youtube   || "";
    result.Contact_TikTok      = accumulated.socials.tiktok    || "";
    result.Contact_Source      = [...new Set(sources)].join("+");

    if (
      !result.Contact_Email &&
      !result.Contact_Phone &&
      !result.Contact_Address &&
      !result.Contact_FormURL &&
      !result.Contact_ContactPageURL
    ) {
      result.Contact_Status = "no_data";
      result.Contact_Error  = "No contact information, contact page, or contact form found on the site";
    } else if (
      !result.Contact_Email &&
      !result.Contact_Phone &&
      !result.Contact_Address &&
      result.Contact_FormURL
    ) {
      result.Contact_Status = "form_only";
      result.Contact_Error = "";
    } else if (
      !result.Contact_Email &&
      !result.Contact_Phone &&
      !result.Contact_Address &&
      result.Contact_ContactPageURL
    ) {
      result.Contact_Status = "contact_page_only";
      result.Contact_Error = "";
    }

    log("Final result:", result);

  } catch (err) {
    result.Contact_Status = "error";
    result.Contact_Error  = err.message || String(err);
    log("Fatal error:", err);
  } finally {
    if (page) {
      try { await page.close(); } catch (_) {}
    }
  }

  return result;
}

module.exports = { scrapeContact };