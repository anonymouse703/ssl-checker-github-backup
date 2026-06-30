#!/usr/bin/env node
"use strict";

/**
 * Smart WordPress detector
 *
 * Goal:
 * - Detect WordPress with high confidence.
 * - Run least intrusive tests first.
 * - Stop early when definitive evidence is found.
 * - Avoid hammering sites with unnecessary probes.
 *
 * Requirements:
 *   Node.js 18+
 *   npm install cheerio
 *
 * Usage:
 *   node detect-wordpress.js example.com
 *   node detect-wordpress.js example.com another-site.com
 */

const cheerio = require("cheerio");

const DEFAULT_TIMEOUT_MS = parseInt(
  process.env.WORDPRESS_DETECT_TIMEOUT_MS || "8000",
  10
);

const MAX_BYTES = parseInt(
  process.env.WORDPRESS_DETECT_MAX_BYTES || "750000",
  10
);

const USER_AGENT =
  "Mozilla/5.0 (compatible; InDepthSiteCheck/3.0; +https://in-depth.com)";

/**
 * Confirmation thresholds.
 *
 * We bail as soon as:
 * - definitive evidence exists, or
 * - score reaches confirmed level with multiple signal categories.
 */
const CONFIRMED_SCORE = 90;
const LIKELY_SCORE = 60;
const POSSIBLE_SCORE = 30;

function normalizeUrl(input, preferredProtocol = "https") {
  if (!input || typeof input !== "string") {
    throw new Error("A website URL is required.");
  }

  const trimmed = input.trim();

  if (!trimmed) {
    throw new Error("A website URL is required.");
  }

  if (!/^https?:\/\//i.test(trimmed)) {
    return `${preferredProtocol}://${trimmed.replace(/^\/+/, "")}`;
  }

  return trimmed;
}

function getOrigin(url) {
  const parsed = new URL(url);
  return `${parsed.protocol}//${parsed.host}`;
}

function buildUrl(origin, path) {
  return `${origin}${path.startsWith("/") ? path : `/${path}`}`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchLimited(url, options = {}) {
  const {
    method = "GET",
    timeoutMs = DEFAULT_TIMEOUT_MS,
    maxBytes = MAX_BYTES,
    accept =
      "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
  } = options;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method,
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent": USER_AGENT,
        Accept: accept,
        "Accept-Language": "en-US,en;q=0.9",
        Connection: "close",
      },
    });

    let body = "";

    if (response.body) {
      const reader = response.body.getReader();
      const chunks = [];
      let totalBytes = 0;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        totalBytes += value.byteLength;

        if (totalBytes <= maxBytes) {
          chunks.push(value);
          continue;
        }

        const overflowBytes = totalBytes - maxBytes;
        const allowedBytesLength = Math.max(0, value.byteLength - overflowBytes);

        if (allowedBytesLength > 0) {
          chunks.push(value.slice(0, allowedBytesLength));
        }

        break;
      }

      body = Buffer.concat(chunks).toString("utf8");
    }

    return {
      ok: response.ok,
      status: response.status,
      finalUrl: response.url,
      contentType: response.headers.get("content-type") || "",
      body,
      error: response.ok ? null : `HTTP ${response.status}`,
    };
  } catch (error) {
    return {
      ok: false,
      status: null,
      finalUrl: url,
      contentType: "",
      body: "",
      error:
        error && error.name === "AbortError"
          ? `Timed out after ${timeoutMs}ms`
          : error && error.message
            ? error.message
            : String(error),
    };
  } finally {
    clearTimeout(timeout);
  }
}

function addEvidence(evidence, marker, score, category, strength, detail) {
  evidence.push({
    marker,
    score,
    category,
    strength,
    detail,
  });
}

function getScore(evidence) {
  return evidence.reduce((sum, item) => sum + item.score, 0);
}

function getCategories(evidence) {
  return new Set(evidence.map((item) => item.category));
}

function hasDefinitiveEvidence(evidence) {
  return evidence.some((item) => item.strength === "definitive");
}

function hasStrongEvidence(evidence) {
  return evidence.some((item) => item.strength === "strong");
}

function hasOnlyWeakUploadEvidence(evidence) {
  return (
    evidence.length > 0 &&
    evidence.every((item) =>
      ["wp-uploads-path", "robots-wp-uploads", "sitemap-upload-path"].includes(
        item.marker
      )
    )
  );
}

function shouldStop(evidence) {
  const score = getScore(evidence);
  const categories = getCategories(evidence);

  if (hasDefinitiveEvidence(evidence)) {
    return true;
  }

  if (hasOnlyWeakUploadEvidence(evidence)) {
    return false;
  }

  if (score >= CONFIRMED_SCORE && categories.size >= 2) {
    return true;
  }

  if (score >= 75 && hasStrongEvidence(evidence) && categories.size >= 2) {
    return true;
  }

  return false;
}

function summarizeEvidence(evidence, completedStages, stoppedEarly) {
  const categories = Array.from(getCategories(evidence)).sort();
  const rawScore = getScore(evidence);

  let verdict = "not_detected";
  let confidence = "low";

  if (hasOnlyWeakUploadEvidence(evidence)) {
    verdict = "possible_wordpress";
    confidence = "low";
  } else if (hasDefinitiveEvidence(evidence)) {
    verdict = "confirmed_wordpress";
    confidence = "high";
  } else if (rawScore >= CONFIRMED_SCORE && categories.length >= 2) {
    verdict = "confirmed_wordpress";
    confidence = "high";
  } else if (rawScore >= LIKELY_SCORE && categories.length >= 2) {
    verdict = "likely_wordpress";
    confidence = "medium";
  } else if (rawScore >= POSSIBLE_SCORE) {
    verdict = "possible_wordpress";
    confidence = "low";
  }

  return {
    verdict,
    confidence,
    score: Math.min(rawScore, 150),
    rawScore,
    signalCategories: categories,
    evidenceCount: evidence.length,
    completedStages,
    stoppedEarly,
  };
}

/**
 * Stage 1:
 * Homepage HTML.
 *
 * Least intrusive. One normal GET request.
 * This catches the majority of obvious WordPress sites.
 */
function detectFromHomepage(html, finalUrl, evidence) {
  const lowerHtml = String(html || "").toLowerCase();
  const $ = cheerio.load(html || "");

  const generator = $('meta[name="generator"]').attr("content") || "";
  if (/wordpress/i.test(generator)) {
    addEvidence(
      evidence,
      "wordpress-generator-meta",
      70,
      "generator",
      "definitive",
      `Found WordPress generator meta tag: ${generator}`
    );
  }

  const restApiLink = $('link[rel="https://api.w.org/"]').attr("href");
  if (restApiLink) {
    addEvidence(
      evidence,
      "wordpress-rest-api-discovery-link",
      70,
      "rest_api",
      "definitive",
      `Found WordPress REST API discovery link: ${restApiLink}`
    );
  }

  if (lowerHtml.includes("/wp-content/plugins/")) {
    addEvidence(
      evidence,
      "wp-plugin-path",
      65,
      "asset_path",
      "definitive",
      "Found /wp-content/plugins/ in homepage HTML."
    );
  }

  if (lowerHtml.includes("/wp-content/themes/")) {
    addEvidence(
      evidence,
      "wp-theme-path",
      65,
      "asset_path",
      "definitive",
      "Found /wp-content/themes/ in homepage HTML."
    );
  }

  if (lowerHtml.includes("/wp-includes/")) {
    addEvidence(
      evidence,
      "wp-includes-path",
      45,
      "asset_path",
      "strong",
      "Found /wp-includes/ in homepage HTML."
    );
  }

  if (lowerHtml.includes("wp-admin/admin-ajax.php")) {
    addEvidence(
      evidence,
      "admin-ajax-reference",
      45,
      "admin",
      "strong",
      "Found wp-admin/admin-ajax.php reference in homepage HTML."
    );
  }

  if (lowerHtml.includes("/wp-content/uploads/")) {
    addEvidence(
      evidence,
      "wp-uploads-path",
      20,
      "media_path",
      "weak",
      "Found /wp-content/uploads/ in homepage HTML. Useful but weak by itself."
    );
  } else if (lowerHtml.includes("/wp-content/")) {
    addEvidence(
      evidence,
      "wp-content-path",
      35,
      "asset_path",
      "medium",
      "Found /wp-content/ in homepage HTML."
    );
  }

  const runtimeMarkers = [
    "wpemojiSettings",
    "_wpemojiSettings",
    "wp-emoji-release.min.js",
    "wp-i18n",
    "wp-hooks",
    "wp-element",
    "wp-polyfill",
    "wp-block-library",
    "wp-blocks",
    "wpApiSettings",
    "wp.apiFetch",
  ];

  const matchedRuntimeMarkers = runtimeMarkers.filter((marker) =>
    lowerHtml.includes(marker.toLowerCase())
  );

  if (matchedRuntimeMarkers.length > 0) {
    addEvidence(
      evidence,
      "wordpress-runtime-markers",
      Math.min(40, matchedRuntimeMarkers.length * 8),
      "runtime",
      matchedRuntimeMarkers.length >= 3 ? "strong" : "medium",
      `Found WordPress runtime markers: ${matchedRuntimeMarkers.join(", ")}`
    );
  }

  const wordpressPluginTextMarkers = [
    "yoast seo",
    "rank math",
    "all in one seo",
    "aioseo",
    "woocommerce",
    "elementor",
    "wp rocket",
    "gravity forms",
    "contact form 7",
  ];

  const matchedPluginTextMarkers = wordpressPluginTextMarkers.filter((marker) =>
    lowerHtml.includes(marker)
  );

  if (matchedPluginTextMarkers.length > 0) {
    addEvidence(
      evidence,
      "wordpress-plugin-text-marker",
      35,
      "plugin",
      "medium",
      `Found common WordPress plugin text markers: ${matchedPluginTextMarkers.join(
        ", "
      )}`
    );
  }

  const bodyClass = $("body").attr("class") || "";
  if (
    /\bwp-embed-responsive\b/i.test(bodyClass) ||
    /\bpage-template\b/i.test(bodyClass) ||
    /\bsingle-post\b/i.test(bodyClass) ||
    /\bpostid-\d+\b/i.test(bodyClass) ||
    /\bcategory-\b/i.test(bodyClass)
  ) {
    addEvidence(
      evidence,
      "wordpress-body-classes",
      25,
      "theme_output",
      "medium",
      `Found WordPress-style body classes: ${bodyClass}`
    );
  }

  return {
    canonical: $('link[rel="canonical"]').attr("href") || finalUrl,
  };
}

function detectWordPressFromHtml(html, options = {}) {
  const evidence = [];
  const finalUrl =
    typeof options === "string"
      ? options
      : String(options?.finalUrl || options?.url || "");
  const completedStages = ["homepage_cached"];
  let canonical = finalUrl;
  let parseError = null;

  try {
    const homepageMeta = detectFromHomepage(html || "", finalUrl, evidence);
    canonical = homepageMeta?.canonical || finalUrl;
  } catch (error) {
    parseError = error?.message || String(error);
  }

  const summary = summarizeEvidence(evidence, completedStages, false);

  return {
    ...summary,
    markerCount: evidence.length,
    evidence,
    finalUrl,
    canonical,
    error: parseError,
  };
}

/**
 * Stage 2:
 * robots.txt
 *
 * Very light. One tiny text file.
 * Good next step because many WP sites reveal /wp-admin/.
 */
function detectFromRobots(body, evidence) {
  const lower = String(body || "").toLowerCase();

  if (lower.includes("allow: /wp-admin/admin-ajax.php")) {
    addEvidence(
      evidence,
      "robots-admin-ajax-allow",
      45,
      "robots",
      "strong",
      "robots.txt allows /wp-admin/admin-ajax.php."
    );
  }

  if (lower.includes("/wp-admin/")) {
    addEvidence(
      evidence,
      "robots-wp-admin",
      40,
      "robots",
      "strong",
      "robots.txt references /wp-admin/."
    );
  }

  if (lower.includes("/wp-content/plugins/")) {
    addEvidence(
      evidence,
      "robots-wp-plugins",
      55,
      "robots",
      "definitive",
      "robots.txt references /wp-content/plugins/."
    );
  }

  if (lower.includes("/wp-content/themes/")) {
    addEvidence(
      evidence,
      "robots-wp-themes",
      55,
      "robots",
      "definitive",
      "robots.txt references /wp-content/themes/."
    );
  }

  if (lower.includes("/wp-content/uploads/")) {
    addEvidence(
      evidence,
      "robots-wp-uploads",
      15,
      "robots",
      "weak",
      "robots.txt references /wp-content/uploads/."
    );
  }
}

/**
 * Stage 3:
 * /wp-json/
 *
 * Very strong, but it is an application endpoint.
 * Run only if homepage and robots did not confirm.
 */
function detectFromRestApiRoot(body, evidence) {
  const raw = String(body || "");
  const lower = raw.toLowerCase();

  let parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch {
    parsed = null;
  }

  if (
    parsed &&
    typeof parsed === "object" &&
    Array.isArray(parsed.namespaces) &&
    parsed.namespaces.includes("wp/v2")
  ) {
    addEvidence(
      evidence,
      "wp-json-root-wp-v2",
      80,
      "rest_api",
      "definitive",
      "Valid /wp-json/ response includes wp/v2 namespace."
    );
    return;
  }

  if (
    parsed &&
    typeof parsed === "object" &&
    parsed.routes &&
    typeof parsed.routes === "object" &&
    Object.keys(parsed.routes).some((route) => route.startsWith("/wp/v2/"))
  ) {
    addEvidence(
      evidence,
      "wp-json-routes",
      75,
      "rest_api",
      "definitive",
      "Valid /wp-json/ response includes /wp/v2/ routes."
    );
    return;
  }

  if (
    lower.includes('"wp/v2"') ||
    lower.includes('"/wp/v2/posts"') ||
    lower.includes('"/wp/v2/pages"') ||
    lower.includes('"wp-site-health/v1"') ||
    lower.includes('"oembed/1.0"')
  ) {
    addEvidence(
      evidence,
      "wp-json-text-markers",
      55,
      "rest_api",
      "strong",
      "REST API response contains WordPress REST markers."
    );
  }
}

/**
 * Stage 4:
 * /wp-login.php
 *
 * Very strong, but more intrusive than robots/homepage.
 * Only run if still not confirmed.
 */
function detectFromLoginPage(response, evidence) {
  const lower = String(response.body || "").toLowerCase();
  const finalUrl = String(response.finalUrl || "").toLowerCase();

  const looksLikeWordPressLogin =
    lower.includes('id="loginform"') ||
    lower.includes("id='loginform'") ||
    lower.includes('id="user_login"') ||
    lower.includes("id='user_login'") ||
    lower.includes('id="user_pass"') ||
    lower.includes("id='user_pass'") ||
    lower.includes('id="wp-submit"') ||
    lower.includes("id='wp-submit'") ||
    lower.includes('name="log"') ||
    lower.includes("name='log'") ||
    lower.includes('name="pwd"') ||
    lower.includes("name='pwd'");

  if (
    looksLikeWordPressLogin &&
    (finalUrl.includes("/wp-login.php") ||
      finalUrl.includes("/wp-admin/") ||
      finalUrl.includes("redirect_to=") ||
      lower.includes("wordpress"))
  ) {
    addEvidence(
      evidence,
      "wp-login-form",
      90,
      "login",
      "definitive",
      "wp-login.php returned a WordPress-style login form."
    );
    return;
  }

  if (
    response.status === 200 &&
    lower.includes("wp-submit") &&
    lower.includes("user_login")
  ) {
    addEvidence(
      evidence,
      "wp-login-text-markers",
      75,
      "login",
      "definitive",
      "Login page contains WordPress login markers."
    );
  }
}

/**
 * Stage 5:
 * /wp-admin/
 *
 * This can redirect to wp-login.php on normal WordPress installs.
 * We use it after /wp-login.php, not before.
 */
function detectFromWpAdmin(response, evidence) {
  const finalUrl = String(response.finalUrl || "").toLowerCase();
  const lower = String(response.body || "").toLowerCase();

  if (
    finalUrl.includes("/wp-login.php") &&
    finalUrl.includes("redirect_to=")
  ) {
    addEvidence(
      evidence,
      "wp-admin-redirects-to-login",
      80,
      "admin",
      "definitive",
      "/wp-admin/ redirected to WordPress login with redirect_to parameter."
    );
    return;
  }

  if (
    lower.includes('id="loginform"') &&
    lower.includes("user_login") &&
    lower.includes("wp-submit")
  ) {
    addEvidence(
      evidence,
      "wp-admin-login-form",
      80,
      "admin",
      "definitive",
      "/wp-admin/ returned a WordPress-style login form."
    );
  }
}

/**
 * Stage 6:
 * admin-ajax.php
 *
 * Common body is simply "0".
 * Strong, but not as definitive by itself as wp-login or wp-json.
 */
function detectFromAdminAjax(response, evidence) {
  const body = String(response.body || "").trim();

  if ((response.status === 200 || response.status === 400) && body === "0") {
    addEvidence(
      evidence,
      "admin-ajax-zero-response",
      45,
      "admin",
      "strong",
      "admin-ajax.php returned the common WordPress body: 0."
    );
  }
}

/**
 * Stage 7:
 * xmlrpc.php
 *
 * Strong classic WordPress marker.
 * Many hardened sites block it, so failure means nothing.
 */
function detectFromXmlRpc(response, evidence) {
  const lower = String(response.body || "").toLowerCase();

  if (lower.includes("xml-rpc server accepts post requests only")) {
    addEvidence(
      evidence,
      "xmlrpc-classic-response",
      75,
      "xmlrpc",
      "definitive",
      "xmlrpc.php returned the classic WordPress XML-RPC response."
    );
  } else if (response.status === 405 && lower.includes("xml-rpc")) {
    addEvidence(
      evidence,
      "xmlrpc-405",
      35,
      "xmlrpc",
      "medium",
      "xmlrpc.php returned XML-RPC-like 405 response."
    );
  }
}

/**
 * Stage 8:
 * sitemaps
 *
 * Useful for headless/custom front ends.
 * Run last because it may require several URLs.
 */
function detectFromSitemap(url, response, evidence) {
  const lower = String(response.body || "").toLowerCase();

  if (!response.body) return;

  if (lower.includes("/wp-content/plugins/")) {
    addEvidence(
      evidence,
      "sitemap-plugin-path",
      65,
      "sitemap",
      "definitive",
      `${url} references /wp-content/plugins/.`
    );
  }

  if (lower.includes("/wp-content/themes/")) {
    addEvidence(
      evidence,
      "sitemap-theme-path",
      65,
      "sitemap",
      "definitive",
      `${url} references /wp-content/themes/.`
    );
  }

  if (lower.includes("wp-sitemap")) {
    addEvidence(
      evidence,
      "wp-core-sitemap",
      55,
      "sitemap",
      "strong",
      `${url} contains WordPress core sitemap marker.`
    );
  }

  if (
    lower.includes("yoast") ||
    lower.includes("rank-math") ||
    lower.includes("all in one seo") ||
    lower.includes("aioseo") ||
    lower.includes("xml-sitemap-feed")
  ) {
    addEvidence(
      evidence,
      "sitemap-seo-plugin-marker",
      50,
      "sitemap",
      "strong",
      `${url} contains common WordPress SEO/sitemap plugin marker.`
    );
  }

  if (
    lower.includes("/post-sitemap.xml") ||
    lower.includes("/page-sitemap.xml") ||
    lower.includes("/category-sitemap.xml") ||
    lower.includes("/sitemap-taxonomy-category.xml")
  ) {
    addEvidence(
      evidence,
      "sitemap-wordpress-url-patterns",
      40,
      "sitemap",
      "strong",
      `${url} contains WordPress-style sitemap URL patterns.`
    );
  }

  if (lower.includes("/wp-content/uploads/")) {
    addEvidence(
      evidence,
      "sitemap-upload-path",
      15,
      "sitemap",
      "weak",
      `${url} references /wp-content/uploads/.`
    );
  }
}

async function detectWordPress(inputUrl, options = {}) {
  const {
    politeDelayMs = parseInt(process.env.WORDPRESS_DETECT_DELAY_MS || "0", 10),
    includeProbeDetails = true,
  } = options;

  const rawInput = String(inputUrl || "").trim();
  let checkedUrl = normalizeUrl(rawInput, "https");

  const evidence = [];
  const completedStages = [];
  const probes = {};

  let homepage = await fetchLimited(checkedUrl, {
    accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  });

  if (!homepage.body && !/^https?:\/\//i.test(rawInput)) {
    const httpUrl = normalizeUrl(rawInput, "http");
    const httpHomepage = await fetchLimited(httpUrl, {
      accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    });

    if (httpHomepage.body) {
      checkedUrl = httpUrl;
      homepage = httpHomepage;
    }
  }

  const finalUrl = homepage.finalUrl || checkedUrl;
  const origin = getOrigin(finalUrl);

  let canonical = finalUrl;

  probes.homepage = {
    url: checkedUrl,
    status: homepage.status,
    finalUrl: homepage.finalUrl,
    error: homepage.error,
  };

  completedStages.push("homepage");

  if (homepage.body) {
    const homepageInfo = detectFromHomepage(homepage.body, finalUrl, evidence);
    canonical = homepageInfo.canonical || finalUrl;
  }

  if (shouldStop(evidence)) {
    return {
      inputUrl,
      checkedUrl,
      finalUrl,
      origin,
      canonical,
      ...summarizeEvidence(evidence, completedStages, true),
      evidence,
      probes: includeProbeDetails ? probes : undefined,
    };
  }

  if (politeDelayMs > 0) await sleep(politeDelayMs);

  /**
   * Stage 2: robots.txt
   */
  const robotsUrl = buildUrl(origin, "/robots.txt");
  const robots = await fetchLimited(robotsUrl, {
    accept: "text/plain,*/*",
    maxBytes: 150000,
  });

  probes.robots = {
    url: robotsUrl,
    status: robots.status,
    error: robots.error,
  };

  completedStages.push("robots");

  detectFromRobots(robots.body, evidence);

  if (shouldStop(evidence)) {
    return {
      inputUrl,
      checkedUrl,
      finalUrl,
      origin,
      canonical,
      ...summarizeEvidence(evidence, completedStages, true),
      evidence,
      probes: includeProbeDetails ? probes : undefined,
    };
  }

  if (politeDelayMs > 0) await sleep(politeDelayMs);

  /**
   * Stage 3: /wp-json/
   */
  const wpJsonUrl = buildUrl(origin, "/wp-json/");
  const wpJson = await fetchLimited(wpJsonUrl, {
    accept: "application/json,text/plain,*/*",
    maxBytes: 350000,
  });

  probes.wpJson = {
    url: wpJsonUrl,
    status: wpJson.status,
    error: wpJson.error,
  };

  completedStages.push("wp-json");

  detectFromRestApiRoot(wpJson.body, evidence);

  if (shouldStop(evidence)) {
    return {
      inputUrl,
      checkedUrl,
      finalUrl,
      origin,
      canonical,
      ...summarizeEvidence(evidence, completedStages, true),
      evidence,
      probes: includeProbeDetails ? probes : undefined,
    };
  }

  if (politeDelayMs > 0) await sleep(politeDelayMs);

  /**
   * Stage 4: /wp-login.php
   */
  const wpLoginUrl = buildUrl(origin, "/wp-login.php");
  const wpLogin = await fetchLimited(wpLoginUrl, {
    accept: "text/html,*/*",
    maxBytes: 350000,
  });

  probes.wpLogin = {
    url: wpLoginUrl,
    status: wpLogin.status,
    finalUrl: wpLogin.finalUrl,
    error: wpLogin.error,
  };

  completedStages.push("wp-login");

  detectFromLoginPage(wpLogin, evidence);

  if (shouldStop(evidence)) {
    return {
      inputUrl,
      checkedUrl,
      finalUrl,
      origin,
      canonical,
      ...summarizeEvidence(evidence, completedStages, true),
      evidence,
      probes: includeProbeDetails ? probes : undefined,
    };
  }

  if (politeDelayMs > 0) await sleep(politeDelayMs);

  /**
   * Stage 5: /wp-admin/
   */
  const wpAdminUrl = buildUrl(origin, "/wp-admin/");
  const wpAdmin = await fetchLimited(wpAdminUrl, {
    accept: "text/html,*/*",
    maxBytes: 350000,
  });

  probes.wpAdmin = {
    url: wpAdminUrl,
    status: wpAdmin.status,
    finalUrl: wpAdmin.finalUrl,
    error: wpAdmin.error,
  };

  completedStages.push("wp-admin");

  detectFromWpAdmin(wpAdmin, evidence);

  if (shouldStop(evidence)) {
    return {
      inputUrl,
      checkedUrl,
      finalUrl,
      origin,
      canonical,
      ...summarizeEvidence(evidence, completedStages, true),
      evidence,
      probes: includeProbeDetails ? probes : undefined,
    };
  }

  if (politeDelayMs > 0) await sleep(politeDelayMs);

  /**
   * Stage 6: admin-ajax.php
   */
  const adminAjaxUrl = buildUrl(origin, "/wp-admin/admin-ajax.php");
  const adminAjax = await fetchLimited(adminAjaxUrl, {
    accept: "text/plain,*/*",
    maxBytes: 50000,
  });

  probes.adminAjax = {
    url: adminAjaxUrl,
    status: adminAjax.status,
    error: adminAjax.error,
  };

  completedStages.push("admin-ajax");

  detectFromAdminAjax(adminAjax, evidence);

  if (shouldStop(evidence)) {
    return {
      inputUrl,
      checkedUrl,
      finalUrl,
      origin,
      canonical,
      ...summarizeEvidence(evidence, completedStages, true),
      evidence,
      probes: includeProbeDetails ? probes : undefined,
    };
  }

  if (politeDelayMs > 0) await sleep(politeDelayMs);

  /**
   * Stage 7: xmlrpc.php
   */
  const xmlrpcUrl = buildUrl(origin, "/xmlrpc.php");
  const xmlrpc = await fetchLimited(xmlrpcUrl, {
    accept: "text/plain,text/html,*/*",
    maxBytes: 100000,
  });

  probes.xmlrpc = {
    url: xmlrpcUrl,
    status: xmlrpc.status,
    error: xmlrpc.error,
  };

  completedStages.push("xmlrpc");

  detectFromXmlRpc(xmlrpc, evidence);

  if (shouldStop(evidence)) {
    return {
      inputUrl,
      checkedUrl,
      finalUrl,
      origin,
      canonical,
      ...summarizeEvidence(evidence, completedStages, true),
      evidence,
      probes: includeProbeDetails ? probes : undefined,
    };
  }

  if (politeDelayMs > 0) await sleep(politeDelayMs);

  /**
   * Stage 8: sitemaps
   *
   * Run one at a time and stop as soon as confirmed.
   */
  const sitemapPaths = [
    "/wp-sitemap.xml",
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/post-sitemap.xml",
    "/page-sitemap.xml",
    "/category-sitemap.xml",
    "/sitemap-taxonomy-category.xml",
  ];

  probes.sitemaps = [];
  completedStages.push("sitemaps");

  for (const path of sitemapPaths) {
    const sitemapUrl = buildUrl(origin, path);
    const sitemap = await fetchLimited(sitemapUrl, {
      accept: "application/xml,text/xml,text/plain,text/html,*/*",
      maxBytes: 350000,
    });

    probes.sitemaps.push({
      url: sitemapUrl,
      status: sitemap.status,
      error: sitemap.error,
    });

    detectFromSitemap(sitemapUrl, sitemap, evidence);

    if (shouldStop(evidence)) {
      return {
        inputUrl,
        checkedUrl,
        finalUrl,
        origin,
        canonical,
        ...summarizeEvidence(evidence, completedStages, true),
        evidence,
        probes: includeProbeDetails ? probes : undefined,
      };
    }

    if (politeDelayMs > 0) await sleep(politeDelayMs);
  }

  return {
    inputUrl,
    checkedUrl,
    finalUrl,
    origin,
    canonical,
    ...summarizeEvidence(evidence, completedStages, false),
    evidence,
    probes: includeProbeDetails ? probes : undefined,
  };
}

async function main() {
  const sites = process.argv.slice(2);

  if (sites.length === 0) {
    console.error("Usage:");
    console.error("  node detect-wordpress.js example.com");
    console.error("  node detect-wordpress.js example.com another-site.com");
    console.error("");
    console.error("Optional environment variables:");
    console.error("  WORDPRESS_DETECT_TIMEOUT_MS=8000");
    console.error("  WORDPRESS_DETECT_MAX_BYTES=750000");
    console.error("  WORDPRESS_DETECT_DELAY_MS=0");
    process.exit(1);
  }

  for (const site of sites) {
    const result = await detectWordPress(site);
    console.log(JSON.stringify(result, null, 2));
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

module.exports = {
  detectWordPress,
  detectWordPressFromHtml,
};