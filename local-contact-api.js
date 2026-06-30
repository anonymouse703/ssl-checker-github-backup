"use strict";

const path = require("path");
const express = require("express");
const puppeteer = require("puppeteer");

const { scrapeContact } = require("./services/scrape-contact");
const { runUiAudit } = require("./services/ui-audit");
const { checkBrokenLinks } = require("./services/broken-links");

const app = express();
const PORT = 3100;

let browser = null;

function normalizeDomain(input) {
  let s = String(input || "").trim();

  s = s.replace(/^https?:\/\//i, "");
  s = s.replace(/^www\./i, "");
  s = s.split("/")[0];
  s = s.split("?")[0];
  s = s.split("#")[0];

  return s.toLowerCase();
}

function parseTarget(req) {
  const rawUrl = String(req.query.url || "").trim();
  const rawDomain = String(req.query.domain || "").trim();

  if (rawUrl && /^https?:\/\//i.test(rawUrl)) {
    const u = new URL(rawUrl);

    return {
      domain: normalizeDomain(u.hostname),
      url: rawUrl,
      startPath: u.pathname || "/"
    };
  }

  if (rawDomain) {
    return {
      domain: normalizeDomain(rawDomain),
      url: "",
      startPath: String(req.query.path || "/")
    };
  }

  return {
    domain: "",
    url: "",
    startPath: "/"
  };
}

async function getBrowser() {
  if (browser) return browser;

  browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage"
    ]
  });

  return browser;
}

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "local-audit-api",
    endpoints: [
      "/scrape-contact?domain=example.com",
      "/broken-links?domain=example.com",
      "/ui-audit?domain=example.com"
    ]
  });
});

app.get("/scrape-contact", async (req, res) => {
  const target = parseTarget(req);

  if (!target.domain) {
    return res.status(400).json({
      ok: false,
      error: "Missing domain. Use /scrape-contact?domain=example.com or /scrape-contact?url=https://example.com/contact/"
    });
  }

  try {
    const b = await getBrowser();

    const result = await scrapeContact({
      domain: target.domain,
      url: target.url,
      startPath: target.startPath,
      newTab: () => b.newPage(),
      timeoutMs: Number(req.query.timeoutMs || 20000),
      debug: String(req.query.debug || "") === "1"
    });

    return res.json({
      ok: true,
      domain: target.domain,
      url: target.url,
      startPath: target.startPath,
      result
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      domain: target.domain,
      error: err.message || String(err)
    });
  }
});

app.get("/broken-links", async (req, res) => {
  const target = parseTarget(req);

  if (!target.domain) {
    return res.status(400).json({
      ok: false,
      error: "Missing domain. Use /broken-links?domain=example.com"
    });
  }

  try {
    const b = await getBrowser();

    const result = await checkBrokenLinks({
      domain: target.domain,
      url: target.url,
      startPath: target.startPath,
      newTab: () => b.newPage(),
      timeoutMs: Number(req.query.timeoutMs || 20000),
      linkTimeoutMs: Number(req.query.linkTimeoutMs || 12000),
      maxPages: Number(req.query.maxPages || 1),
      maxLinks: Number(req.query.maxLinks || 80),
      concurrency: Number(req.query.concurrency || 5),
      includeExternal: String(req.query.includeExternal || "") === "1",
      debug: String(req.query.debug || "") === "1"
    });

    return res.json({
      ok: true,
      domain: target.domain,
      url: target.url,
      startPath: target.startPath,
      result
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      domain: target.domain,
      error: err.message || String(err)
    });
  }
});

app.get("/ui-audit", async (req, res) => {
  const target = parseTarget(req);

  if (!target.domain) {
    return res.status(400).json({
      ok: false,
      error: "Missing domain. Use /ui-audit?domain=example.com"
    });
  }

  try {
    const b = await getBrowser();

    const screenshotDir = path.join(
      __dirname,
      "local-output",
      "ui-screenshots"
    );

    const result = await runUiAudit({
      domain: target.domain,
      url: target.url,
      startPath: target.startPath,
      newTab: () => b.newPage(),
      timeoutMs: Number(req.query.timeoutMs || 25000),
      maxPages: Number(req.query.maxPages || 1),
      maxLinks: Number(req.query.maxLinks || 80),
      includeExternal: String(req.query.includeExternal || "") === "1",
      screenshotDir,
      takeScreenshots: String(req.query.screenshots || "1") !== "0",
      checkLinks: String(req.query.checkLinks || "1") !== "0",
      debug: String(req.query.debug || "") === "1"
    });

    return res.json({
      ok: true,
      domain: target.domain,
      url: target.url,
      startPath: target.startPath,
      result
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      domain: target.domain,
      error: err.message || String(err)
    });
  }
});

process.on("SIGINT", async () => {
  if (browser) {
    try {
      await browser.close();
    } catch (_) {}
  }

  process.exit(0);
});

app.listen(PORT, () => {
  console.log("Local audit API running:");
  console.log(`http://localhost:${PORT}/health`);
  console.log(`http://localhost:${PORT}/scrape-contact?domain=example.com`);
  console.log(`http://localhost:${PORT}/broken-links?domain=example.com`);
  console.log(`http://localhost:${PORT}/ui-audit?domain=example.com`);
});