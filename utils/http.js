const https = require("https");
const http = require("http");

function httpGet(url, options = {}) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    lib
      .get(
        url,
        { 
          headers: { 
            "User-Agent": "ssl-checker-tool/1.0", 
            ...options.headers 
          } 
        },
        (res) => {
          let data = "";
          res.on("data", (chunk) => (data += chunk));
          res.on("end", () => {
            try {
              resolve(JSON.parse(data));
            } catch (e) {
              resolve(data);
            }
          });
        },
      )
      .on("error", reject);
  });
}

function httpPost(url, body, authKey) {
  return new Promise((resolve, reject) => {
    const bodyStr = JSON.stringify(body);
    const auth = Buffer.from(`${authKey}:`).toString("base64");
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname,
      method: "POST",
      headers: {
        "Content-Type": "application/vnd.api+json",
        "Content-Length": Buffer.byteLength(bodyStr),
        Authorization: `Basic ${auth}`,
      },
    };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch (e) {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });
    req.on("error", reject);
    req.write(bodyStr);
    req.end();
  });
}

function httpGetAuth(url, authKey) {
  return new Promise((resolve, reject) => {
    const auth = Buffer.from(`${authKey}:`).toString("base64");
    const urlObj = new URL(url);
    const options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + urlObj.search,
      method: "GET",
      headers: { Authorization: `Basic ${auth}` },
    };
    const req = https.request(options, (res) => {
      let data = "";
      res.on("data", (chunk) => (data += chunk));
      res.on("end", () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch (e) {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });
    req.on("error", reject);
    req.end();
  });
}

function fetch(url) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith("https") ? https : http;
    lib
      .get(url, (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            resolve({
              status: res.statusCode,
              json: () => Promise.resolve(JSON.parse(data)),
            });
          } catch (e) {
            resolve({
              status: res.statusCode,
              json: () =>
                Promise.resolve({
                  error: { message: "Invalid JSON response" },
                }),
            });
          }
        });
      })
      .on("error", reject);
  });
}

module.exports = { httpGet, httpPost, httpGetAuth, fetch };