function getErrorCode(data) {
  if (!data || !data.error) return null;
  const error = data.error.toLowerCase();
  if (error.includes("403")) return "403";
  if (error.includes("404")) return "404";
  if (error.includes("429")) return "429";
  if (error.includes("500")) return "500";
  if (error.includes("502")) return "502";
  if (error.includes("503")) return "503";
  if (error.includes("444")) return "444";
  if (error.includes("521")) return "521";
  if (error.includes("523")) return "523";
  if (error.includes("525")) return "525";
  if (error.includes("capacity") || error.includes("overloaded"))
    return "RATE_LIMIT";
  if (error.includes("dns") || error.includes("resolve")) return "DNS_ERROR";
  if (error.includes("timeout")) return "TIMEOUT";
  if (error.includes("ssl") || error.includes("certificate"))
    return "SSL_ERROR";
  return "UNKNOWN";
}

module.exports = { getErrorCode };