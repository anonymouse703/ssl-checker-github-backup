const { httpGet } = require('../utils/http');
const { getErrorCode } = require('../utils/error-codes');
const { ENDPOINTS } = require('../config/constants');

async function runPageRank(domain, context) {
  const start = Date.now();
  const { paths } = context; 

  try {
    const PAGERANK_API_KEY = context.env.PAGERANK_API_KEY || "";
    
    if (!PAGERANK_API_KEY) {
      throw new Error("No API key configured");
    }


    const url = `${ENDPOINTS.PAGERANK_API}?domains[]=${encodeURIComponent(domain)}`;
    
    const options = {
      headers: {
        'API-OPR': PAGERANK_API_KEY,
        'User-Agent': 'ssl-checker-tool/1.0'
      }
    };
    
    
    const data = await httpGet(url, options);

    if (data && data.status_code === 200 && data.response && data.response.length > 0) {
      const rankData = data.response[0];
      

      return {
        status: "SUCCESS",
        data: {
          page_rank_integer: rankData.page_rank_integer,
          page_rank_decimal: rankData.page_rank_decimal,
          rank: rankData.rank,
          domain: rankData.domain,
          error: rankData.error,
        },
        error: null,
        errorCode: null,
        url: `https://www.domcop.com/openpagerank/${domain}`,
        screenshot: null
      };
      
    } else {
      const errorMsg = data?.response?.[0]?.error || data?.error || "Unknown API error";
      throw new Error(`API error: ${errorMsg} (Status: ${data?.status_code || 'unknown'})`);
    }

  } catch (err) {

    return {
      status: "SKIPPED",
      data: {
        page_rank_integer: null,
        page_rank_decimal: null,
        rank: null,
      },
      error: err.message,
      errorCode: getErrorCode({ error: err.message }),
      url: `https://www.domcop.com/openpagerank/${domain}`,
      screenshot: null
    };
  }
}

function formatDuration(ms) {
  const totalSec = Math.round(ms / 1000);
  const mins = Math.floor(totalSec / 60);
  const secs = totalSec % 60;
  return mins === 0 ? `${secs} sec` : `${mins} min ${secs} sec`;
}

module.exports = { runPageRank };