import { createClient } from '@supabase/supabase-js';

/**
 * Vercel serverless API: fetch HK stock financial data from East Money
 * (same data source as akshare's stock_financial_hk_report_em)
 * and upsert into Supabase hk_balance_sheet / hk_income_statement / hk_cash_flow.
 *
 * Query:  GET /api/fetch-hk-data?symbol=01211.HK
 */

const REPORT_CFG = [
  { key: 'balance_sheet',    table: 'hk_balance_sheet',      reportName: 'RPT_HKF10_FN_BALANCE_PC' },
  { key: 'income_statement', table: 'hk_income_statement',   reportName: 'RPT_HKF10_FN_INCOME_PC' },
  { key: 'cash_flow',        table: 'hk_cash_flow',          reportName: 'RPT_HKF10_FN_CASHFLOW_PC' },
];

const EM_BASE = 'https://datacenter.eastmoney.com/securities/api/data/v1/get';
const EM_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
  'Referer': 'https://emweb.securities.eastmoney.com/',
};
const ENIU_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
  'Referer': 'https://eniu.com/',
};
const HKD_TO_CNY = 0.901;

function normalizeSymbol(raw) {
  let s = (raw || '').trim().toUpperCase().replace(/\.HK$/i, '');
  return s.padStart(5, '0');
}

function toDateStr(v) {
  if (!v) return null;
  const m = String(v).match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function toFloat(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toInt(v) {
  if (v == null || v === '') return null;
  const n = parseInt(v, 10);
  return Number.isFinite(n) ? n : null;
}

function normalizeCompanyDesc(s) {
  if (!s) return '';
  return String(s)
    .toLowerCase()
    .replace(/\bclass\s+[ah]\b/g, ' ')
    .replace(/\bshares?\b/g, ' ')
    .replace(/\blimited\b/g, ' ')
    .replace(/\bco\.?\b/g, ' ')
    .replace(/\bltd\.?\b/g, ' ')
    .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

async function fetchReportDates(secucode) {
  const url = new URL(EM_BASE);
  url.searchParams.set('reportName', 'RPT_CUSTOM_HKSK_APPFN_CASHFLOW_SUMMARY');
  url.searchParams.set('columns', 'SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,START_DATE,REPORT_DATE,FISCAL_YEAR,CURRENCY,ACCOUNT_STANDARD,REPORT_TYPE');
  url.searchParams.set('quoteColumns', '');
  url.searchParams.set('filter', `(SECUCODE="${secucode}.HK")`);
  url.searchParams.set('source', 'F10');
  url.searchParams.set('client', 'PC');
  url.searchParams.set('v', '02092616586970355');

  const resp = await fetch(url.toString(), { headers: EM_HEADERS });
  if (!resp.ok) throw new Error(`East Money summary API returned HTTP ${resp.status}`);
  const json = await resp.json();

  const reportList = json?.result?.data?.[0]?.REPORT_LIST;
  if (!Array.isArray(reportList) || reportList.length === 0) return [];

  return reportList.map(r => {
    const dt = r.REPORT_DATE || '';
    return dt.split(' ')[0];
  }).filter(Boolean);
}

const BS_COLUMNS = 'SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,ORG_CODE,REPORT_DATE,DATE_TYPE_CODE,FISCAL_YEAR,STD_ITEM_CODE,STD_ITEM_NAME,AMOUNT,STD_REPORT_DATE';
const IS_CF_COLUMNS = 'SECUCODE,SECURITY_CODE,SECURITY_NAME_ABBR,ORG_CODE,REPORT_DATE,DATE_TYPE_CODE,FISCAL_YEAR,START_DATE,STD_ITEM_CODE,STD_ITEM_NAME,AMOUNT';

async function fetchAllPages(secucode, reportName, reportDates, cfgKey) {
  if (!reportDates.length) return [];

  const dateLiteral = "'" + reportDates.join("','") + "'";
  const filterStr = `(SECUCODE="${secucode}.HK")(REPORT_DATE in (${dateLiteral}))`;
  const columns = cfgKey === 'balance_sheet' ? BS_COLUMNS : IS_CF_COLUMNS;

  const rows = [];
  let page = 1;

  while (true) {
    const url = new URL(EM_BASE);
    url.searchParams.set('reportName', reportName);
    url.searchParams.set('columns', columns);
    url.searchParams.set('quoteColumns', '');
    url.searchParams.set('filter', filterStr);
    url.searchParams.set('pageNumber', String(page));
    url.searchParams.set('pageSize', '');
    url.searchParams.set('sortTypes', '-1,1');
    url.searchParams.set('sortColumns', 'REPORT_DATE,STD_ITEM_CODE');
    url.searchParams.set('source', 'F10');
    url.searchParams.set('client', 'PC');
    url.searchParams.set('v', '01975982096513973');

    const resp = await fetch(url.toString(), { headers: EM_HEADERS });
    if (!resp.ok) throw new Error(`East Money API returned HTTP ${resp.status}`);

    const json = await resp.json();
    const data = json?.result?.data;
    if (!Array.isArray(data) || data.length === 0) break;

    rows.push(...data);

    const totalCount = json?.result?.count || 0;
    if (rows.length >= totalCount) break;
    page++;
  }

  return rows;
}

function extractYear(raw) {
  if (raw.ORIG_FISCAL_YEAR) {
    const m = String(raw.ORIG_FISCAL_YEAR).match(/^(\d{4})/);
    if (m) return parseInt(m[1], 10);
  }
  return null;
}

function mapRow(raw, tableKey) {
  const base = {
    secucode:          raw.SECUCODE || null,
    security_code:     raw.SECURITY_CODE || null,
    security_name:     raw.SECURITY_NAME_ABBR || null,
    org_code:          raw.ORG_CODE || null,
    report_date:       toDateStr(raw.REPORT_DATE),
    date_type_code:    toInt(raw.DATE_TYPE_CODE),
    fiscal_year:       extractYear(raw),
    std_item_code:     raw.STD_ITEM_CODE || null,
    std_item_name:     raw.ITEM_NAME || raw.STD_ITEM_NAME || null,
    amount:            toFloat(raw.AMOUNT),
  };
  if (tableKey === 'balance_sheet') {
    base.std_report_date = toDateStr(raw.STD_REPORT_DATE);
  } else {
    base.start_date = toDateStr(raw.START_DATE) || null;
  }
  return base;
}

async function upsertBatch(supabase, table, rows, batchSize = 500, onConflict = 'secucode,report_date,std_item_code') {
  let inserted = 0;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const { error } = await supabase.from(table).upsert(batch, {
      onConflict,
    });
    if (error) throw new Error(`Supabase upsert to ${table} failed: ${error.message}`);
    inserted += batch.length;
  }
  return inserted;
}

async function fetchAksharePriceSeries(code5) {
  // AkShare stock_hk_hist(...) ultimately maps to EastMoney K-line data.
  // We pull unadjusted daily close (fqt=0) to align with market-cap snapshots.
  const url = new URL('https://push2his.eastmoney.com/api/qt/stock/kline/get');
  url.searchParams.set('secid', `116.${code5}`);
  url.searchParams.set('fields1', 'f1,f2,f3,f4,f5,f6');
  url.searchParams.set('fields2', 'f51,f52,f53,f54,f55,f56,f57,f58');
  url.searchParams.set('klt', '101');
  url.searchParams.set('fqt', '0');
  url.searchParams.set('beg', '20000101');
  url.searchParams.set('end', '20500101');
  const resp = await fetch(url.toString(), { headers: EM_HEADERS });
  if (!resp.ok) throw new Error(`EastMoney HK price API returned HTTP ${resp.status}`);
  const json = await resp.json();
  const klines = json?.data?.klines;
  if (!Array.isArray(klines)) {
    return [];
  }
  const out = [];
  for (const line of klines) {
    const parts = String(line).split(',');
    // Format: date,open,close,high,low,vol,amount,amp
    const date = toDateStr(parts[0]);
    const close = toFloat(parts[2]);
    if (!date || close == null || close <= 0) continue;
    out.push({ date, close_hkd: close });
  }
  return out;
}

async function fetchHkseSnapshots(supabase, code5) {
  const { data, error } = await supabase
    .from('hkse_market')
    .select('symbol,download_date,market_capitalization')
    .eq('symbol', code5)
    .order('download_date', { ascending: true });
  if (error) throw new Error(`Read hkse_market failed: ${error.message}`);
  return (data || []).filter((r) => toDateStr(r.download_date) && toFloat(r.market_capitalization) != null);
}

function median(nums) {
  if (!nums.length) return null;
  const a = [...nums].sort((x, y) => x - y);
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}

function nearestPriceOnOrAroundDate(priceRowsAsc, targetDate, maxGapDays = 7) {
  const targetMs = new Date(targetDate).getTime();
  if (!Number.isFinite(targetMs)) return null;
  let best = null;
  let bestGap = Number.POSITIVE_INFINITY;
  for (const r of priceRowsAsc) {
    const ms = new Date(r.date).getTime();
    if (!Number.isFinite(ms)) continue;
    const gapDays = Math.abs(ms - targetMs) / 86400000;
    if (gapDays < bestGap) {
      bestGap = gapDays;
      best = r;
    }
  }
  if (!best || !Number.isFinite(bestGap) || bestGap > maxGapDays) return null;
  return best;
}

function computeMarketValueSeriesFromPrice(priceRowsAsc, shares) {
  return priceRowsAsc.map((r) => ({
    date: r.date,
    market_value_hkd_亿元: (r.close_hkd * shares) / 1e8,
  }));
}

async function resolveAshareSymbolForDualListed(supabase, code5) {
  // Detect A+H by matching normalized company descriptions.
  const { data: hkRows, error: hkErr } = await supabase
    .from('hkse_market')
    .select('description,download_date')
    .eq('symbol', code5)
    .order('download_date', { ascending: false })
    .limit(1);
  if (hkErr) throw new Error(`Read hkse_market description failed: ${hkErr.message}`);
  const hkDesc = hkRows?.[0]?.description || '';
  const norm = normalizeCompanyDesc(hkDesc);
  if (!norm) return null;

  const terms = Array.from(new Set(norm.split(' ').filter((t) => t && t.length >= 2)));
  const queryTerms = terms.slice(0, 3);
  if (!queryTerms.length) return null;

  const candidatesBySymbol = new Map();
  for (const term of queryTerms) {
    const { data, error } = await supabase
      .from('share_a_market')
      .select('symbol,description,download_date')
      .ilike('description', `%${term}%`)
      .order('download_date', { ascending: false })
      .limit(100);
    if (error) throw new Error(`Search share_a_market failed: ${error.message}`);
    for (const r of data || []) {
      const d = normalizeCompanyDesc(r.description || '');
      if (!d) continue;
      if (!queryTerms.some((t) => d.includes(t))) continue;
      const prev = candidatesBySymbol.get(r.symbol);
      if (!prev || String(r.download_date || '') > String(prev.download_date || '')) {
        candidatesBySymbol.set(r.symbol, r);
      }
    }
  }

  if (!candidatesBySymbol.size) return null;
  const ranked = Array.from(candidatesBySymbol.values()).map((r) => {
    const d = normalizeCompanyDesc(r.description || '');
    const score = queryTerms.reduce((acc, t) => acc + (d.includes(t) ? 1 : 0), 0);
    return { ...r, score };
  }).sort((a, b) => (b.score - a.score) || String(b.download_date || '').localeCompare(String(a.download_date || '')));

  const best = ranked[0];
  if (!best || best.score <= 0) return null;
  return String(best.symbol || '').trim() || null;
}

async function fetchAshareMktCapSeriesAsHkUnit(supabase, ashareSymbol) {
  // Fetch A-share market cap (bn CNY) and convert to 亿CNY for storage.
  // Storage convention: market_value_hkd_亿元 actually stores CNY 亿 for dual-listed companies.
  const out = [];
  const batchSize = 1000;
  let offset = 0;
  while (true) {
    const { data, error } = await supabase
      .from('cn_mkt_cap_10y')
      .select('trade_date,mkt_cap_billion_cny')
      .eq('symbol', ashareSymbol)
      .order('trade_date', { ascending: true })
      .range(offset, offset + batchSize - 1);
    if (error) throw new Error(`Read cn_mkt_cap_10y failed: ${error.message}`);
    if (!data || data.length === 0) break;
    for (const r of data) {
      const date = toDateStr(r.trade_date);
      const bn = toFloat(r.mkt_cap_billion_cny);
      if (!date || bn == null) continue;
      out.push({
        date,
        // Store as 亿CNY (bn * 10), not HKD. Field name is legacy but unit is CNY for dual-listed.
        market_value_hkd_亿元: bn * 10,
      });
    }
    if (data.length < batchSize) break;
    offset += batchSize;
  }
  return out;
}

async function fetchAsharePriceSeries(symbol6) {
  const marketPrefix = String(symbol6).startsWith('6') ? '1' : '0';
  const url = new URL('https://push2his.eastmoney.com/api/qt/stock/kline/get');
  url.searchParams.set('secid', `${marketPrefix}.${symbol6}`);
  url.searchParams.set('fields1', 'f1,f2,f3,f4,f5,f6');
  url.searchParams.set('fields2', 'f51,f52,f53,f54,f55,f56,f57,f58');
  url.searchParams.set('klt', '101');
  url.searchParams.set('fqt', '0');
  url.searchParams.set('beg', '20000101');
  url.searchParams.set('end', '20500101');
  const resp = await fetch(url.toString(), { headers: EM_HEADERS });
  if (!resp.ok) throw new Error(`EastMoney A-share price API returned HTTP ${resp.status}`);
  const json = await resp.json();
  const klines = json?.data?.klines;
  if (!Array.isArray(klines)) return [];
  const out = [];
  for (const line of klines) {
    const parts = String(line).split(',');
    const date = toDateStr(parts[0]);
    const close = toFloat(parts[2]);
    if (!date || close == null || close <= 0) continue;
    out.push({ date, close_cny: close });
  }
  return out;
}

async function inferAshareSharesFromSnapshots(supabase, ashareSymbol, asharePriceRows) {
  const { data, error } = await supabase
    .from('share_a_market')
    .select('download_date,market_capitalization')
    .eq('symbol', ashareSymbol)
    .order('download_date', { ascending: true });
  if (error) throw new Error(`Read share_a_market snapshots failed: ${error.message}`);
  const candidates = [];
  for (const s of data || []) {
    const mcap = toFloat(s.market_capitalization);
    const dt = toDateStr(s.download_date);
    if (mcap == null || !dt) continue;
    const near = nearestPriceOnOrAroundDate(
      asharePriceRows.map((r) => ({ date: r.date, close_hkd: r.close_cny })),
      dt,
      7
    );
    if (!near || near.close_hkd <= 0) continue;
    candidates.push(mcap / near.close_hkd);
  }
  return {
    shares: median(candidates),
    candidateCount: candidates.length,
    snapshotCount: (data || []).length,
  };
}

async function inferSharesFromBalanceSheet(supabase, secucode) {
  // Prefer "股本" from HK balance sheet.
  // For HK firms this is often numerically close to total shares (par value commonly 1).
  const candidateNames = ['股本', '普通股股本', '发行股本'];
  const { data, error } = await supabase
    .from('hk_balance_sheet')
    .select('amount,std_item_name,report_date')
    .eq('secucode', secucode)
    .in('std_item_name', candidateNames)
    .order('report_date', { ascending: false })
    .limit(50);
  if (error) throw new Error(`Read hk_balance_sheet share capital failed: ${error.message}`);
  const rows = data || [];
  for (const r of rows) {
    const n = toFloat(r?.amount);
    if (n == null || n <= 0) continue;
    // Heuristic:
    // - If already in "shares", this is typically >= 1e7.
    // - If accidentally stored in "亿股", multiply back.
    const shares = n >= 1e7 ? n : n * 1e8;
    if (Number.isFinite(shares) && shares > 0) return shares;
  }
  return null;
}

async function upsertMarketCapHistory(supabase, code5, fallbackMeta) {
  const secucode = `${code5}.HK`;
  let method = 'price_x_shares';
  let ashareSymbol = null;
  let inferredShares = null;
  let shareSource = null;
  let snapshots = [];
  let shareCandidates = [];
  let mktRows = [];

  // Rule #1: A+H dual-listed -> use A-share market-cap sequence.
  ashareSymbol = await resolveAshareSymbolForDualListed(supabase, code5);
  if (ashareSymbol) {
    const ashareRows = await fetchAshareMktCapSeriesAsHkUnit(supabase, ashareSymbol);
    if (ashareRows.length) {
      mktRows = ashareRows;
      method = 'dual_listed_use_ashare';
    } else {
      // Fallback for dual-listed: A-share price * A-share shares inferred from share_a_market snapshots.
      const asharePrices = await fetchAsharePriceSeries(ashareSymbol);
      if (asharePrices.length) {
        const shareInfo = await inferAshareSharesFromSnapshots(supabase, ashareSymbol, asharePrices);
        if (shareInfo.shares && Number.isFinite(shareInfo.shares) && shareInfo.shares > 0) {
          mktRows = asharePrices.map((r) => ({
            date: r.date,
            // CNY 亿 -> HKD 亿 for storage convention
            market_value_hkd_亿元: ((r.close_cny * shareInfo.shares) / 1e8) / HKD_TO_CNY,
          }));
          method = 'dual_listed_use_ashare_price_x_shares';
          inferredShares = shareInfo.shares;
          shareSource = 'share_a_market';
          snapshots = Array(shareInfo.snapshotCount).fill(0);
          shareCandidates = Array(shareInfo.candidateCount).fill(0);
        }
      }
    }
  }

  // Rule #2: HK-only fallback -> price * shares.
  if (!mktRows.length) {
    const priceRows = await fetchAksharePriceSeries(code5);
    if (!priceRows.length) return { fetched: 0, upserted: 0, method: 'price_x_shares' };

    inferredShares = await inferSharesFromBalanceSheet(supabase, secucode);
    if (inferredShares && Number.isFinite(inferredShares) && inferredShares > 0) {
      shareSource = 'hk_balance_sheet';
    }

    snapshots = await fetchHkseSnapshots(supabase, code5);
    if (!inferredShares) {
      for (const s of snapshots) {
        const mcap = toFloat(s.market_capitalization);
        const dt = toDateStr(s.download_date);
        if (mcap == null || !dt) continue;
        const near = nearestPriceOnOrAroundDate(priceRows, dt, 7);
        if (!near || near.close_hkd <= 0) continue;
        shareCandidates.push(mcap / near.close_hkd);
      }
      inferredShares = median(shareCandidates);
      if (inferredShares && Number.isFinite(inferredShares) && inferredShares > 0) {
        shareSource = 'hkse_market';
      }
    }

    if (inferredShares == null || !Number.isFinite(inferredShares) || inferredShares <= 0) {
      return { fetched: 0, upserted: 0, method: 'price_x_shares', error: 'cannot infer shares from hkse snapshots' };
    }
    mktRows = computeMarketValueSeriesFromPrice(priceRows, inferredShares);
  }

  let meta = null;
  try {
    const { data, error } = await supabase
      .from('hk_balance_sheet')
      .select('secucode,security_code,security_name')
      .eq('secucode', secucode)
      .order('report_date', { ascending: false })
      .limit(1);
    if (error) throw error;
    meta = data?.[0] || null;
  } catch {
    meta = null;
  }

  const secucodeVal = meta?.secucode || fallbackMeta?.secucode || secucode;
  const securityCodeVal = meta?.security_code || fallbackMeta?.security_code || code5;
  const securityNameAbbrVal = meta?.security_name || fallbackMeta?.security_name || null;

  const payload = mktRows.map((r) => ({
    date: r.date,
    market_value_hkd_亿元: r.market_value_hkd_亿元,
    secucode: secucodeVal,
    security_code: securityCodeVal,
    security_name_abbr: securityNameAbbrVal,
  }));

  const upserted = await upsertBatch(supabase, 'hk_company_mkt_cap', payload, 500, 'secucode,date');
  return {
    fetched: mktRows.length,
    upserted,
    method,
    ashare_symbol: ashareSymbol,
    share_source: shareSource,
    inferred_shares: inferredShares,
    snapshot_count: snapshots?.length || 0,
    share_candidate_count: shareCandidates.length,
  };
}

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,OPTIONS');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const rawSymbol = req.query?.symbol || '';
  const code = normalizeSymbol(rawSymbol);

  if (!code || code === '00000') {
    return res.status(400).json({ error: 'Missing or invalid symbol parameter. Example: 01211.HK' });
  }

  const sbUrl = process.env.SUPABASE_URL;
  const sbKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!sbUrl || !sbKey) {
    return res.status(500).json({ error: 'Supabase credentials not configured' });
  }

  const supabase = createClient(sbUrl, sbKey);
  const results = {};
  let fallbackMeta = null;

  try {
    const reportDates = await fetchReportDates(code);
    if (!reportDates.length) {
      return res.status(404).json({ error: `No report dates found for ${code}.HK` });
    }

    for (const cfg of REPORT_CFG) {
      const emRows = await fetchAllPages(code, cfg.reportName, reportDates, cfg.key);
      if (!emRows.length) {
        results[cfg.key] = { fetched: 0, upserted: 0 };
        continue;
      }

      const mapped = emRows.map(r => mapRow(r, cfg.key)).filter(r => r.report_date && r.std_item_code);
      if (!fallbackMeta && mapped.length) {
        fallbackMeta = {
          secucode: mapped[0].secucode,
          security_code: mapped[0].security_code,
          security_name: mapped[0].security_name,
        };
      }
      const upserted = await upsertBatch(supabase, cfg.table, mapped);
      results[cfg.key] = { fetched: emRows.length, upserted };
    }

    // Also fetch AkShare market value history and upsert to hk_company_mkt_cap.
    // secucode/security_code/security_name_abbr are aligned with hk_balance_sheet company info.
    results.market_cap = await upsertMarketCapHistory(supabase, code, fallbackMeta);

    return res.status(200).json({
      ok: true,
      symbol: `${code}.HK`,
      message: `港股 ${code}.HK 数据下载完成`,
      details: results,
    });
  } catch (err) {
    console.error('fetch-hk-data error:', err);
    return res.status(500).json({ error: err.message || 'Internal server error' });
  }
}
