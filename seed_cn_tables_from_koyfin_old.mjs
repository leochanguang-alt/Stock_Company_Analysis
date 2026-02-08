import "dotenv/config";
import path from "node:path";
import { chromium } from "playwright-core";
import { createClient } from "@supabase/supabase-js";

function argValue(name, defaultValue = null) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return defaultValue;
  const v = process.argv[idx + 1];
  if (!v || v.startsWith("--")) return defaultValue;
  return v;
}

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// old dashboard wide rows are in CNY (元). cn_* tables use 亿元.
function yuanToYiYuan(v) {
  const n = toNum(v);
  if (n === null) return null;
  return n / 1e8;
}

async function extractWideRowsFromOld(symbol) {
  const oldFile = path.resolve(process.cwd(), "koyfin_dashboard_002508.html");
  const oldUrl = `file://${oldFile}`;

  const browser = await chromium.launch({ headless: true, channel: "chrome" });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();

  await page.goto(oldUrl, { waitUntil: "domcontentloaded" });
  await page.locator("#company-search-input").waitFor({ timeout: 60_000 });
  await page.fill("#company-search-input", symbol);
  await page.dispatchEvent("#company-search-input", "change");
  await page.keyboard.press("Enter").catch(() => {});

  // Build rows using the same functions defined in the old dashboard.
  const payload = await page.evaluate(async (sym) => {
    const recs = await fetchAllFinancials(sym);
    const marketCaps = await fetchMarketCapHistory(sym);
    let rows = buildWideRows(recs);
    rows = attachMarketCap(rows, marketCaps);
    // Ensure stable order asc by date
    rows = (rows || []).slice().sort((a, b) => String(a.report_date).localeCompare(String(b.report_date)));
    return { rowsCount: rows.length, rows };
  }, symbol);

  await browser.close();
  return payload;
}

async function batchUpsert(supabase, table, rows, onConflict) {
  const batchSize = 500;
  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const { error } = await supabase.from(table).upsert(batch, { onConflict });
    if (error) throw new Error(`${table} upsert failed: ${error.message}`);
  }
}

async function run() {
  const symbol = argValue("--symbol", "002508");
  if (!/^\d{6}$/.test(symbol)) throw new Error("symbol must be 6 digits");

  const url = process.env.SUPABASE_URL;
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !key) throw new Error("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in environment");

  console.log(`Extracting wide rows from old dashboard for ${symbol}...`);
  const { rowsCount, rows } = await extractWideRowsFromOld(symbol);
  console.log(`Got rows: ${rowsCount}`);

  const supabase = createClient(url, key);

  // Prepare cn_* upserts from old wide rows.
  const bsUpserts = [];
  const isUpserts = [];
  const cfUpserts = [];

  for (const r of rows || []) {
    const report_date = r?.report_date ? String(r.report_date).slice(0, 10) : null;
    if (!report_date || !/^\d{4}-\d{2}-\d{2}$/.test(report_date)) continue;

    // Balance sheet (亿元)
    const bs = {
      report_date,
      symbol,
      security_name: null,
      report_date_name: null,
      monetaryfunds: yuanToYiYuan(r.Cash_Equivalents),
      total_current_assets: yuanToYiYuan(r.Total_Current_Assets),
      total_noncurrent_assets: yuanToYiYuan(r.Total_NonCurrent_Assets),
      total_assets: yuanToYiYuan(r.Total_Assets),
      short_loan: yuanToYiYuan(r.Short_Term_Debt),
      long_loan: yuanToYiYuan(r.Long_Term_Debt),
      bond_payable: yuanToYiYuan(r.Bonds_Payable),
      lease_liab: yuanToYiYuan(r.Lease_Liabilities),
      total_current_liab: yuanToYiYuan(r.Total_Current_Liabilities),
      total_noncurrent_liab: yuanToYiYuan(r.Total_NonCurrent_Liabilities),
      total_liabilities: yuanToYiYuan(r.Total_Liabilities),
      total_parent_equity: yuanToYiYuan(r.Equity_Parent ?? r.Common_Equity),
      minority_equity: yuanToYiYuan(r.Minority_Interest),
      total_equity: yuanToYiYuan(r.Total_Equity),
    };

    // Income statement (亿元) - use common keys from old wide rows
    const isr = {
      report_date,
      symbol,
      security_name: null,
      report_date_name: null,
      total_operate_income: yuanToYiYuan(r.Revenue),
      operate_income: yuanToYiYuan(r.Revenue),
      operate_cost: yuanToYiYuan(r.COGS),
      operate_profit: yuanToYiYuan(r.Operating_Income),
      total_profit: yuanToYiYuan(r.Pretax_Income),
      income_tax: yuanToYiYuan(r.Income_Tax_Exp),
      netprofit: yuanToYiYuan(r.Net_Income),
      parent_netprofit: yuanToYiYuan(r.Net_Income_Parent ?? r.Net_Income),
      basic_eps: toNum(r.EPS ?? r.Diluted_EPS),
      sale_expense: yuanToYiYuan(r.Selling_Exp),
      manage_expense: yuanToYiYuan(r.Admin_Exp),
      research_expense: yuanToYiYuan(r.RD_Exp),
      finance_expense: yuanToYiYuan(r.Fin_Exp),
    };

    // Cash flow (亿元)
    const cf = {
      report_date,
      symbol,
      security_name: null,
      report_date_name: null,
      netcash_operate: yuanToYiYuan(r.OCF),
      netcash_invest: yuanToYiYuan(r.ICF),
      netcash_finance: yuanToYiYuan(r.CFF),
      construct_long_asset: yuanToYiYuan(r.CapEx),
      cce_add: yuanToYiYuan(r.Net_Change_In_Cash),
      begin_cce: yuanToYiYuan(r.Beginning_Cash),
      end_cce: yuanToYiYuan(r.Ending_Cash),
      assign_dividend_porfit: yuanToYiYuan(r.Dividends_Paid ?? r.Common_Dividends_Paid),
    };

    // Only push if we have at least one meaningful field to avoid polluting with null-only rows
    if (bs.total_assets !== null || bs.total_liabilities !== null || bs.total_equity !== null) bsUpserts.push(bs);
    if (isr.operate_income !== null || isr.parent_netprofit !== null) isUpserts.push(isr);
    if (cf.netcash_operate !== null || cf.construct_long_asset !== null) cfUpserts.push(cf);
  }

  console.log(`Upserting cn_balance_sheet_10y rows: ${bsUpserts.length}`);
  await batchUpsert(supabase, "cn_balance_sheet_10y", bsUpserts, "symbol,report_date");

  console.log(`Upserting cn_income_statement_10y rows: ${isUpserts.length}`);
  await batchUpsert(supabase, "cn_income_statement_10y", isUpserts, "symbol,report_date");

  console.log(`Upserting cn_cash_flow_10y rows: ${cfUpserts.length}`);
  await batchUpsert(supabase, "cn_cash_flow_10y", cfUpserts, "symbol,report_date");

  // Backfill cn_mkt_cap_10y from stock_valuation_history (already in billion CNY)
  console.log("Fetching stock_valuation_history for market cap backfill...");
  const mktRows = [];
  const pageSize = 1000;
  let from = 0;
  while (true) {
    const { data, error } = await supabase
      .from("stock_valuation_history")
      .select("symbol,date,Market_cap,unit")
      .eq("symbol", symbol)
      .range(from, from + pageSize - 1);
    if (error) throw new Error(`stock_valuation_history fetch failed: ${error.message}`);
    if (!data || data.length === 0) break;
    mktRows.push(...data);
    if (data.length < pageSize) break;
    from += pageSize;
  }

  const mcUpserts = (mktRows || [])
    .map((r) => {
      const trade_date = r?.date ? String(r.date).slice(0, 10) : null;
      const cap = toNum(r?.Market_cap);
      if (!trade_date || cap === null) return null;
      return { symbol, trade_date, mkt_cap_billion_cny: cap };
    })
    .filter(Boolean);

  // Deduplicate by (symbol, trade_date) to avoid "cannot affect row a second time"
  const mcByKey = new Map();
  for (const r of mcUpserts) {
    mcByKey.set(`${r.symbol}|${r.trade_date}`, r);
  }
  const mcUpsertsDedup = Array.from(mcByKey.values()).sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)));

  console.log(`Upserting cn_mkt_cap_10y rows: ${mcUpsertsDedup.length} (deduped from ${mcUpserts.length})`);
  await batchUpsert(supabase, "cn_mkt_cap_10y", mcUpsertsDedup, "symbol,trade_date");

  console.log("Done.");
}

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

