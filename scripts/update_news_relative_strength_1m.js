/**
 * 计算并写回 cn_company_news.mkt_cap_change_1_month
 * - 使用 outputs/{symbol}_mkt_cap_10y.csv（date,mkt_cap_billion_cny）
 * - 使用 outputs/000001_sse_index_daily.csv（date,close）
 * - 结果写入“相对强度 = 股票1M市值变化 - 上证指数1M变化”
 *
 * 运行：
 *   node scripts/update_news_relative_strength_1m.js --symbol 000333
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('❌ 缺少 SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

function parseArgs(argv) {
  const args = argv.slice(2);
  const out = { symbol: null, limit: 500 };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--symbol') out.symbol = args[i + 1] || null;
    if (a.startsWith('--symbol=')) out.symbol = a.split('=', 2)[1] || null;
    if (a === '--limit') out.limit = Number(args[i + 1]) || out.limit;
    if (a.startsWith('--limit=')) out.limit = Number(a.split('=', 2)[1]) || out.limit;
  }
  if (out.symbol) out.symbol = String(out.symbol).trim().toUpperCase();
  return out;
}

function parseCsvMarketCap(filePath) {
  const text = fs.readFileSync(filePath, 'utf-8');
  const lines = text.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const header = lines[0].split(',').map(s => s.replace(/^\uFEFF/, '').trim());
  const idxDate = header.indexOf('date');
  const idxCap = header.indexOf('mkt_cap_billion_cny');
  if (idxDate === -1 || idxCap === -1) throw new Error(`CSV 表头不符合预期: ${header.join(',')}`);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',');
    const d = parts[idxDate]?.replace(/^\uFEFF/, '').trim();
    const c = parts[idxCap]?.trim();
    if (!d || !c) continue;
    const dt = new Date(d + 'T00:00:00+08:00');
    const cap = Number(c);
    if (Number.isFinite(dt.getTime()) && Number.isFinite(cap)) {
      rows.push({ date: d, ts: dt.getTime(), val: cap });
    }
  }
  rows.sort((a, b) => a.ts - b.ts);
  return rows;
}

function parseCsvIndex(filePath) {
  const text = fs.readFileSync(filePath, 'utf-8');
  const lines = text.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const header = lines[0].split(',').map(s => s.replace(/^\uFEFF/, '').trim());
  const idxDate = header.indexOf('date');
  const idxClose = header.indexOf('close');
  if (idxDate === -1 || idxClose === -1) throw new Error(`CSV 表头不符合预期: ${header.join(',')}`);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',');
    const d = parts[idxDate]?.replace(/^\uFEFF/, '').trim();
    const c = parts[idxClose]?.trim();
    if (!d || !c) continue;
    const dt = new Date(d + 'T00:00:00+08:00');
    const close = Number(c);
    if (Number.isFinite(dt.getTime()) && Number.isFinite(close)) {
      rows.push({ date: d, ts: dt.getTime(), val: close });
    }
  }
  rows.sort((a, b) => a.ts - b.ts);
  return rows;
}

function addOneMonthCN(pubDateCN) {
  const [y, m, d] = pubDateCN.split('-').map(Number);
  let y2 = y, m2 = m + 1;
  if (m2 === 13) { y2 += 1; m2 = 1; }
  const lastDay = new Date(Date.UTC(y2, m2, 0)).getUTCDate();
  const d2 = Math.min(d, lastDay);
  return `${y2.toString().padStart(4, '0')}-${m2.toString().padStart(2, '0')}-${d2.toString().padStart(2, '0')}`;
}

function toCNDateString(publishedAt) {
  const dt = new Date(publishedAt);
  if (!Number.isFinite(dt.getTime())) return null;
  const cn = new Date(dt.getTime() + 8 * 3600 * 1000);
  const y = cn.getUTCFullYear();
  const m = String(cn.getUTCMonth() + 1).padStart(2, '0');
  const d = String(cn.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function tsCN(dateStr) {
  return new Date(dateStr + 'T00:00:00+08:00').getTime();
}

function nearestOnOrBefore(series, targetTs) {
  let lo = 0, hi = series.length - 1;
  let ans = null;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (series[mid].ts <= targetTs) {
      ans = series[mid];
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

function computeChange1M(series, publishedAt) {
  const pubCN = toCNDateString(publishedAt);
  if (!pubCN) return { value: null, why: 'bad published_at' };
  const targetCN = addOneMonthCN(pubCN);
  const targetTs = tsCN(targetCN);
  const maxTs = series[series.length - 1]?.ts;
  if (!maxTs || maxTs < targetTs) {
    return { value: null, why: 'less than 1 month data' };
  }
  const v0 = nearestOnOrBefore(series, tsCN(pubCN));
  const v1 = nearestOnOrBefore(series, targetTs);
  if (!v0 || !v1) return { value: null, why: 'no match' };
  const chgPct = ((v1.val - v0.val) / v0.val) * 100;
  if (!Number.isFinite(chgPct)) return { value: null, why: 'bad pct' };
  return { value: chgPct, why: 'ok', pubMatch: v0.date, afterMatch: v1.date, pubCN, targetCN };
}

async function listSymbolsToProcess(filterSymbol) {
  if (filterSymbol) return [filterSymbol];
  const { data, error } = await supabase
    .from('cn_company_news')
    .select('symbol')
    .not('symbol', 'is', null);
  if (error) throw new Error(`读取 symbol 列表失败: ${error.message}`);
  const set = new Set();
  (data || []).forEach(r => set.add(String(r.symbol).toUpperCase()));
  return Array.from(set);
}

async function main() {
  const args = parseArgs(process.argv);
  const symbols = await listSymbolsToProcess(args.symbol);

  const indexPath = path.join(__dirname, '../outputs/000001_sse_index_daily.csv');
  if (!fs.existsSync(indexPath)) {
    console.error('❌ 缺少上证指数 CSV: outputs/000001_sse_index_daily.csv');
    process.exit(1);
  }
  const indexSeries = parseCsvIndex(indexPath);
  if (indexSeries.length < 10) {
    console.error('❌ 上证指数 CSV 数据不足');
    process.exit(1);
  }

  let updated = 0;
  let naCount = 0;

  for (const symbol of symbols) {
    const csvPath = path.join(__dirname, `../outputs/${symbol}_mkt_cap_10y.csv`);
    if (!fs.existsSync(csvPath)) continue;
    const stockSeries = parseCsvMarketCap(csvPath);
    if (stockSeries.length < 10) continue;

    const { data: news, error } = await supabase
      .from('cn_company_news')
      .select('id,symbol,published_at,news_title,mkt_cap_change_1_month')
      .eq('symbol', symbol)
      .is('mkt_cap_change_1_month', null)
      .order('published_at', { ascending: true })
      .limit(args.limit);

    if (error) throw new Error(`读取新闻失败 symbol=${symbol}: ${error.message}`);
    if (!news || !news.length) continue;

    for (const row of news) {
      const stock = computeChange1M(stockSeries, row.published_at);
      const index = computeChange1M(indexSeries, row.published_at);
      let value = 'n/a';
      if (stock.value !== null && index.value !== null) {
        const rs = stock.value - index.value;
        value = `${rs.toFixed(2)}%`;
      }
      const { error: upErr } = await supabase
        .from('cn_company_news')
        .update({ mkt_cap_change_1_month: value })
        .eq('id', row.id);
      if (upErr) throw new Error(`更新失败 id=${row.id}: ${upErr.message}`);
      updated += 1;
      if (value === 'n/a') naCount += 1;
      console.log(`✅ id=${row.id} ${symbol} -> ${value} (stock:${stock.why}, index:${index.why})`);
    }
  }

  console.log(`\n📊 完成：更新 ${updated} 条，其中 n/a = ${naCount} 条`);
}

main().catch((e) => {
  console.error('❌ 脚本异常:', e);
  process.exit(1);
});
