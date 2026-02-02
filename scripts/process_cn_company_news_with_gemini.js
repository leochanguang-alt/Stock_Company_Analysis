/**
 * 使用 Gemini-3 对 cn_company_news 做清洗与评分：
 * 1) 删除“行情播报/盘中播报/突破年线/跨越牛熊分界线”等行情信息类新闻
 * 2) 评分并写入 grade（-10~10）与 reason（一句话）
 * 3) 去重：如与历史新闻重复（标题相同且内容高度一致/相同），删除重复记录（保留更早 id）
 *
 * 依赖：
 * - .env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, GEMINI_API_KEY, GEMINI_MODEL, GEMINI_BASE_URL
 *
 * 运行：
 *   node scripts/process_cn_company_news_with_gemini.js
 *   node scripts/process_cn_company_news_with_gemini.js --dedupe-only --symbol 002508
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-3-pro-preview';
const GEMINI_BASE_URL = (process.env.GEMINI_BASE_URL || 'https://generativelanguage.googleapis.com').replace(/\/+$/, '');

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('❌ 缺少 SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}
if (!GEMINI_API_KEY) {
  console.error('❌ 缺少 GEMINI_API_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const CONTENT_SIM_THRESHOLD = 0.92; // news_content 相似度阈值：越大越严格
const CONTENT_BG_MAX_CHARS = 1800;  // 只取前 N 字符做 bigram，避免过重
const EVENT_SIM_THRESHOLD = 0.45;   // 事件签名相似度（更适合“同一公告多渠道转发”）
const EVENT_NEAR_DAYS = 1;          // 仅对“快讯/电报类”在较短时间窗内去重，避免误删深度稿/解读稿

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function normalizeText(s) {
  return String(s ?? '')
    .replace(/\s+/g, ' ')
    .replace(/[【】\[\]（）()]/g, '')
    .trim()
    .toLowerCase();
}

function bigrams(s) {
  const t = normalizeText(s).replace(/\s+/g, '');
  const set = new Set();
  for (let i = 0; i < t.length - 1; i++) set.add(t.slice(i, i + 2));
  return set;
}

function contentBigrams(s) {
  const t = String(s ?? '').slice(0, CONTENT_BG_MAX_CHARS);
  return bigrams(t);
}

function jaccard(a, b) {
  if (!a.size && !b.size) return 1;
  let inter = 0;
  for (const x of a) if (b.has(x)) inter += 1;
  const union = a.size + b.size - inter;
  return union ? inter / union : 0;
}

function hashText(s) {
  return crypto.createHash('sha1').update(String(s ?? ''), 'utf8').digest('hex');
}

function parseArgs(argv) {
  const args = argv.slice(2);
  const out = { dedupeOnly: false, symbol: null };
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--dedupe-only') out.dedupeOnly = true;
    if (a === '--symbol') out.symbol = args[i + 1] || null;
    if (a.startsWith('--symbol=')) out.symbol = a.split('=', 2)[1] || null;
  }
  if (out.symbol) out.symbol = String(out.symbol).trim().toUpperCase();
  return out;
}

function getDedupeText(row) {
  const c = String(row?.news_content ?? '');
  // content 过短时，用 title 作为补充（否则无法抓到关键实体/金额）
  if (c.trim().length >= 120) return c;
  return `${row?.news_title || ''} ${c}`.trim();
}

function isFlashLike(row) {
  const source = String(row?.source || '');
  const title = String(row?.news_title || '');
  const content = String(row?.news_content || '');
  const s = `${source} ${title} ${content}`.toLowerCase();

  // 明显“电报/快讯/播报/财讯”类
  const patterns = [
    /ai快讯/,
    /盘中播报/,
    /财讯/,
    /电/,
    /公告称/,
    /发布公告/,
  ];
  if (patterns.some(re => re.test(s))) return true;

  // 来源白名单：偏“快讯/资讯流”
  const sourceHints = [
    '人民财讯',
    '每日经济新闻',
    '中国证券报',
    '中证网',
    '财中社',
    '证券时报网',
  ];
  if (sourceHints.some(k => source.includes(k))) return true;

  return false;
}

function extractMoneyTerms(text) {
  const s = String(text || '');
  const out = new Set();
  const re = /(\d+(?:\.\d+)?)\s*(亿|亿元|万|万元)\s*(?:人民币|元)?/g;
  let m;
  while ((m = re.exec(s)) !== null) {
    out.add(`${m[1]}${m[2]}`); // 例如：1亿元
  }
  // 兜底：识别“1亿”这类写法
  const re2 = /(\d+(?:\.\d+)?)\s*亿/g;
  while ((m = re2.exec(s)) !== null) out.add(`${m[1]}亿`);
  return out;
}

function extractEventSignature(text) {
  const s = String(text || '');
  const terms = new Set();

  // 关键实体（按需扩展）
  const entities = [
    '老板电器',
    '优特智厨',
    'UTCOOK',
    'UTcook',
    'JIN XIAO',
    '珠海优特智厨',
    '投资合作意向书',
    '意向书',
    '战略投资',
    '增资',
    '投资',
    '炒菜机器人',
    '智能餐饮',
  ];

  for (const e of entities) {
    if (s.toLowerCase().includes(e.toLowerCase())) terms.add(e);
  }

  // 金额
  for (const t of extractMoneyTerms(s)) terms.add(t);

  // 证券代码（如 002508）
  const code = s.match(/\b\d{6}\b/);
  if (code) terms.add(code[0]);

  return terms;
}

function setJaccard(a, b) {
  if (!a.size && !b.size) return 1;
  let inter = 0;
  for (const x of a) if (b.has(x)) inter += 1;
  const union = a.size + b.size - inter;
  return union ? inter / union : 0;
}

function daysBetween(a, b) {
  const da = a ? new Date(a) : null;
  const db = b ? new Date(b) : null;
  if (!da || !db || isNaN(da.getTime()) || isNaN(db.getTime())) return null;
  const diff = Math.abs(da.getTime() - db.getTime());
  return diff / (24 * 3600 * 1000);
}

function hasMoneyTerm(sig) {
  for (const t of sig) {
    if (/[亿万]/.test(t) && /\d/.test(t)) return true;
  }
  return false;
}

function hasAny(sig, terms) {
  for (const t of terms) if (sig.has(t)) return true;
  return false;
}

function intersection(a, b) {
  const out = new Set();
  for (const x of a) if (b.has(x)) out.add(x);
  return out;
}

function isEventDuplicate(aSig, bSig, aDate, bDate, aRow, bRow) {
  if (!aSig.size || !bSig.size) return false;
  const inter = intersection(aSig, bSig);
  const j = setJaccard(aSig, bSig);

  const nearDays = daysBetween(aDate, bDate);
  const isNear = nearDays !== null && nearDays <= EVENT_NEAR_DAYS;
  const flashA = isFlashLike(aRow);
  const flashB = isFlashLike(bRow);
  // 事件级去重仅用于“快讯/电报”两边都属于资讯流的情况
  if (!(flashA && flashB)) return false;
  if (!isNear) return false;

  const counterparty = hasAny(inter, new Set(['优特智厨', 'UTCOOK', 'UTcook', '珠海优特智厨']));
  const action = hasAny(inter, new Set(['增资', '投资合作意向书', '意向书', '战略投资', '投资']));
  const theme = hasAny(inter, new Set(['炒菜机器人', '智能餐饮']));
  const code = Array.from(inter).some(t => /^\d{6}$/.test(t));
  const money = hasMoneyTerm(inter) || (hasMoneyTerm(aSig) && hasMoneyTerm(bSig));

  // 强条件：同代码 + 同对手方 + 同动作 + 金额（或签名相似度足够）
  if (code && counterparty && action && (money || j >= EVENT_SIM_THRESHOLD)) return true;

  // 弱条件（仍要求双方为快讯）：可能缺金额，要求“近 N 天 + 对手方 + 动作 + 主题”
  if (counterparty && action && theme) return true;

  return false;
}

function isMarketInfoNews(title, content) {
  const t = `${title || ''} ${content || ''}`.trim();
  if (!t) return false;

  // 典型行情播报/技术面快讯（可按需扩展）
  const patterns = [
    /盘中播报/,
    /今日\d+只个股/,
    /突破年线/,
    /跨越牛熊分界线/,
    /创(历史)?新高/,
    /涨停/,
    /跌停/,
    /资金流向/,
    /龙虎榜/,
    /换手率/,
    /技术面/,
    /均线/,
    /K线/,
    /分时/,
    /收盘|开盘/,
    /主力/,
    /大单/,
    /北向资金/,
  ];

  return patterns.some(re => re.test(t));
}

function buildPrompt(row) {
  const title = row.news_title || '';
  const content = row.news_content || '';
  const symbol = row.symbol || '';

  return `你是一个专业的金融新闻分析师。请分析以下新闻：

### 新闻内容：
股票代码：${symbol}
标题：${title}
内容：${content}

### 任务要求：
1. 重点是对股价的潜在影响，如果对股价正面影响巨大，评分10，如果对股价负面影响巨大，评分-10，对股价影响中性，评分为0
2. 提供一句话的评分理由。

### 输出格式：
请严格仅返回 JSON 格式，不要包含 Markdown 代码块（\`\`\`json），不要有任何前导或后缀文字。

### 示例格式：
{
  "stock_name": "英伟达/NVIDIA",
  "grade": 8.5,
  "reason": "发布了超预期的新一代 AI 芯片，预计将显著提升下一季度营收。"
}`;
}

function extractJsonObject(text) {
  const s = String(text || '').trim();
  if (!s) return null;
  // 允许模型偶尔带前后空白；抓取第一个 {...} 段
  const first = s.indexOf('{');
  const last = s.lastIndexOf('}');
  if (first === -1 || last === -1 || last <= first) return null;
  const candidate = s.slice(first, last + 1);
  try {
    return JSON.parse(candidate);
  } catch (e) {
    return null;
  }
}

async function geminiScore(row) {
  const url = `${GEMINI_BASE_URL}/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
  const payload = {
    contents: [{ role: 'user', parts: [{ text: buildPrompt(row) }] }],
  };

  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  const bodyText = await resp.text();
  if (!resp.ok) {
    throw new Error(`Gemini API HTTP ${resp.status}: ${bodyText.slice(0, 400)}`);
  }

  let j;
  try {
    j = JSON.parse(bodyText);
  } catch (e) {
    throw new Error(`Gemini 返回非 JSON: ${bodyText.slice(0, 300)}`);
  }

  const text = j?.candidates?.[0]?.content?.parts?.[0]?.text;
  const out = extractJsonObject(text);
  if (!out) {
    throw new Error(`无法解析模型输出 JSON: ${String(text || '').slice(0, 300)}`);
  }

  const grade = Number(out.grade);
  const reason = typeof out.reason === 'string' ? out.reason.trim() : '';

  if (!Number.isFinite(grade)) throw new Error(`grade 非数值: ${JSON.stringify(out).slice(0, 200)}`);
  if (grade < -10 || grade > 10) throw new Error(`grade 超范围(-10~10): ${grade}`);
  if (!reason) throw new Error(`reason 为空: ${JSON.stringify(out).slice(0, 200)}`);

  return { grade, reason };
}

async function deleteById(id, why) {
  const { error } = await supabase.from('cn_company_news').delete().eq('id', id);
  if (error) throw new Error(`删除失败 id=${id}: ${error.message}`);
  console.log(`🗑️ 删除 id=${id} (${why})`);
}

async function updateScore(id, grade, reason) {
  const { error } = await supabase
    .from('cn_company_news')
    .update({ grade, reason })
    .eq('id', id);
  if (error) throw new Error(`更新失败 id=${id}: ${error.message}`);
  console.log(`✅ 更新 id=${id} grade=${grade}`);
}

async function isDuplicate(row) {
  // 使用 news_content 做重复判断（优先），而不是 title/url
  const symbol = row.symbol;
  if (!symbol) return false;

  // 找同 symbol 的更早记录（最多 80 条）
  const { data, error } = await supabase
    .from('cn_company_news')
    .select('id,news_title,news_content,published_at,news_url')
    .eq('symbol', symbol)
    .lt('id', row.id)
    .order('published_at', { ascending: false })
    .limit(80);

  if (error) {
    console.warn('去重查询失败（忽略）:', error.message);
    return false;
  }
  if (!data || !data.length) return false;

  const curText = getDedupeText(row);
  const curContentNorm = normalizeText(curText);
  if (!curContentNorm) return false;
  const curBg = contentBigrams(curText);
  const curSig = hashText(curContentNorm.slice(0, 800));
  const curEvent = extractEventSignature(curText);

  for (const prev of data) {
    const prevText = getDedupeText(prev);
    const prevNorm = normalizeText(prevText);
    if (!prevNorm) continue;
    const prevSig = hashText(prevNorm.slice(0, 800));
    if (prevSig === curSig) return true;
    const sim = jaccard(curBg, contentBigrams(prevText));
    if (sim >= CONTENT_SIM_THRESHOLD) return true;

    // 同一事件多渠道转发：用“事件签名”相似度判断（更稳）
    const prevEvent = extractEventSignature(prevText);
    if (isEventDuplicate(curEvent, prevEvent, row.published_at, prev.published_at, row, prev)) return true;
  }
  return false;
}

async function dedupeAllForSymbol(symbol) {
  const { data, error } = await supabase
    .from('cn_company_news')
    .select('id,symbol,news_title,news_content,published_at,news_url')
    .eq('symbol', symbol)
    .order('published_at', { ascending: true })
    .order('id', { ascending: true })
    .limit(1000);

  if (error) throw new Error(`去重读取失败 symbol=${symbol}: ${error.message}`);
  const rows = data || [];
  if (rows.length <= 1) return 0;

  let deleted = 0;
  const kept = [];

  for (const r of rows) {
    const text = getDedupeText(r);
    const contentNorm = normalizeText(text);
    const contentSig = hashText(contentNorm.slice(0, 800));
    const contentBg = contentBigrams(text);
    const eventSig = extractEventSignature(text);

    let isDup = false;
    let dupOfId = null;
    let bestSim = 0;
    for (const k of kept) {
      if (contentSig === k._contentSig) {
        isDup = true;
        dupOfId = k.id;
        break;
      }
      const sim = jaccard(contentBg, k._contentBg);
      if (sim > bestSim) bestSim = sim;
      if (sim >= CONTENT_SIM_THRESHOLD) {
        isDup = true;
        dupOfId = k.id;
        break;
      }

      if (isEventDuplicate(eventSig, k._eventSig, r.published_at, k._published_at, r, k._row)) {
        isDup = true;
        dupOfId = k.id;
        bestSim = Math.max(bestSim, setJaccard(eventSig, k._eventSig));
        break;
      }
    }

    if (isDup) {
      await deleteById(r.id, `去重（基于 news_content/事件签名${dupOfId ? `，保留 id=${dupOfId}` : ''}${bestSim ? `，sim≈${bestSim.toFixed(3)}` : ''})`);
      deleted += 1;
      continue;
    }

    kept.push({ id: r.id, _contentBg: contentBg, _contentSig: contentSig, _eventSig: eventSig, _published_at: r.published_at, _row: r });
  }

  return deleted;
}

async function listSymbols() {
  const { data, error } = await supabase
    .from('cn_company_news')
    .select('symbol')
    .not('symbol', 'is', null);
  if (error) throw new Error(`读取 symbol 列表失败: ${error.message}`);
  const set = new Set();
  (data || []).forEach(r => set.add(r.symbol));
  return Array.from(set);
}

async function fetchUnprocessedBatch(limit = 50) {
  // 仅处理尚未评分的记录（grade 为空）
  const { data, error } = await supabase
    .from('cn_company_news')
    .select('id,symbol,news_title,news_content,published_at,source,news_url,grade,reason')
    .is('grade', null)
    .order('published_at', { ascending: true })
    .limit(limit);

  if (error) throw new Error(`读取失败: ${error.message}`);
  return data || [];
}

async function main() {
  const args = parseArgs(process.argv);
  console.log(`🔎 使用模型 ${GEMINI_MODEL} 开始处理 cn_company_news...`);

  let processed = 0;
  let deletedMarket = 0;
  let deletedDup = 0;
  let scored = 0;

  if (!args.dedupeOnly) {
    while (true) {
      const rows = await fetchUnprocessedBatch(30);
      if (!rows.length) break;

      for (const row of rows) {
        processed += 1;

        // 1) 行情信息过滤
        if (isMarketInfoNews(row.news_title, row.news_content)) {
          await deleteById(row.id, '行情信息/技术面播报');
          deletedMarket += 1;
          continue;
        }

        // 2) 去重（与历史重复则删）——基于 news_content
        const dup = await isDuplicate(row);
        if (dup) {
          await deleteById(row.id, '重复新闻（news_content）');
          deletedDup += 1;
          continue;
        }

        // 3) Gemini 评分
        try {
          const { grade, reason } = await geminiScore(row);
          await updateScore(row.id, grade, reason);
          scored += 1;
        } catch (e) {
          console.warn(`⚠️ 评分失败 id=${row.id}: ${e.message}`);
          // 失败不删，保留待复跑
        }

        // 温和限速，避免触发配额/429
        await sleep(600);
      }
    }
  }

  // 4) 评分后再做一次全量去重（覆盖已评分记录）
  let deletedAfter = 0;
  try {
    const symbols = args.symbol ? [args.symbol] : await listSymbols();
    for (const s of symbols) {
      deletedAfter += await dedupeAllForSymbol(s);
    }
  } catch (e) {
    console.warn('⚠️ 去重阶段失败（忽略）:', e.message);
  }

  console.log('\n📊 完成');
  console.log(`- 处理记录数: ${processed}`);
  console.log(`- 删除（行情）: ${deletedMarket}`);
  console.log(`- 删除（重复）: ${deletedDup}`);
  console.log(`- 完成评分: ${scored}`);
  console.log(`- 追加去重删除: ${deletedAfter}`);
}

main().catch((e) => {
  console.error('❌ 脚本异常:', e);
  process.exit(1);
});

