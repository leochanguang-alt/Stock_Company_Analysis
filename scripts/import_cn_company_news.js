/**
 * 导入公司新闻数据到 Supabase: cn_company_news
 * 数据来源: outputs/002508_stock_news_em_6m.csv
 *
 * CSV 列: 关键词,新闻标题,新闻内容,发布时间,文章来源,新闻链接
 * 表字段: symbol, news_title, news_content, published_at, source, news_url
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('❌ 请在 .env 文件中设置 SUPABASE_URL 和 SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const columnMapping = {
  '关键词': 'symbol',
  '新闻标题': 'news_title',
  '新闻内容': 'news_content',
  '发布时间': 'published_at',
  '文章来源': 'source',
  '新闻链接': 'news_url',
};

function normalizePublishedAt(val) {
  const s = (val || '').trim();
  if (!s) return null;
  // 输入示例: 2026-01-20 15:45:55
  // 加上中国时区，避免 timestamptz 解析为 UTC 导致偏移
  if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(s)) {
    return s.replace(' ', 'T') + '+08:00';
  }
  return s; // 兜底：交给 Postgres 解析
}

function splitCSVLine(line) {
  // 本数据集不含英文逗号的引用字段，直接 split 即可。
  // 如未来出现英文逗号字段，再升级为更健壮的 CSV parser。
  return line.split(',');
}

function parseCSV(csvContent) {
  const lines = csvContent.trim().split('\n').filter(Boolean);
  if (lines.length <= 1) return [];

  const headers = lines[0].split(',').map(h => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const values = splitCSVLine(line);
    const row = {};

    headers.forEach((header, idx) => {
      const dbField = columnMapping[header];
      if (!dbField) return;
      let value = values[idx] !== undefined ? String(values[idx]).trim() : '';

      if (dbField === 'published_at') value = normalizePublishedAt(value);
      if (dbField === 'symbol') value = value || null;
      if (dbField === 'news_url') value = value || null;
      if (dbField === 'news_title') value = value || null;
      if (dbField === 'news_content') value = value || null;
      if (dbField === 'source') value = value || null;

      row[dbField] = value;
    });

    // 必要字段校验
    if (row.symbol && row.news_url && row.news_title) rows.push(row);
  }

  return rows;
}

async function importData() {
  const csvPath = path.join(__dirname, '../outputs/002508_stock_news_em_6m.csv');

  console.log('📖 读取 CSV 文件...');
  const csvContent = fs.readFileSync(csvPath, 'utf-8');
  const rows = parseCSV(csvContent);

  console.log(`📊 解析到 ${rows.length} 条记录`);
  if (!rows.length) return;

  console.log('⬆️ 导入数据到 Supabase...');

  // 分批导入
  const batchSize = 50;
  let totalInserted = 0;

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const { error } = await supabase
      .from('cn_company_news')
      .insert(batch);

    if (error) {
      console.error(`❌ 批次 ${Math.floor(i / batchSize) + 1} 导入失败:`, error.message);
      console.error('详细错误:', error);
    } else {
      totalInserted += batch.length;
      process.stdout.write(`\r  已导入: ${totalInserted}/${rows.length}`);
    }
  }

  console.log(`\n✅ 导入完成（尝试插入 ${totalInserted} 条；重复 news_url 会被触发器 upsert）`);

  // 验证：取最新 5 条
  const { data: verifyData, error: verifyError } = await supabase
    .from('cn_company_news')
    .select('symbol, news_title, published_at, source, news_url')
    .eq('symbol', '002508')
    .order('published_at', { ascending: false })
    .limit(5);

  if (verifyError) {
    console.error('验证失败:', verifyError.message);
  } else {
    console.log('\n📋 最新 5 条新闻:');
    verifyData.forEach(r => {
      console.log(`  ${r.published_at || '-'} | ${r.source || '-'} | ${String(r.news_title).slice(0, 30)}...`);
    });
  }
}

importData().catch((e) => {
  console.error('❌ 未处理异常:', e);
  process.exit(1);
});

