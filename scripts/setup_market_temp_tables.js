/**
 * Create temp tables for market data import.
 */
require('dotenv').config();

const { Client } = require('pg');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_DB_PASSWORD = process.env.SUPABASE_DB_PASSWORD;

if (!SUPABASE_URL || !SUPABASE_DB_PASSWORD) {
  console.error('缺少 SUPABASE_URL 或 SUPABASE_DB_PASSWORD');
  process.exit(1);
}

const projectRef = SUPABASE_URL.replace('https://', '').split('.')[0];

const BASE_TABLES = ['us_market', 'hkse_market', 'share_a_market'];

function buildTempSql(baseTable) {
  const tempTable = `${baseTable}_temp`;
  return `
CREATE TABLE IF NOT EXISTS public.${tempTable} (LIKE public.${baseTable} INCLUDING ALL);

ALTER TABLE public.${tempTable} ADD COLUMN IF NOT EXISTS total_liabilities_quarterly DOUBLE PRECISION;
ALTER TABLE public.${tempTable} ADD COLUMN IF NOT EXISTS total_liabilities_quarterly_currency TEXT;
ALTER TABLE public.${tempTable} ADD COLUMN IF NOT EXISTS analyst_rating TEXT;
ALTER TABLE public.${tempTable} ADD COLUMN IF NOT EXISTS download_date DATE;

ALTER TABLE public.${tempTable} ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow public read" ON public.${tempTable};
DROP POLICY IF EXISTS "Allow service insert" ON public.${tempTable};
DROP POLICY IF EXISTS "Allow service update" ON public.${tempTable};
DROP POLICY IF EXISTS "Allow service delete" ON public.${tempTable};

CREATE POLICY "Allow public read" ON public.${tempTable} FOR SELECT USING (true);
CREATE POLICY "Allow service insert" ON public.${tempTable} FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow service update" ON public.${tempTable} FOR UPDATE USING (true);
CREATE POLICY "Allow service delete" ON public.${tempTable} FOR DELETE USING (true);
`;
}

async function main() {
  const connectionString = `postgresql://postgres.${projectRef}:${SUPABASE_DB_PASSWORD}@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres`;
  const client = new Client({
    connectionString,
    ssl: { rejectUnauthorized: false }
  });

  try {
    console.log('连接数据库...');
    await client.connect();
    console.log('连接成功');

    for (const baseTable of BASE_TABLES) {
      console.log(`创建 ${baseTable}_temp...`);
      await client.query(buildTempSql(baseTable));
      console.log(`完成 ${baseTable}_temp`);
    }

    console.log('全部 temp 表创建完成');
  } catch (error) {
    console.error('错误:', error.message);
    process.exitCode = 1;
  } finally {
    await client.end();
  }
}

main();
