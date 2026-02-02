/**
 * 通过 Supabase REST API 创建 market_index_history 表
 */

require('dotenv').config();

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error('缺少环境变量');
    process.exit(1);
}

// 从 URL 提取项目 ref
const projectRef = SUPABASE_URL.replace('https://', '').split('.')[0];

async function executeSql(sql) {
    const response = await fetch(`${SUPABASE_URL}/rest/v1/rpc/exec_sql`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'apikey': SUPABASE_SERVICE_ROLE_KEY,
            'Authorization': `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`
        },
        body: JSON.stringify({ query: sql })
    });
    
    if (!response.ok) {
        const text = await response.text();
        throw new Error(`SQL 执行失败: ${response.status} - ${text}`);
    }
    return response.json();
}

// 使用 pg 直连 - Supabase pooler
const { Client } = require('pg');

async function main() {
    // Supabase 数据库连接
    const connectionString = `postgresql://postgres.${projectRef}:${process.env.SUPABASE_DB_PASSWORD}@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres`;
    
    const client = new Client({
        connectionString,
        ssl: { rejectUnauthorized: false }
    });

    try {
        console.log('连接数据库...');
        await client.connect();
        console.log('连接成功');

        const sql = `
-- 创建 market_index_history 表
CREATE TABLE IF NOT EXISTS stock_analysis.market_index_history (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    market TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(symbol, market, date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_market_index_history_symbol ON stock_analysis.market_index_history(symbol);
CREATE INDEX IF NOT EXISTS idx_market_index_history_market ON stock_analysis.market_index_history(market);
CREATE INDEX IF NOT EXISTS idx_market_index_history_date ON stock_analysis.market_index_history(date);
CREATE INDEX IF NOT EXISTS idx_market_index_history_symbol_date ON stock_analysis.market_index_history(symbol, date);
        `;

        console.log('创建表...');
        await client.query(sql);
        console.log('表创建成功');

        // 创建 view
        const viewSql = `
-- 删除旧的 view 和 trigger
DROP TRIGGER IF EXISTS market_index_history_insert_trigger ON public.market_index_history;
DROP FUNCTION IF EXISTS public.market_index_history_insert();
DROP VIEW IF EXISTS public.market_index_history;

-- 创建 public view
CREATE OR REPLACE VIEW public.market_index_history AS
SELECT * FROM stock_analysis.market_index_history;
        `;
        
        console.log('创建 view...');
        await client.query(viewSql);
        console.log('View 创建成功');

        // 创建触发器函数
        const triggerFnSql = `
CREATE OR REPLACE FUNCTION public.market_index_history_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stock_analysis.market_index_history (symbol, market, date, open, high, low, close, volume)
    VALUES (NEW.symbol, NEW.market, NEW.date, NEW.open, NEW.high, NEW.low, NEW.close, NEW.volume)
    ON CONFLICT (symbol, market, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
        `;
        
        console.log('创建触发器函数...');
        await client.query(triggerFnSql);
        console.log('触发器函数创建成功');

        // 创建触发器
        const triggerSql = `
CREATE TRIGGER market_index_history_insert_trigger
    INSTEAD OF INSERT ON public.market_index_history
    FOR EACH ROW
    EXECUTE FUNCTION public.market_index_history_insert();
        `;
        
        console.log('创建触发器...');
        await client.query(triggerSql);
        console.log('触发器创建成功');

        // 启用 RLS
        const rlsSql = `
ALTER TABLE stock_analysis.market_index_history ENABLE ROW LEVEL SECURITY;

-- 删除旧策略
DROP POLICY IF EXISTS "Allow anon read market_index_history" ON stock_analysis.market_index_history;
DROP POLICY IF EXISTS "Allow service_role full access market_index_history" ON stock_analysis.market_index_history;

-- 创建 RLS 策略
CREATE POLICY "Allow anon read market_index_history"
    ON stock_analysis.market_index_history
    FOR SELECT
    TO anon
    USING (true);

CREATE POLICY "Allow service_role full access market_index_history"
    ON stock_analysis.market_index_history
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);
        `;
        
        console.log('配置 RLS...');
        await client.query(rlsSql);
        console.log('RLS 配置成功');

        console.log('');
        console.log('=== 表创建完成 ===');

    } catch (error) {
        console.error('错误:', error.message);
    } finally {
        await client.end();
    }
}

main();
