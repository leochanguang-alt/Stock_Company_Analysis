-- 创建中国A股前十大股东数据表
-- 数据来源: outputs/002508_top10_shareholders_10y.csv

-- 确保 stock_analysis schema 存在
CREATE SCHEMA IF NOT EXISTS stock_analysis;

CREATE TABLE IF NOT EXISTS stock_analysis.cn_top10_sharehold (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,                    -- 股票代码 (如 SZ002508)
    report_date VARCHAR(8) NOT NULL,                -- 报告期 (如 20160331)
    rank INTEGER NOT NULL,                          -- 名次 (1-10)
    shareholder_name TEXT NOT NULL,                 -- 股东名称
    share_type VARCHAR(100),                        -- 股份类型 (如 流通A股)
    shares_held BIGINT,                             -- 持股数
    holding_ratio DECIMAL(10,4),                    -- 占总股本持股比例 (%)
    change_amount TEXT,                             -- 增减 (数字或"新进"/"不变")
    change_ratio DECIMAL(15,8),                     -- 变动比率 (%)
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- 唯一约束: 同一股票同一报告期同一名次只有一条记录
    UNIQUE(symbol, report_date, rank)
);

-- 创建索引以优化查询性能
CREATE INDEX IF NOT EXISTS idx_cn_top10_symbol ON stock_analysis.cn_top10_sharehold(symbol);
CREATE INDEX IF NOT EXISTS idx_cn_top10_date ON stock_analysis.cn_top10_sharehold(report_date);
CREATE INDEX IF NOT EXISTS idx_cn_top10_shareholder ON stock_analysis.cn_top10_sharehold(shareholder_name);

-- 添加表注释
COMMENT ON TABLE stock_analysis.cn_top10_sharehold IS '中国A股前十大股东数据';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.symbol IS '股票代码';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.report_date IS '报告期';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.rank IS '持股名次';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.shareholder_name IS '股东名称';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.share_type IS '股份类型';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.shares_held IS '持股数量';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.holding_ratio IS '占总股本比例(%)';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.change_amount IS '增减情况';
COMMENT ON COLUMN stock_analysis.cn_top10_sharehold.change_ratio IS '变动比率(%)';

-- 启用 RLS
ALTER TABLE stock_analysis.cn_top10_sharehold ENABLE ROW LEVEL SECURITY;

-- 授予权限
GRANT USAGE ON SCHEMA stock_analysis TO anon, authenticated, service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON stock_analysis.cn_top10_sharehold TO anon, authenticated, service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA stock_analysis TO anon, authenticated, service_role;

-- 创建 RLS 策略
CREATE POLICY "Allow public read access" ON stock_analysis.cn_top10_sharehold
    FOR SELECT USING (true);

CREATE POLICY "Allow authenticated insert" ON stock_analysis.cn_top10_sharehold
    FOR INSERT WITH CHECK (true);

CREATE POLICY "Allow authenticated update" ON stock_analysis.cn_top10_sharehold
    FOR UPDATE USING (true);

-- 在 public schema 创建视图以便通过 REST API 访问
CREATE OR REPLACE VIEW public.cn_top10_sharehold AS
SELECT * FROM stock_analysis.cn_top10_sharehold;

-- 创建 INSERT 触发器函数
CREATE OR REPLACE FUNCTION public.cn_top10_sharehold_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stock_analysis.cn_top10_sharehold (
        symbol, report_date, rank, shareholder_name, share_type,
        shares_held, holding_ratio, change_amount, change_ratio
    ) VALUES (
        NEW.symbol, NEW.report_date, NEW.rank, NEW.shareholder_name, NEW.share_type,
        NEW.shares_held, NEW.holding_ratio, NEW.change_amount, NEW.change_ratio
    )
    ON CONFLICT (symbol, report_date, rank) DO UPDATE SET
        shareholder_name = EXCLUDED.shareholder_name,
        share_type = EXCLUDED.share_type,
        shares_held = EXCLUDED.shares_held,
        holding_ratio = EXCLUDED.holding_ratio,
        change_amount = EXCLUDED.change_amount,
        change_ratio = EXCLUDED.change_ratio,
        updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 创建触发器
DROP TRIGGER IF EXISTS cn_top10_sharehold_insert_trigger ON public.cn_top10_sharehold;
CREATE TRIGGER cn_top10_sharehold_insert_trigger
    INSTEAD OF INSERT ON public.cn_top10_sharehold
    FOR EACH ROW
    EXECUTE FUNCTION public.cn_top10_sharehold_insert();

-- 授予视图权限
GRANT SELECT, INSERT ON public.cn_top10_sharehold TO anon, authenticated, service_role;
