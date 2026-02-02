-- 创建中国A股股东人数及集中度数据表
-- 数据来源: outputs/002508_holder_count_concentration_10y.csv

-- 确保 stock_analysis schema 存在
CREATE SCHEMA IF NOT EXISTS stock_analysis;

CREATE TABLE IF NOT EXISTS stock_analysis.cn_sharehold_data (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,                    -- 证券代码 (如 002508)
    name VARCHAR(50),                               -- 证券简称 (如 老板电器)
    report_date DATE NOT NULL,                      -- 变动日期 (如 2016-03-31)
    current_holder_count DECIMAL(15,2),             -- 本期股东人数
    previous_holder_count DECIMAL(15,2),            -- 上期股东人数
    holder_count_change_pct DECIMAL(10,2),          -- 股东人数增幅 (%)
    current_avg_shares DECIMAL(15,2),               -- 本期人均持股数量
    previous_avg_shares DECIMAL(15,2),              -- 上期人均持股数量
    avg_shares_change_pct DECIMAL(10,2),            -- 人均持股数量增幅 (%)
    report_period VARCHAR(8),                       -- 报告期 (如 20160331)
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- 唯一约束: 同一股票同一报告日期只有一条记录
    UNIQUE(symbol, report_date)
);

-- 创建索引以优化查询性能
CREATE INDEX IF NOT EXISTS idx_cn_sharehold_symbol ON stock_analysis.cn_sharehold_data(symbol);
CREATE INDEX IF NOT EXISTS idx_cn_sharehold_date ON stock_analysis.cn_sharehold_data(report_date);
CREATE INDEX IF NOT EXISTS idx_cn_sharehold_period ON stock_analysis.cn_sharehold_data(report_period);

-- 添加表注释
COMMENT ON TABLE stock_analysis.cn_sharehold_data IS '中国A股股东人数及集中度数据';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.symbol IS '证券代码';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.name IS '证券简称';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.report_date IS '变动日期';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.current_holder_count IS '本期股东人数';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.previous_holder_count IS '上期股东人数';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.holder_count_change_pct IS '股东人数增幅(%)';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.current_avg_shares IS '本期人均持股数量';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.previous_avg_shares IS '上期人均持股数量';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.avg_shares_change_pct IS '人均持股数量增幅(%)';
COMMENT ON COLUMN stock_analysis.cn_sharehold_data.report_period IS '报告期标识';

-- 启用 RLS (Row Level Security)
ALTER TABLE stock_analysis.cn_sharehold_data ENABLE ROW LEVEL SECURITY;

-- 创建允许所有用户读取的策略
CREATE POLICY "Allow public read access" ON stock_analysis.cn_sharehold_data
    FOR SELECT USING (true);

-- 创建允许认证用户写入的策略
CREATE POLICY "Allow authenticated insert" ON stock_analysis.cn_sharehold_data
    FOR INSERT WITH CHECK (true);

CREATE POLICY "Allow authenticated update" ON stock_analysis.cn_sharehold_data
    FOR UPDATE USING (true);
