-- 股东人数及集中度 (10年) 表
-- 存储从 AKShare 下载的股东人数和人均持股数据

CREATE TABLE IF NOT EXISTS public.holder_count_concentration_10y (
    id BIGSERIAL PRIMARY KEY,
    
    -- 基本信息
    report_date DATE NOT NULL,                    -- 变动日期 (报告期)
    symbol VARCHAR(20) NOT NULL,                  -- 证券代码 (600031)
    security_name VARCHAR(100),                   -- 证券简称
    
    -- 股东人数
    holder_count NUMERIC,                         -- 本期股东人数
    holder_count_prev NUMERIC,                    -- 上期股东人数
    holder_count_change NUMERIC,                  -- 股东人数增幅 (%)
    
    -- 人均持股
    avg_hold_num NUMERIC,                         -- 本期人均持股数量 (股)
    avg_hold_num_prev NUMERIC,                    -- 上期人均持股数量 (股)
    avg_hold_num_change NUMERIC,                  -- 人均持股数量增幅 (%)
    
    -- 元数据
    data_source VARCHAR(50) DEFAULT 'EastMoney',  -- 数据源
    updated_at TIMESTAMP DEFAULT NOW(),           -- 更新日期
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 唯一约束：同一股票同一报告期只能有一条记录
    CONSTRAINT holder_count_concentration_10y_unique UNIQUE (symbol, report_date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_holder_count_symbol ON public.holder_count_concentration_10y(symbol);
CREATE INDEX IF NOT EXISTS idx_holder_count_report_date ON public.holder_count_concentration_10y(report_date);
CREATE INDEX IF NOT EXISTS idx_holder_count_symbol_date ON public.holder_count_concentration_10y(symbol, report_date DESC);

-- 添加注释
COMMENT ON TABLE public.holder_count_concentration_10y IS '股东人数及集中度（10年历史数据）';
COMMENT ON COLUMN public.holder_count_concentration_10y.report_date IS '报告日期';
COMMENT ON COLUMN public.holder_count_concentration_10y.symbol IS '股票代码（纯数字6位）';
COMMENT ON COLUMN public.holder_count_concentration_10y.holder_count IS '本期股东人数';
COMMENT ON COLUMN public.holder_count_concentration_10y.holder_count_change IS '股东人数增幅（%）';
COMMENT ON COLUMN public.holder_count_concentration_10y.avg_hold_num IS '本期人均持股数量（股）';
COMMENT ON COLUMN public.holder_count_concentration_10y.avg_hold_num_change IS '人均持股数量增幅（%）';

-- 启用 RLS (Row Level Security)
ALTER TABLE public.holder_count_concentration_10y ENABLE ROW LEVEL SECURITY;

-- 创建公开读取策略
CREATE POLICY "Allow public read access" ON public.holder_count_concentration_10y
    FOR SELECT USING (true);

-- 创建服务角色写入策略
CREATE POLICY "Allow service role full access" ON public.holder_count_concentration_10y
    FOR ALL USING (auth.role() = 'service_role');
