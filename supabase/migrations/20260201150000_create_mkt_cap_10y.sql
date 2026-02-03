-- 市值历史 (10年) 表
-- 存储从 AKShare 下载的每日市值数据

CREATE TABLE IF NOT EXISTS public.mkt_cap_10y (
    id BIGSERIAL PRIMARY KEY,
    
    -- 基本信息
    trade_date DATE NOT NULL,                     -- 交易日期
    symbol VARCHAR(20) NOT NULL,                  -- 股票代码 (600031)
    
    -- 市值数据
    mkt_cap_billion_cny NUMERIC,                  -- 总市值 (亿元人民币)
    
    -- 元数据
    data_source VARCHAR(50) DEFAULT 'AKShare',    -- 数据源
    updated_at TIMESTAMP DEFAULT NOW(),           -- 更新日期
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 唯一约束：同一股票同一交易日只能有一条记录
    CONSTRAINT mkt_cap_10y_unique UNIQUE (symbol, trade_date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_mkt_cap_symbol ON public.mkt_cap_10y(symbol);
CREATE INDEX IF NOT EXISTS idx_mkt_cap_trade_date ON public.mkt_cap_10y(trade_date);
CREATE INDEX IF NOT EXISTS idx_mkt_cap_symbol_date ON public.mkt_cap_10y(symbol, trade_date DESC);

-- 添加注释
COMMENT ON TABLE public.mkt_cap_10y IS '市值历史（10年每日数据）';
COMMENT ON COLUMN public.mkt_cap_10y.trade_date IS '交易日期';
COMMENT ON COLUMN public.mkt_cap_10y.symbol IS '股票代码（纯数字6位）';
COMMENT ON COLUMN public.mkt_cap_10y.mkt_cap_billion_cny IS '总市值（亿元人民币）';

-- 启用 RLS (Row Level Security)
ALTER TABLE public.mkt_cap_10y ENABLE ROW LEVEL SECURITY;

-- 创建公开读取策略
CREATE POLICY "Allow public read access" ON public.mkt_cap_10y
    FOR SELECT USING (true);

-- 创建服务角色写入策略
CREATE POLICY "Allow service role full access" ON public.mkt_cap_10y
    FOR ALL USING (auth.role() = 'service_role');
