-- 前十大股东 (10年) 表
-- 存储从 AKShare 下载的前十大股东数据

CREATE TABLE IF NOT EXISTS public.top10_shareholders_10y (
    id BIGSERIAL PRIMARY KEY,
    
    -- 基本信息
    report_date DATE NOT NULL,                    -- 报告期 (20250930 -> 2025-09-30)
    symbol VARCHAR(20) NOT NULL,                  -- 股票代码 (600031)
    stock_code VARCHAR(20),                       -- 完整股票代码 (SH600031)
    
    -- 股东信息
    rank INTEGER NOT NULL,                        -- 名次 (1-10)
    shareholder_name VARCHAR(200) NOT NULL,       -- 股东名称
    share_type VARCHAR(50),                       -- 股份类型 (流通A股/流通B股/限售股等)
    
    -- 持股数据
    hold_num NUMERIC,                             -- 持股数 (股)
    hold_ratio NUMERIC,                           -- 占总股本持股比例 (%)
    
    -- 变动信息
    change_num NUMERIC,                           -- 增减 (股)
    change_ratio NUMERIC,                         -- 变动比率 (%)
    
    -- 元数据
    data_source VARCHAR(50) DEFAULT 'EastMoney',  -- 数据源
    updated_at TIMESTAMP DEFAULT NOW(),           -- 更新日期
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 唯一约束：同一股票同一报告期同一名次只能有一条记录
    CONSTRAINT top10_shareholders_10y_unique UNIQUE (symbol, report_date, rank)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_top10_sh_symbol ON public.top10_shareholders_10y(symbol);
CREATE INDEX IF NOT EXISTS idx_top10_sh_report_date ON public.top10_shareholders_10y(report_date);
CREATE INDEX IF NOT EXISTS idx_top10_sh_symbol_date ON public.top10_shareholders_10y(symbol, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_top10_sh_holder_name ON public.top10_shareholders_10y(shareholder_name);

-- 添加注释
COMMENT ON TABLE public.top10_shareholders_10y IS '前十大股东（10年历史数据）';
COMMENT ON COLUMN public.top10_shareholders_10y.report_date IS '报告日期';
COMMENT ON COLUMN public.top10_shareholders_10y.symbol IS '股票代码（纯数字6位）';
COMMENT ON COLUMN public.top10_shareholders_10y.rank IS '股东排名（1-10）';
COMMENT ON COLUMN public.top10_shareholders_10y.shareholder_name IS '股东名称';
COMMENT ON COLUMN public.top10_shareholders_10y.hold_num IS '持股数量（股）';
COMMENT ON COLUMN public.top10_shareholders_10y.hold_ratio IS '占总股本比例（%）';

-- 启用 RLS (Row Level Security)
ALTER TABLE public.top10_shareholders_10y ENABLE ROW LEVEL SECURITY;

-- 创建公开读取策略
CREATE POLICY "Allow public read access" ON public.top10_shareholders_10y
    FOR SELECT USING (true);

-- 创建服务角色写入策略
CREATE POLICY "Allow service role full access" ON public.top10_shareholders_10y
    FOR ALL USING (auth.role() = 'service_role');
