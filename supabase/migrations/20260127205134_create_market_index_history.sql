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

-- 创建 public view 用于前端访问
CREATE OR REPLACE VIEW public.market_index_history AS
SELECT * FROM stock_analysis.market_index_history;

-- 创建 upsert 触发器函数
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

-- 创建 INSTEAD OF INSERT 触发器
CREATE TRIGGER market_index_history_insert_trigger
    INSTEAD OF INSERT ON public.market_index_history
    FOR EACH ROW
    EXECUTE FUNCTION public.market_index_history_insert();

-- 启用 RLS
ALTER TABLE stock_analysis.market_index_history ENABLE ROW LEVEL SECURITY;

-- 创建 RLS 策略 (允许 anon 读取)
CREATE POLICY "Allow anon read market_index_history"
    ON stock_analysis.market_index_history
    FOR SELECT
    TO anon
    USING (true);

-- 允许 service_role 完全访问
CREATE POLICY "Allow service_role full access market_index_history"
    ON stock_analysis.market_index_history
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);
