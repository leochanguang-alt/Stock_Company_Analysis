-- 删除旧的temp表（如果存在）
DROP TABLE IF EXISTS public.temp;

-- 创建新的temp表，结构与CSV文件匹配
CREATE TABLE IF NOT EXISTS public.temp (
    id SERIAL PRIMARY KEY,
    "Symbol" TEXT,
    "Description" TEXT,
    "Enterprise value" NUMERIC,
    "Enterprise value - Currency" TEXT,
    "Market capitalization" NUMERIC,
    "Market capitalization - Currency" TEXT,
    "Total debt, Quarterly" NUMERIC,
    "Total debt, Quarterly - Currency" TEXT,
    "Total equity, Quarterly" NUMERIC,
    "Total equity, Quarterly - Currency" TEXT,
    "Total assets, Quarterly" NUMERIC,
    "Total assets, Quarterly - Currency" TEXT,
    "Beta 5 years" NUMERIC,
    "Cash from operating activities, Trailing 12 months" NUMERIC,
    "Cash from operating activities, Trailing 12 months - Currency" TEXT,
    "Cash from financing activities, Trailing 12 months" NUMERIC,
    "Cash from financing activities, Trailing 12 months - Currency" TEXT,
    "Total cash dividends paid, Annual" NUMERIC,
    "Total cash dividends paid, Annual - Currency" TEXT,
    "Industry" TEXT,
    "Sector" TEXT,
    "Exchange" TEXT,
    "Index" TEXT,
    "Beta 1 year" NUMERIC,
    "Simple Moving Average (120) 1 day" NUMERIC,
    "Exponential Moving Average (120) 1 day" NUMERIC,
    "Return on invested capital %, Trailing 12 months" NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Enable Row Level Security
ALTER TABLE public.temp ENABLE ROW LEVEL SECURITY;

-- Create policies
CREATE POLICY "Allow public read" ON public.temp FOR SELECT USING (true);
CREATE POLICY "Allow service insert" ON public.temp FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow service update" ON public.temp FOR UPDATE USING (true);
CREATE POLICY "Allow service delete" ON public.temp FOR DELETE USING (true);
