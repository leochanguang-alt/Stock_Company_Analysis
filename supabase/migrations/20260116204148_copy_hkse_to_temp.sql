-- 复制 hkse_market 结构到 temp
CREATE TABLE IF NOT EXISTS public.temp (LIKE public.hkse_market INCLUDING ALL);

-- 启用 Row Level Security
ALTER TABLE public.temp ENABLE ROW LEVEL SECURITY;

-- 创建策略
CREATE POLICY "Allow public read" ON public.temp FOR SELECT USING (true);
CREATE POLICY "Allow service insert" ON public.temp FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow service update" ON public.temp FOR UPDATE USING (true);
CREATE POLICY "Allow service delete" ON public.temp FOR DELETE USING (true);
