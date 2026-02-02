-- Create stock_index table
CREATE TABLE IF NOT EXISTS public.stock_index (
    id SERIAL PRIMARY KEY,
    symbol TEXT,
    description TEXT,
    index_name TEXT,
    download_date DATE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Enable Row Level Security
ALTER TABLE public.stock_index ENABLE ROW LEVEL SECURITY;

-- Create policies
CREATE POLICY "Allow public read" ON public.stock_index FOR SELECT USING (true);
CREATE POLICY "Allow service insert" ON public.stock_index FOR INSERT WITH CHECK (true);
CREATE POLICY "Allow service update" ON public.stock_index FOR UPDATE USING (true);
CREATE POLICY "Allow service delete" ON public.stock_index FOR DELETE USING (true);
