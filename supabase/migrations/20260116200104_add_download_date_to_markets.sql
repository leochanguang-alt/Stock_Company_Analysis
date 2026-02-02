-- Add download_date column to us_market
ALTER TABLE public.us_market ADD COLUMN IF NOT EXISTS download_date DATE;

-- Add download_date column to hkse_market
ALTER TABLE public.hkse_market ADD COLUMN IF NOT EXISTS download_date DATE;

-- Add download_date column to share_a_market
ALTER TABLE public.share_a_market ADD COLUMN IF NOT EXISTS download_date DATE;
