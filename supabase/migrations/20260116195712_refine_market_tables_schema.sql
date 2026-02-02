-- Rename hkse to hkse_market
ALTER TABLE public.hkse RENAME TO hkse_market;

-- Change symbol column to text for hkse_market
ALTER TABLE public.hkse_market ALTER COLUMN symbol TYPE text USING symbol::text;

-- Change symbol column to text for share_a_market
ALTER TABLE public.share_a_market ALTER COLUMN symbol TYPE text USING symbol::text;
