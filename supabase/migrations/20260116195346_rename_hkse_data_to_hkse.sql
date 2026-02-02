-- Rename the table hkse_data to hkse
ALTER TABLE public.hkse_data RENAME TO hkse;

-- Ensure the symbol column is bigint (it should be already, but being explicit as requested)
ALTER TABLE public.hkse ALTER COLUMN symbol TYPE bigint;
