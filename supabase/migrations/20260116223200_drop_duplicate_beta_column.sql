-- 删除 share_a_market 表中的重复列 beta_5_years.1
ALTER TABLE public.share_a_market DROP COLUMN IF EXISTS "beta_5_years.1";
