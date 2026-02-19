-- Rename market value column to explicit HKD/亿 unit label
ALTER TABLE public.hk_company_mkt_cap
RENAME COLUMN market_value TO "market_value_hkd_亿元";
