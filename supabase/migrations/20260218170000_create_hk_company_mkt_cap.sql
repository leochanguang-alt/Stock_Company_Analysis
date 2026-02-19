-- Create HK company market cap history table from AkShare CSV schema
-- Base CSV columns: date, market_value
-- Additional columns aligned with hk_balance_sheet style:
--   secucode, security_code, security_name_abbr

CREATE TABLE IF NOT EXISTS public.hk_company_mkt_cap (
  id BIGSERIAL PRIMARY KEY,
  date DATE NOT NULL,
  market_value NUMERIC(24, 6),
  secucode TEXT,
  security_code TEXT,
  security_name_abbr TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS hk_company_mkt_cap_secucode_date_uk
  ON public.hk_company_mkt_cap (secucode, date);

CREATE INDEX IF NOT EXISTS hk_company_mkt_cap_date_idx
  ON public.hk_company_mkt_cap (date);

CREATE INDEX IF NOT EXISTS hk_company_mkt_cap_security_code_idx
  ON public.hk_company_mkt_cap (security_code);

ALTER TABLE public.hk_company_mkt_cap ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'hk_company_mkt_cap'
      AND policyname = 'Allow public read hk_company_mkt_cap'
  ) THEN
    CREATE POLICY "Allow public read hk_company_mkt_cap"
      ON public.hk_company_mkt_cap
      FOR SELECT
      TO anon, authenticated
      USING (true);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies
    WHERE schemaname = 'public'
      AND tablename = 'hk_company_mkt_cap'
      AND policyname = 'Allow service write hk_company_mkt_cap'
  ) THEN
    CREATE POLICY "Allow service write hk_company_mkt_cap"
      ON public.hk_company_mkt_cap
      FOR ALL
      TO service_role
      USING (true)
      WITH CHECK (true);
  END IF;
END $$;
