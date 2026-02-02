create table if not exists public.company_financials_long (
  symbol text not null,
  report_date text not null,
  statement_type text not null,
  account text not null,
  value numeric,
  data_source text,
  is_audited text,
  announcement_date text,
  currency text,
  report_type text,
  updated_at text,
  primary key (symbol, report_date, statement_type, account)
);
