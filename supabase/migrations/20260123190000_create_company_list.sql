-- Create company_list table
create table if not exists public.company_list (
  symbol text not null,
  market text not null,
  description text,
  sector text,
  industry text,
  exchange text,
  primary key (symbol, market)
);

-- Populate from existing market tables
insert into public.company_list (symbol, market, description, sector, industry, exchange)
select distinct
  symbol::text,
  'us'::text as market,
  description::text,
  sector::text,
  industry::text,
  exchange::text
from public.us_market
where symbol is not null
on conflict (symbol, market) do nothing;

insert into public.company_list (symbol, market, description, sector, industry, exchange)
select distinct
  symbol::text,
  'hk'::text as market,
  description::text,
  sector::text,
  industry::text,
  exchange::text
from public.hkse_market
where symbol is not null
on conflict (symbol, market) do nothing;

insert into public.company_list (symbol, market, description, sector, industry, exchange)
select distinct
  symbol::text,
  'cn'::text as market,
  description::text,
  sector::text,
  industry::text,
  exchange::text
from public.share_a_market
where symbol is not null
on conflict (symbol, market) do nothing;
