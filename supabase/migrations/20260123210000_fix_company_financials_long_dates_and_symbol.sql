-- Normalize symbol format to match company_list (CN symbols)
update public.company_financials_long c
set symbol = cl.symbol
from public.company_list cl
where cl.market = 'cn'
  and lpad(c.symbol, 6, '0') = cl.symbol;

-- Convert announcement_date to date
alter table public.company_financials_long
  alter column announcement_date type date
  using (
    case
      when announcement_date ~ '^[0-9]{8}$' then to_date(announcement_date, 'YYYYMMDD')
      when announcement_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then announcement_date::date
      when announcement_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' then left(announcement_date, 10)::date
      else null
    end
  );

-- Convert updated_at to date
alter table public.company_financials_long
  alter column updated_at type date
  using (
    case
      when updated_at ~ '^[0-9]{8}$' then to_date(updated_at, 'YYYYMMDD')
      when updated_at ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then updated_at::date
      when updated_at ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' then left(updated_at, 10)::date
      else null
    end
  );
