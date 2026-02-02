alter table if exists public.company_list
add column if not exists currency text;

update public.company_list
set currency = case market
  when 'us' then 'USD'
  when 'hk' then 'HKD'
  when 'cn' then 'CNY'
  else null
end
where currency is null;
