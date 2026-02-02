#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
supabase = create_client(url, key)

print('=== 002594 cn_top10_sharehold 样本 ===')
result = supabase.table('cn_top10_sharehold').select('*').eq('symbol', '002594').order('report_date', desc=True).limit(3).execute()
for r in result.data:
    print(r)

print('\n=== 002508 company_financials_long 样本 ===')
result2 = supabase.table('company_financials_long').select('*').eq('symbol', '002508').limit(3).execute()
for r in result2.data:
    print(r)

print('\n=== 检查前端表名匹配 ===')
# 检查数据库实际表名
tables = ['company_financials_long', 'stock_valuation_history', 'cn_sharehold_data', 'cn_top10_sharehold']
for t in tables:
    result = supabase.table(t).select('symbol', count='exact').eq('symbol', '002594').limit(1).execute()
    print(f'{t}: {result.count} 条 (002594)')
    result = supabase.table(t).select('symbol', count='exact').eq('symbol', '002508').limit(1).execute()
    print(f'{t}: {result.count} 条 (002508)')
