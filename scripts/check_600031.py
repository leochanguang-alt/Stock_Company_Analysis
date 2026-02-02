#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

tables = [
    ("company_financials_long", "symbol"),
    ("stock_valuation_history", "symbol"),
    ("cn_sharehold_data", "symbol"),
    ("cn_top10_sharehold", "symbol"),
]

symbol = "600031"
print(f"=== 检查 {symbol} 在 Supabase 中的数据 ===")
for table, col in tables:
    try:
        result = supabase.table(table).select("*", count="exact").eq(col, symbol).limit(1).execute()
        count = result.count or 0
        print(f"  {table}: {count} 条记录")
    except Exception as e:
        print(f"  {table}: 错误 - {str(e)[:50]}")
