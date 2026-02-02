#!/usr/bin/env python3
"""
Create temp tables for market data import using Supabase Management API.
"""
import os
import httpx
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_ACCESS_TOKEN = os.getenv("SUPABASE_ACCESS_TOKEN")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("缺少 SUPABASE_URL 或 SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_ACCESS_TOKEN:
    raise RuntimeError("缺少 SUPABASE_ACCESS_TOKEN")

# 从 URL 提取项目 ref
PROJECT_REF = SUPABASE_URL.replace("https://", "").split(".")[0]


def execute_sql(sql: str) -> list:
    """Execute SQL using Supabase Management API."""
    endpoint = f"https://api.supabase.com/v1/projects/{PROJECT_REF}/database/query"
    headers = {
        "Authorization": f"Bearer {SUPABASE_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    resp = httpx.post(endpoint, json={"query": sql}, headers=headers, timeout=60.0)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"SQL 执行失败: {resp.status_code} - {resp.text}")
    return resp.json()


def create_temp_tables():
    """Create temp tables with new columns."""
    # 创建三张 temp 表
    base_tables = ["us_market", "hkse_market", "share_a_market"]
    
    for base_table in base_tables:
        temp_table = f"{base_table}_temp"
        print(f"创建 {temp_table}...")
        
        # 检查表是否已存在
        result = execute_sql(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{temp_table}'
            );
        """)
        
        table_exists = result[0].get("exists", False) if result else False
        
        if table_exists:
            print(f"  {temp_table} 已存在，跳过创建")
        else:
            # 复制表结构
            execute_sql(f"CREATE TABLE IF NOT EXISTS public.{temp_table} (LIKE public.{base_table} INCLUDING ALL);")
            print(f"  创建 {temp_table} 完成")
        
        # 添加新列（如果不存在）
        print(f"  添加新列...")
        execute_sql(f"ALTER TABLE public.{temp_table} ADD COLUMN IF NOT EXISTS total_liabilities_quarterly DOUBLE PRECISION;")
        execute_sql(f"ALTER TABLE public.{temp_table} ADD COLUMN IF NOT EXISTS total_liabilities_quarterly_currency TEXT;")
        execute_sql(f"ALTER TABLE public.{temp_table} ADD COLUMN IF NOT EXISTS analyst_rating TEXT;")
        execute_sql(f"ALTER TABLE public.{temp_table} ADD COLUMN IF NOT EXISTS download_date DATE;")
        
        # 启用 RLS
        execute_sql(f"ALTER TABLE public.{temp_table} ENABLE ROW LEVEL SECURITY;")
        
        # 创建策略（先删除旧的）
        execute_sql(f"DROP POLICY IF EXISTS \"Allow public read\" ON public.{temp_table};")
        execute_sql(f"DROP POLICY IF EXISTS \"Allow service insert\" ON public.{temp_table};")
        execute_sql(f"DROP POLICY IF EXISTS \"Allow service update\" ON public.{temp_table};")
        execute_sql(f"DROP POLICY IF EXISTS \"Allow service delete\" ON public.{temp_table};")
        
        execute_sql(f"CREATE POLICY \"Allow public read\" ON public.{temp_table} FOR SELECT USING (true);")
        execute_sql(f"CREATE POLICY \"Allow service insert\" ON public.{temp_table} FOR INSERT WITH CHECK (true);")
        execute_sql(f"CREATE POLICY \"Allow service update\" ON public.{temp_table} FOR UPDATE USING (true);")
        execute_sql(f"CREATE POLICY \"Allow service delete\" ON public.{temp_table} FOR DELETE USING (true);")
        
        print(f"  {temp_table} 配置完成")
    
    print("\n所有 temp 表创建完成！")
    
    # 验证
    print("\n验证表结构:")
    for base_table in base_tables:
        temp_table = f"{base_table}_temp"
        result = execute_sql(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = '{temp_table}'
            AND column_name IN ('total_liabilities_quarterly', 'total_liabilities_quarterly_currency', 'analyst_rating', 'download_date')
            ORDER BY column_name;
        """)
        print(f"\n{temp_table}:")
        for row in result:
            print(f"  - {row.get('column_name')}: {row.get('data_type')}")


if __name__ == "__main__":
    create_temp_tables()
