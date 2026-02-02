#!/usr/bin/env python3
"""
从 hkse_market, us_market, share_a_market 三张表中提取 index 信息
并写入 stock_index 表。每个 index 名称创建一条单独的记录。
"""

import os
import sys
from dotenv import load_dotenv
from supabase import create_client
from typing import List, Dict, Any

# 加载环境变量
load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

# 要处理的表名
MARKET_TABLES = ['hkse_market', 'us_market', 'share_a_market']

def parse_index_names(index_str: str) -> List[str]:
    """
    解析 index 字段，提取所有 index 名称
    
    Args:
        index_str: 逗号分隔的 index 名称字符串
        
    Returns:
        index 名称列表
    """
    if not index_str or index_str.strip() == '':
        return []
    
    # 按逗号分隔并去除空格
    index_names = [name.strip() for name in index_str.split(',')]
    # 过滤掉空字符串
    return [name for name in index_names if name]

def fetch_market_data(supabase, table_name: str) -> List[Dict[str, Any]]:
    """
    从指定的市场表中获取所有数据
    
    Args:
        supabase: Supabase 客户端
        table_name: 表名
        
    Returns:
        记录列表
    """
    print(f"\n🔄 正在从 {table_name} 表获取数据...")
    
    all_records = []
    page_size = 1000
    offset = 0
    
    while True:
        response = supabase.table(table_name)\
            .select('symbol, description, index, download_date')\
            .range(offset, offset + page_size - 1)\
            .execute()
        
        if not response.data:
            break
            
        all_records.extend(response.data)
        offset += page_size
        
        if len(response.data) < page_size:
            break
    
    print(f"   ✅ 获取了 {len(all_records)} 条记录")
    return all_records

def create_stock_index_records(market_records: List[Dict[str, Any]], table_name: str) -> List[Dict[str, Any]]:
    """
    从市场记录创建 stock_index 记录
    
    Args:
        market_records: 市场表记录列表
        table_name: 源表名（用于日志）
        
    Returns:
        stock_index 记录列表
    """
    stock_index_records = []
    records_with_index = 0
    total_index_entries = 0
    
    for record in market_records:
        index_field = record.get('index', '')
        
        # 如果没有 index 信息，跳过
        if not index_field:
            continue
        
        records_with_index += 1
        index_names = parse_index_names(index_field)
        
        # 为每个 index 名称创建一条记录
        for index_name in index_names:
            stock_index_records.append({
                'symbol': record.get('symbol'),
                'description': record.get('description'),
                'index_name': index_name,
                'download_date': record.get('download_date')
            })
            total_index_entries += 1
    
    print(f"   📊 {table_name}: {records_with_index} 条记录包含 index 信息")
    print(f"   📊 {table_name}: 创建了 {total_index_entries} 条 stock_index 记录")
    
    return stock_index_records

def batch_insert_records(supabase, records: List[Dict[str, Any]], batch_size: int = 500):
    """
    批量插入记录到 stock_index 表
    
    Args:
        supabase: Supabase 客户端
        records: 要插入的记录列表
        batch_size: 每批插入的记录数
    """
    total_records = len(records)
    print(f"\n🔄 准备插入 {total_records} 条记录到 stock_index 表...")
    
    inserted_count = 0
    
    for i in range(0, total_records, batch_size):
        batch = records[i:i + batch_size]
        
        try:
            response = supabase.table('stock_index').insert(batch).execute()
            inserted_count += len(batch)
            print(f"   ✅ 已插入 {inserted_count}/{total_records} 条记录")
        except Exception as e:
            print(f"   ❌ 插入第 {i}-{i+len(batch)} 条记录时出错: {e}")
            raise
    
    print(f"   ✅ 成功插入所有 {inserted_count} 条记录")

def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ 错误: 未找到 SUPABASE_URL 或 SUPABASE_SERVICE_ROLE_KEY 环境变量。")
        sys.exit(1)

    print("=" * 60)
    print("从市场表提取 index 信息并写入 stock_index 表")
    print("=" * 60)

    try:
        # 连接 Supabase
        print("\n🔄 连接 Supabase...")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("   ✅ 连接成功!")

        all_stock_index_records = []
        
        # 处理每个市场表
        for table_name in MARKET_TABLES:
            print(f"\n{'=' * 60}")
            print(f"处理表: {table_name}")
            print(f"{'=' * 60}")
            
            # 获取市场数据
            market_records = fetch_market_data(supabase, table_name)
            
            # 创建 stock_index 记录
            stock_index_records = create_stock_index_records(market_records, table_name)
            all_stock_index_records.extend(stock_index_records)
        
        # 批量插入所有记录
        if all_stock_index_records:
            print(f"\n{'=' * 60}")
            print(f"汇总统计")
            print(f"{'=' * 60}")
            print(f"   📊 总共创建了 {len(all_stock_index_records)} 条 stock_index 记录")
            
            batch_insert_records(supabase, all_stock_index_records)
        else:
            print("\n⚠️  没有找到任何包含 index 信息的记录")

    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n" + "=" * 60)
    print("完成")
    print("=" * 60)

if __name__ == "__main__":
    main()
