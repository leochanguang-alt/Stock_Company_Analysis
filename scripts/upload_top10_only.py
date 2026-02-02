#!/usr/bin/env python3
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
import math
import numpy as np

load_dotenv()
url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
supabase = create_client(url, key)

def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    if isinstance(v, (np.floating, np.integer)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v) if isinstance(v, np.floating) else int(v)
    return v

def clean_record(record):
    return {k: clean_value(v) for k, v in record.items()}

def chunked(items, size=500):
    return [items[i:i + size] for i in range(0, len(items), size)]

def load_top10(symbol):
    path = os.path.join('outputs', f'{symbol}_top10_shareholders_10y.csv')
    if not os.path.exists(path):
        print(f'  文件不存在: {path}')
        return []
    df = pd.read_csv(path, dtype={'股票代码': str})
    
    records = []
    for _, row in df.iterrows():
        if pd.isna(row.get('名次')):
            continue
        
        raw_symbol = str(row.get('股票代码', '')).strip()
        if raw_symbol.upper().startswith(('SZ', 'SH', 'SS')):
            raw_symbol = raw_symbol[2:]
        formatted_symbol = raw_symbol.zfill(6) if raw_symbol.isdigit() else raw_symbol
        
        records.append({
            'symbol': formatted_symbol,
            'report_date': row.get('报告期'),
            'rank': int(row.get('名次')),
            'shareholder_name': row.get('股东名称'),
            'share_type': row.get('股份类型'),
            'shares_held': int(row.get('持股数')) if pd.notna(row.get('持股数')) else None,
            'holding_ratio': row.get('占总股本持股比例') if pd.notna(row.get('占总股本持股比例')) else None,
            'change_amount': row.get('增减'),
            'change_ratio': row.get('变动比率') if pd.notna(row.get('变动比率')) else None,
        })
    return records

symbols = sys.argv[1:] if len(sys.argv) > 1 else ['002508', '600066']

for symbol in symbols:
    print(f'处理 {symbol}...', flush=True)
    
    records = load_top10(symbol)
    if not records:
        continue
    print(f'  加载 {len(records)} 条记录', flush=True)
    
    uploaded = 0
    for batch in chunked(records, 500):
        cleaned_batch = [clean_record(r) for r in batch]
        try:
            supabase.table('cn_top10_sharehold').insert(cleaned_batch).execute()
            uploaded += len(batch)
        except Exception as e:
            err_str = str(e)[:100]
            if 'duplicate' not in err_str.lower():
                print(f'  错误: {err_str}', flush=True)
    
    print(f'  上传完成: {uploaded} 条', flush=True)

print('全部完成!', flush=True)
