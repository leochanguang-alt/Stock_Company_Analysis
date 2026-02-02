#!/usr/bin/env python3
"""
去除所有市场中的重复记录（优先股/债券/沪港通重复股票）
- US Market: 优先股、单位股、权证等
- HK Market: 沪港通代码 (8xxxx) vs 正股代码 (0xxxx)
- CN Market: 检查是否有重复
"""

import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_all(table, date):
    """获取指定日期的所有数据"""
    data = []
    offset = 0
    while True:
        res = supabase.table(table).select('id,symbol,description,sector,enterprise_value,cash_from_operating_activities_trailing_12_months,total_equity_quarterly,total_assets_quarterly,total_debt_quarterly,market_capitalization').eq('download_date', date).range(offset, offset+999).execute()
        if not res.data:
            break
        data.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000
    return pd.DataFrame(data)


def is_us_preferred(symbol):
    """判断是否是美股优先股/单位股/权证"""
    if '/' in symbol:  # e.g., GS/PD
        return True
    if symbol.endswith('U') and len(symbol) > 3:  # e.g., EURKU (units)
        return True
    if symbol.endswith('P') and len(symbol) > 3:  # e.g., WVVIP
        return True
    if symbol.endswith('W') and len(symbol) > 3:  # warrants
        return True
    if '.A' in symbol or '.B' in symbol:  # e.g., PBR.A
        return True
    return False


def is_us_non_common_security(symbol, description):
    """识别美股非普通股证券（优先股/票据/信托等）"""
    if is_us_preferred(symbol):
        return True
    desc = str(description or '').lower()
    keywords = [
        'preferred', 'depositary', 'trust preferred', 'capital trust', 'trust',
        'notes', 'note due', 'subordinated', 'senior', 'perpetual', 'debenture',
        'warrant', 'unit', 'units', 'certificate', 'certificates',
        'fixed rate', 'floating rate', 'fixed-to-floating', 'cumulative',
        'series '
    ]
    return any(k in desc for k in keywords)


def should_drop_us_finance_row(row):
    """US Finance: 删除非普通股证券与无财务数据记录"""
    if str(row.get('sector', '')).strip().lower() != 'finance':
        return False
    if is_us_non_common_security(row.get('symbol'), row.get('description')):
        return True
    metrics = [
        row.get('enterprise_value'),
        row.get('cash_from_operating_activities_trailing_12_months'),
        row.get('total_equity_quarterly'),
        row.get('total_assets_quarterly'),
        row.get('total_debt_quarterly')
    ]
    return all(pd.isna(m) or m == 0 for m in metrics)


def is_hk_southbound(symbol):
    """判断是否是港股沪港通代码 (8开头)"""
    return symbol.startswith('8') and len(symbol) == 5


def find_duplicates_by_metrics(df, is_preferred_fn):
    """通过财务指标相似度找重复记录"""
    df = df.copy()
    df['ev'] = pd.to_numeric(df['enterprise_value'], errors='coerce')
    df['ocf'] = pd.to_numeric(df['cash_from_operating_activities_trailing_12_months'], errors='coerce')
    df['equity'] = pd.to_numeric(df['total_equity_quarterly'], errors='coerce')
    
    df_sorted = df.sort_values(['ev', 'ocf', 'equity']).reset_index(drop=True)
    
    to_delete = set()
    duplicates_found = []
    
    for i in range(len(df_sorted) - 1):
        row1 = df_sorted.iloc[i]
        row2 = df_sorted.iloc[i + 1]
        
        if row1['id'] in to_delete or row2['id'] in to_delete:
            continue
        if pd.isna(row1['ev']) or pd.isna(row2['ev']) or row1['ev'] == 0 or row2['ev'] == 0:
            continue
        if pd.isna(row1['equity']) or pd.isna(row2['equity']) or row1['equity'] == 0 or row2['equity'] == 0:
            continue
        
        # 计算相似度
        ev_sim = min(row1['ev'], row2['ev']) / max(row1['ev'], row2['ev'])
        equity_sim = min(abs(row1['equity']), abs(row2['equity'])) / max(abs(row1['equity']), abs(row2['equity']))
        
        # OCF 相似度
        if pd.isna(row1['ocf']) or pd.isna(row2['ocf']):
            ocf_similar = pd.isna(row1['ocf']) and pd.isna(row2['ocf'])
        elif row1['ocf'] == row2['ocf']:
            ocf_similar = True
        elif max(abs(row1['ocf']), abs(row2['ocf'])) > 0:
            ocf_diff = abs(row1['ocf'] - row2['ocf']) / max(abs(row1['ocf']), abs(row2['ocf']))
            ocf_similar = ocf_diff < 0.005
        else:
            ocf_similar = True
        
        if ev_sim > 0.995 and equity_sim > 0.995 and ocf_similar:
            pref1 = is_preferred_fn(row1['symbol'])
            pref2 = is_preferred_fn(row2['symbol'])
            
            delete_id = None
            if pref1 and not pref2:
                delete_id = row1['id']
                duplicates_found.append({'keep': row2['symbol'], 'delete': row1['symbol'], 'ocf': row1['ocf']})
            elif pref2 and not pref1:
                delete_id = row2['id']
                duplicates_found.append({'keep': row1['symbol'], 'delete': row2['symbol'], 'ocf': row1['ocf']})
            elif len(row1['symbol']) > len(row2['symbol']):
                delete_id = row1['id']
                duplicates_found.append({'keep': row2['symbol'], 'delete': row1['symbol'], 'ocf': row1['ocf']})
            elif len(row2['symbol']) > len(row1['symbol']):
                delete_id = row2['id']
                duplicates_found.append({'keep': row1['symbol'], 'delete': row2['symbol'], 'ocf': row1['ocf']})
            
            if delete_id:
                to_delete.add(delete_id)
    
    return to_delete, duplicates_found


def delete_records(table, ids):
    """批量删除记录"""
    if not ids:
        return
    id_list = list(ids)
    for i in range(0, len(id_list), 100):
        batch = id_list[i:i+100]
        supabase.table(table).delete().in_('id', batch).execute()


def process_us_market():
    """处理美股市场"""
    print('\n' + '='*60)
    print('🇺🇸 US Market 去重')
    print('='*60)
    
    total_deleted = 0
    for date in ['2025-10-03', '2025-11-01', '2026-01-15']:
        df = fetch_all('us_market', date)
        if df.empty:
            print(f'{date}: 无数据')
            continue

        # 先删除金融行业非普通股证券/无财务数据记录
        finance_drop_ids = set(df[df.apply(should_drop_us_finance_row, axis=1)]['id'].tolist())

        to_delete, duplicates = find_duplicates_by_metrics(df, is_us_preferred)
        to_delete = set(to_delete) | finance_drop_ids
        
        print(f'\n{date}: 发现 {len(to_delete)} 条重复记录/非普通股记录')
        if duplicates:
            duplicates.sort(key=lambda x: abs(x['ocf']) if pd.notna(x['ocf']) else 0, reverse=True)
            for d in duplicates[:5]:
                ocf = d['ocf'] / 1e9 if pd.notna(d['ocf']) else 0
                print(f'  {d["keep"]} -> 删除 {d["delete"]} (OCF={ocf:.2f}B)')
        
        delete_records('us_market', to_delete)
        total_deleted += len(to_delete)
    
    print(f'\n✅ US Market 共删除 {total_deleted} 条记录')
    return total_deleted


def process_hk_market():
    """处理港股市场"""
    print('\n' + '='*60)
    print('🇭🇰 HK Market 去重')
    print('='*60)
    
    total_deleted = 0
    for date in ['2025-10-03', '2025-11-01', '2026-01-15']:
        df = fetch_all('hkse_market', date)
        if df.empty:
            print(f'{date}: 无数据')
            continue
        
        to_delete, duplicates = find_duplicates_by_metrics(df, is_hk_southbound)
        
        print(f'\n{date}: 发现 {len(to_delete)} 条重复记录')
        if duplicates:
            duplicates.sort(key=lambda x: abs(x['ocf']) if pd.notna(x['ocf']) else 0, reverse=True)
            for d in duplicates[:5]:
                ocf = d['ocf'] / 1e9 if pd.notna(d['ocf']) else 0
                print(f'  {d["keep"]} -> 删除 {d["delete"]} (OCF={ocf:.2f}B)')
        
        delete_records('hkse_market', to_delete)
        total_deleted += len(to_delete)
    
    print(f'\n✅ HK Market 共删除 {total_deleted} 条记录')
    return total_deleted


def process_cn_market():
    """处理A股市场"""
    print('\n' + '='*60)
    print('🇨🇳 CN Market (A股) 去重')
    print('='*60)
    
    total_deleted = 0
    for date in ['2025-08-12', '2026-01-16']:
        df = fetch_all('share_a_market', date)
        if df.empty:
            print(f'{date}: 无数据')
            continue
        
        # A股一般没有优先股，但检查是否有财务数据重复
        to_delete, duplicates = find_duplicates_by_metrics(df, lambda x: False)
        
        print(f'\n{date}: 发现 {len(to_delete)} 条重复记录')
        if duplicates:
            duplicates.sort(key=lambda x: abs(x['ocf']) if pd.notna(x['ocf']) else 0, reverse=True)
            for d in duplicates[:5]:
                ocf = d['ocf'] / 1e9 if pd.notna(d['ocf']) else 0
                print(f'  {d["keep"]} -> 删除 {d["delete"]} (OCF={ocf:.2f}B)')
        
        delete_records('share_a_market', to_delete)
        total_deleted += len(to_delete)
    
    print(f'\n✅ CN Market 共删除 {total_deleted} 条记录')
    return total_deleted


def verify_results():
    """验证结果"""
    print('\n' + '='*60)
    print('📊 去重后各市场记录数')
    print('='*60)
    
    print('\nUS Market:')
    for date in ['2025-10-03', '2025-11-01', '2026-01-15']:
        res = supabase.table('us_market').select('*', count='exact').eq('download_date', date).execute()
        print(f'  {date}: {res.count}')
    
    print('\nHK Market:')
    for date in ['2025-10-03', '2025-11-01', '2026-01-15']:
        res = supabase.table('hkse_market').select('*', count='exact').eq('download_date', date).execute()
        print(f'  {date}: {res.count}')
    
    print('\nCN Market:')
    for date in ['2025-08-12', '2026-01-16']:
        res = supabase.table('share_a_market').select('*', count='exact').eq('download_date', date).execute()
        print(f'  {date}: {res.count}')


if __name__ == '__main__':
    us_deleted = process_us_market()
    hk_deleted = process_hk_market()
    cn_deleted = process_cn_market()
    
    print('\n' + '='*60)
    print(f'🎉 总计删除: US={us_deleted}, HK={hk_deleted}, CN={cn_deleted}')
    print('='*60)
    
    verify_results()
