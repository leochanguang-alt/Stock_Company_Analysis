#!/usr/bin/env python3
"""
Generate data for Fundamental-based Stock Recommendations dashboard.
Filters stocks based on:
  - OCF/Assets > 10%
  - GAP < 0.8
  - EMA > SMA

Generates data for each available date with next-period market cap comparison.
"""

import os
import json
import pandas as pd
import numpy as np
import math
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

MARKETS = {
    'us': {'table': 'us_market', 'label': 'US Market'},
    'hk': {'table': 'hkse_market', 'label': 'HK Market'},
    'cn': {'table': 'share_a_market', 'label': 'A股市场'}
}

def fetch_all(supabase, table):
    """Fetch all records from a table using pagination."""
    all_records = []
    offset = 0
    limit = 1000
    while True:
        res = supabase.table(table).select('*').range(offset, offset+limit-1).execute()
        if not res.data:
            break
        all_records.extend(res.data)
        if len(res.data) < limit:
            break
        offset += limit
    return pd.DataFrame(all_records)

def safe_float(val):
    """Convert value to float safely, return None for invalid values."""
    if pd.isna(val) or val is None:
        return None
    try:
        f = float(val)
        if math.isinf(f) or math.isnan(f):
            return None
        return f
    except:
        return None

def format_large_number(val):
    """Format large numbers for display (e.g., 1.5T, 200B, 50M)."""
    if val is None:
        return None
    abs_val = abs(val)
    if abs_val >= 1e12:
        return f"{val/1e12:.2f}T"
    elif abs_val >= 1e9:
        return f"{val/1e9:.2f}B"
    elif abs_val >= 1e6:
        return f"{val/1e6:.2f}M"
    elif abs_val >= 1e3:
        return f"{val/1e3:.2f}K"
    else:
        return f"{val:.2f}"

def calculate_metrics(df):
    """Calculate derived metrics for each stock."""
    df = df.copy()
    
    # Convert to numeric
    num_cols = [
        'cash_from_operating_activities_trailing_12_months',
        'total_assets_quarterly',
        'enterprise_value',
        'total_debt_quarterly',
        'market_capitalization',
        'total_equity_quarterly',
        'exponential_moving_average_120_1_day',
        'simple_moving_average_120_1_day'
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate metrics
    df['ocf'] = df['cash_from_operating_activities_trailing_12_months']
    df['assets'] = df['total_assets_quarterly']
    df['ev'] = df['enterprise_value']
    df['debt'] = df['total_debt_quarterly']
    df['mkt_cap'] = df['market_capitalization']
    df['equity'] = df['total_equity_quarterly']
    df['ema'] = df['exponential_moving_average_120_1_day']
    df['sma'] = df['simple_moving_average_120_1_day']
    
    # OCF/Assets ratio
    df['ocf_assets'] = df['ocf'] / df['assets'].replace(0, np.nan)
    
    # OCF/EV ratio
    df['ocf_ev'] = df['ocf'] / df['ev'].replace(0, np.nan)
    
    # Multiplier and Valuation
    df['multiplier'] = (df['ocf_assets'].fillna(0) * 100) + 5
    df['valuation'] = df['ocf'] * df['multiplier']
    
    # GAP = EV / Valuation
    df['gap'] = df['ev'] / df['valuation'].replace(0, np.nan)
    
    return df

def filter_recommendations(df):
    """
    Filter stocks based on recommendation criteria:
    - OCF/Assets > 10%
    - GAP < 0.8
    - EMA > SMA (if EMA/SMA data available)
    """
    df = df.copy()
    
    # Criteria 1: OCF/Assets > 10%
    cond1 = df['ocf_assets'] > 0.10
    
    # Criteria 2: GAP < 0.8
    cond2 = df['gap'] < 0.8
    
    # Criteria 3: EMA > SMA (only if data available)
    has_ema_data = df['ema'].notna().any()
    if has_ema_data:
        cond3 = df['ema'] > df['sma']
    else:
        cond3 = pd.Series([True] * len(df), index=df.index)  # Skip this criteria
    
    # Apply all conditions
    filtered = df[cond1 & cond2 & cond3].copy()
    
    return filtered, has_ema_data

def build_recommendation_record(row, next_mkt_cap=None):
    """Build a recommendation record for JSON output."""
    mkt_cap_current = safe_float(row.get('mkt_cap'))
    mkt_cap_change = None
    mkt_cap_change_pct = None
    
    if mkt_cap_current is not None and next_mkt_cap is not None:
        mkt_cap_change = next_mkt_cap - mkt_cap_current
        if mkt_cap_current != 0:
            mkt_cap_change_pct = (mkt_cap_change / mkt_cap_current) * 100
    
    return {
        'symbol': str(row.get('symbol', '')),
        'description': str(row.get('description', '')),
        'sector': str(row.get('sector', '')),
        'industry': str(row.get('industry', '')),
        'mkt_cap': mkt_cap_current,
        'mkt_cap_fmt': format_large_number(mkt_cap_current),
        'ev': safe_float(row.get('ev')),
        'ev_fmt': format_large_number(safe_float(row.get('ev'))),
        'equity': safe_float(row.get('equity')),
        'equity_fmt': format_large_number(safe_float(row.get('equity'))),
        'debt': safe_float(row.get('debt')),
        'debt_fmt': format_large_number(safe_float(row.get('debt'))),
        'ocf': safe_float(row.get('ocf')),
        'ocf_fmt': format_large_number(safe_float(row.get('ocf'))),
        'ocf_assets': safe_float(row.get('ocf_assets')),
        'ocf_ev': safe_float(row.get('ocf_ev')),
        'gap': safe_float(row.get('gap')),
        'ema': safe_float(row.get('ema')),
        'sma': safe_float(row.get('sma')),
        'next_mkt_cap': safe_float(next_mkt_cap),
        'next_mkt_cap_fmt': format_large_number(safe_float(next_mkt_cap)),
        'mkt_cap_change': safe_float(mkt_cap_change),
        'mkt_cap_change_fmt': format_large_number(safe_float(mkt_cap_change)),
        'mkt_cap_change_pct': safe_float(mkt_cap_change_pct)
    }

def format_date_label(date_str):
    """Format date to readable label like 'Oct 2025'."""
    from datetime import datetime
    d = datetime.strptime(date_str, '%Y-%m-%d')
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    return f"{months[d.month-1]} {d.year}"

def main():
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    recommendations = {}
    
    for market_key, market_info in MARKETS.items():
        print(f"🔄 Processing {market_info['label']}...")
        df = fetch_all(supabase, market_info['table'])
        
        if df.empty:
            print(f"  ⚠️ No data for {market_info['label']}")
            recommendations[market_key] = {
                'label': market_info['label'],
                'dates': [],
                'by_date': {}
            }
            continue
        
        # Get all dates sorted
        dates = sorted(df['download_date'].unique())
        print(f"  📅 Available dates: {dates}")
        
        # Build market cap lookup for each date
        mkt_cap_by_date = {}
        for date in dates:
            df_date = df[df['download_date'] == date].copy()
            df_date['market_capitalization'] = pd.to_numeric(df_date['market_capitalization'], errors='coerce')
            mkt_cap_by_date[date] = dict(zip(df_date['symbol'], df_date['market_capitalization']))
        
        # Process each date
        by_date = {}
        for i, date in enumerate(dates):
            print(f"  📊 Processing {date}...")
            
            df_date = df[df['download_date'] == date].copy()
            df_date = calculate_metrics(df_date)
            
            # Get next date's market cap for comparison
            next_date = dates[i + 1] if i + 1 < len(dates) else None
            next_mkt_cap_map = mkt_cap_by_date.get(next_date, {}) if next_date else {}
            
            # Filter recommendations
            df_filtered, has_ema_data = filter_recommendations(df_date)
            ema_note = "" if has_ema_data else " (no EMA/SMA data)"
            print(f"    ✅ Found {len(df_filtered)} stocks meeting criteria{ema_note}")
            
            # Build records
            stocks = []
            for _, row in df_filtered.iterrows():
                symbol = row['symbol']
                next_mkt_cap = next_mkt_cap_map.get(symbol)
                rec = build_recommendation_record(row, next_mkt_cap)
                stocks.append(rec)
            
            # Get unique sectors and industries
            sectors = sorted(df_filtered['sector'].dropna().unique().tolist())
            industries = sorted(df_filtered['industry'].dropna().unique().tolist())
            
            by_date[date] = {
                'date': date,
                'date_label': format_date_label(date),
                'next_date': next_date,
                'next_date_label': format_date_label(next_date) if next_date else None,
                'total_stocks': len(df_date),
                'filtered_count': len(df_filtered),
                'has_ema_data': has_ema_data,
                'stocks': stocks,
                'sectors': sectors,
                'industries': industries
            }
        
        # Build date options for dropdown
        date_options = [
            {'value': d, 'label': format_date_label(d), 'next_label': format_date_label(dates[i+1]) if i+1 < len(dates) else None}
            for i, d in enumerate(dates)
        ]
        
        recommendations[market_key] = {
            'label': market_info['label'],
            'dates': date_options,
            'by_date': by_date
        }
    
    # Build output
    output = {
        'recommendations': recommendations,
        'criteria': {
            'ocf_assets_min': 0.10,
            'gap_max': 0.8,
            'ema_gt_sma': True
        },
        'generated_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Write JS file
    with open('recommend_data.js', 'w') as f:
        f.write(f"const recommendData = {json.dumps(output, indent=2, default=str)};")
    
    print("✅ Data written to recommend_data.js")

if __name__ == "__main__":
    main()
