#!/usr/bin/env python3
"""
Generate data for Market Valuation Dynamics dashboard.
Calculates market-level aggregates for all available dates.
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

def calc_market_summary(df):
    """Calculate market summary excluding Finance sector."""
    df_non_fin = df[df['sector'] != 'Finance'].copy()
    if df_non_fin.empty:
        return None
    
    # Ensure numeric columns
    num_cols = ['cash_from_operating_activities_trailing_12_months', 'total_assets_quarterly',
                'enterprise_value', 'total_debt_quarterly', 'market_capitalization']
    for col in num_cols:
        if col in df_non_fin.columns:
            df_non_fin[col] = pd.to_numeric(df_non_fin[col], errors='coerce').fillna(0)
    
    total_ocf = df_non_fin['cash_from_operating_activities_trailing_12_months'].sum()
    total_assets = df_non_fin['total_assets_quarterly'].sum()
    total_ev = df_non_fin['enterprise_value'].sum()
    total_debt = df_non_fin['total_debt_quarterly'].sum()
    total_mcap = df_non_fin['market_capitalization'].sum()
    
    return {
        'total_ocf': float(total_ocf) if not math.isnan(total_ocf) else 0,
        'total_assets': float(total_assets) if not math.isnan(total_assets) else 0,
        'total_ev': float(total_ev) if not math.isnan(total_ev) else 0,
        'total_debt': float(total_debt) if not math.isnan(total_debt) else 0,
        'total_mcap': float(total_mcap) if not math.isnan(total_mcap) else 0,
        'ocf_assets': float(total_ocf / total_assets) if total_assets != 0 else 0,
        'ocf_ev': float(total_ocf / total_ev) if total_ev != 0 else 0
    }

def calc_company_data(df):
    """Calculate company-level metrics."""
    df = df.copy()
    num_cols = ['cash_from_operating_activities_trailing_12_months', 'total_assets_quarterly',
                'enterprise_value', 'total_debt_quarterly', 'market_capitalization', 'total_equity_quarterly']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    df['ocf'] = df['cash_from_operating_activities_trailing_12_months']
    df['assets'] = df['total_assets_quarterly']
    df['ev'] = df['enterprise_value']
    df['debts'] = df['total_debt_quarterly']
    df['equity'] = df.get('total_equity_quarterly', 0)
    df['mkt_cap'] = df.get('market_capitalization', 0)
    
    df['ocf_assets'] = df['ocf'] / df['assets'].replace(0, np.nan)
    df['ocf_ev'] = df['ocf'] / df['ev'].replace(0, np.nan)
    df['multiplier'] = (df['ocf_assets'].fillna(0) * 100) + 5
    df['valuation'] = df['ocf'] * df['multiplier']
    df['gap'] = df['ev'] / df['valuation'].replace(0, np.nan)
    
    # Clean for JSON
    result = []
    for _, row in df.iterrows():
        r = {
            'symbol': str(row.get('symbol', '')),
            'description': str(row.get('description', '')),
            'sector': str(row.get('sector', '')),
            'industry': str(row.get('industry', '')),
            'ocf': float(row['ocf']) if pd.notna(row['ocf']) and not math.isinf(row['ocf']) else 0,
            'assets': float(row['assets']) if pd.notna(row['assets']) and not math.isinf(row['assets']) else 0,
            'ev': float(row['ev']) if pd.notna(row['ev']) and not math.isinf(row['ev']) else 0,
            'debts': float(row['debts']) if pd.notna(row['debts']) and not math.isinf(row['debts']) else 0,
            'equity': float(row['equity']) if pd.notna(row['equity']) and not math.isinf(row['equity']) else 0,
            'mkt_cap': float(row['mkt_cap']) if pd.notna(row['mkt_cap']) and not math.isinf(row['mkt_cap']) else 0,
            'ocf_assets': float(row['ocf_assets']) if pd.notna(row['ocf_assets']) and not math.isinf(row['ocf_assets']) else None,
            'ocf_ev': float(row['ocf_ev']) if pd.notna(row['ocf_ev']) and not math.isinf(row['ocf_ev']) else None,
            'multiplier': float(row['multiplier']) if pd.notna(row['multiplier']) and not math.isinf(row['multiplier']) else 0,
            'gap': float(row['gap']) if pd.notna(row['gap']) and not math.isinf(row['gap']) else None
        }
        result.append(r)
    return result

def get_hierarchy(df):
    """Get sector and industry level aggregates."""
    if df.empty:
        return [], []
    
    num_cols = ['cash_from_operating_activities_trailing_12_months', 'total_assets_quarterly',
                'enterprise_value', 'total_debt_quarterly']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    sectors = []
    for name, g in df.groupby('sector'):
        s_ocf = g['cash_from_operating_activities_trailing_12_months'].sum()
        s_assets = g['total_assets_quarterly'].sum()
        s_ev = g['enterprise_value'].sum()
        s_debt = g['total_debt_quarterly'].sum()
        sectors.append({
            'name': str(name),
            'ocf': float(s_ocf) if not math.isnan(s_ocf) else 0,
            'assets': float(s_assets) if not math.isnan(s_assets) else 0,
            'ev': float(s_ev) if not math.isnan(s_ev) else 0,
            'debts': float(s_debt) if not math.isnan(s_debt) else 0,
            'ocf_assets': float(s_ocf / s_assets) if s_assets != 0 else 0,
            'ocf_ev': float(s_ocf / s_ev) if s_ev != 0 else 0,
            'multiplier': float((s_ocf / s_assets * 100 + 5)) if s_assets != 0 else 0,
            'gap': float(g['enterprise_value'].sum() / (s_ocf * ((s_ocf/s_assets*100)+5))) if s_assets != 0 and s_ocf != 0 else None
        })
    
    industries = []
    for (s_name, i_name), g in df.groupby(['sector', 'industry']):
        i_ocf = g['cash_from_operating_activities_trailing_12_months'].sum()
        i_assets = g['total_assets_quarterly'].sum()
        i_ev = g['enterprise_value'].sum()
        i_debt = g['total_debt_quarterly'].sum()
        industries.append({
            'name': str(i_name),
            'sector': str(s_name),
            'ocf': float(i_ocf) if not math.isnan(i_ocf) else 0,
            'assets': float(i_assets) if not math.isnan(i_assets) else 0,
            'ev': float(i_ev) if not math.isnan(i_ev) else 0,
            'debts': float(i_debt) if not math.isnan(i_debt) else 0,
            'ocf_assets': float(i_ocf / i_assets) if i_assets != 0 else 0,
            'ocf_ev': float(i_ocf / i_ev) if i_ev != 0 else 0,
            'multiplier': float((i_ocf / i_assets * 100 + 5)) if i_assets != 0 else 0,
            'gap': float(i_ev / (i_ocf * ((i_ocf/i_assets*100)+5))) if i_assets != 0 and i_ocf != 0 else None
        })
    
    return sectors, industries

def is_finance_sector(sector):
    if pd.isna(sector):
        return False
    return str(sector).strip().lower() == 'finance'

def is_us_preferred(symbol):
    if pd.isna(symbol):
        return False
    symbol = str(symbol)
    if '/' in symbol:
        return True
    if symbol.endswith('U') and len(symbol) > 3:
        return True
    if symbol.endswith('P') and len(symbol) > 3:
        return True
    if symbol.endswith('W') and len(symbol) > 3:
        return True
    if '.A' in symbol or '.B' in symbol:
        return True
    return False

def is_hk_southbound(symbol):
    if pd.isna(symbol):
        return False
    symbol = str(symbol)
    return symbol.startswith('8') and len(symbol) == 5

def filter_finance_symbols(df, market_key):
    if df.empty:
        return df
    if market_key == 'us':
        df = df[~df['symbol'].apply(is_us_preferred)]
    elif market_key == 'hk':
        df = df[~df['symbol'].apply(is_hk_southbound)]
    # CN finance: keep all, just dedup by symbol
    return df.drop_duplicates(subset=['symbol'], keep='first')

def calc_finance_companies(df):
    df = df.copy()
    num_cols = [
        'cash_from_operating_activities_trailing_12_months',
        'total_assets_quarterly',
        'total_debt_quarterly',
        'total_equity_quarterly',
        'market_capitalization',
        'enterprise_value'
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['ocf'] = df['cash_from_operating_activities_trailing_12_months']
    df['assets'] = df['total_assets_quarterly']
    df['debts'] = df['total_debt_quarterly']
    df['equity'] = df.get('total_equity_quarterly', 0)
    df['mkt_cap'] = df.get('market_capitalization', 0)
    df['ev'] = df.get('enterprise_value', 0)

    result = []
    for _, row in df.iterrows():
        result.append({
            'symbol': str(row.get('symbol', '')),
            'industry': str(row.get('industry', '')),
            'assets': float(row['assets']) if pd.notna(row['assets']) and not math.isinf(row['assets']) else 0,
            'debts': float(row['debts']) if pd.notna(row['debts']) and not math.isinf(row['debts']) else 0,
            'ocf': float(row['ocf']) if pd.notna(row['ocf']) and not math.isinf(row['ocf']) else 0,
            'equity': float(row['equity']) if pd.notna(row['equity']) and not math.isinf(row['equity']) else 0,
            'mkt_cap': float(row['mkt_cap']) if pd.notna(row['mkt_cap']) and not math.isinf(row['mkt_cap']) else 0,
            'ev': float(row['ev']) if pd.notna(row['ev']) and not math.isinf(row['ev']) else 0
        })
    return result

def calc_finance_industry_summary(df):
    if df.empty:
        return {}
    num_cols = [
        'cash_from_operating_activities_trailing_12_months',
        'total_assets_quarterly',
        'total_debt_quarterly',
        'total_equity_quarterly',
        'enterprise_value'
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    summary = {}
    for name, g in df.groupby('industry'):
        assets = g['total_assets_quarterly'].sum()
        debts = g['total_debt_quarterly'].sum()
        ocf = g['cash_from_operating_activities_trailing_12_months'].sum()
        equity = g['total_equity_quarterly'].sum()
        ev = g['enterprise_value'].sum()
        summary[str(name)] = {
            'assets': float(assets) if not math.isnan(assets) else 0,
            'debts': float(debts) if not math.isnan(debts) else 0,
            'ocf': float(ocf) if not math.isnan(ocf) else 0,
            'equity': float(equity) if not math.isnan(equity) else 0,
            'ev': float(ev) if not math.isnan(ev) else 0,
            'count': int(len(g))
        }
    return summary

def main():
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    # Time series data for overview
    time_series = {'us': {}, 'hk': {}, 'cn': {}}
    
    # Individual market data (latest date)
    market_details = {}
    finance_data = {}
    
    for market_key, market_info in MARKETS.items():
        print(f"🔄 Processing {market_info['label']}...")
        df = fetch_all(supabase, market_info['table'])
        
        if df.empty:
            print(f"  ⚠️ No data for {market_info['label']}")
            continue
        
        # Get all dates
        dates = sorted(df['download_date'].unique())
        print(f"  📅 Dates: {dates}")
        
        # Find companies present in ALL dates (for consistent comparison)
        symbol_sets = []
        for date in dates:
            df_date = df[df['download_date'] == date]
            df_non_fin = df_date[df_date['sector'] != 'Finance']
            symbol_sets.append(set(df_non_fin['symbol'].unique()))
        
        if symbol_sets:
            common_symbols = symbol_sets[0]
            for s in symbol_sets[1:]:
                common_symbols = common_symbols & s
            print(f"  📊 Companies in all dates: {len(common_symbols)}")
        else:
            common_symbols = set()
        
        # Calculate summary for each date (only common companies)
        for date in dates:
            df_date = df[df['download_date'] == date]
            # Filter to only common symbols
            df_date_common = df_date[df_date['symbol'].isin(common_symbols)]
            summary = calc_market_summary(df_date_common)
            if summary:
                summary['company_count'] = len(df_date_common[df_date_common['sector'] != 'Finance'])
                time_series[market_key][date] = summary
        
        # Get latest date data for detail view (all companies, not just common)
        latest_date = dates[-1]
        df_latest = df[df['download_date'] == latest_date].copy()
        
        # Calculate company data with metrics
        companies = calc_company_data(df_latest)
        sectors, industries = get_hierarchy(df_latest)
        
        market_details[market_key] = {
            'date': latest_date,
            'summary': time_series[market_key].get(latest_date, {}),
            'sectors': sectors,
            'industries': industries,
            'companies': companies,
            'common_symbols_count': len(common_symbols)
        }

        # Finance data (all dates, Finance sector only)
        finance_dates = dates
        finance_by_date = {}
        all_finance_industries = set()
        for date in finance_dates:
            df_date = df[df['download_date'] == date].copy()
            df_finance = df_date[df_date['sector'].apply(is_finance_sector)].copy()
            df_finance = filter_finance_symbols(df_finance, market_key)
            if df_finance.empty:
                finance_by_date[date] = {'companies': [], 'industry_summary': {}}
                continue
            finance_companies = calc_finance_companies(df_finance)
            finance_summary = calc_finance_industry_summary(df_finance)
            all_finance_industries.update(finance_summary.keys())
            finance_by_date[date] = {
                'companies': finance_companies,
                'industry_summary': finance_summary
            }

        finance_data[market_key] = {
            'label': market_info['label'],
            'dates': finance_dates,
            'industries': sorted(all_finance_industries),
            'by_date': finance_by_date
        }
    
    # Build output
    output = {
        'time_series': time_series,
        'markets': market_details,
        'finance_data': finance_data,
        'generated_at': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Write JS file
    with open('market_dynamics_data.js', 'w') as f:
        f.write(f"const rawData = {json.dumps(output, indent=2, default=str)};")
    
    print("✅ Data written to market_dynamics_data.js")

if __name__ == "__main__":
    main()
