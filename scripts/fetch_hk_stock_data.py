#!/usr/bin/env python3
"""
Fetch HK stock financial data from Akshare and write to Supabase.
Usage:
    python scripts/fetch_hk_stock_data.py 01211
    python scripts/fetch_hk_stock_data.py 01211.HK
"""
import os
import sys
import datetime as dt
from typing import Optional

import pandas as pd
import akshare as ak
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

TABLES = {
    "balance_sheet": "hk_balance_sheet",
    "income_statement": "hk_income_statement",
    "cash_flow": "hk_cash_flow",
}

REPORT_TYPES = {
    "balance_sheet": "资产负债表",
    "income_statement": "利润表",
    "cash_flow": "现金流量表",
}


def normalize_hk_symbol(raw: str) -> str:
    """01211.HK -> 01211, 1211 -> 01211"""
    s = raw.strip().upper().replace(".HK", "")
    return s.zfill(5)


def fetch_hk_financial(symbol: str, report_type: str) -> pd.DataFrame:
    """
    Fetch HK quarterly_raw financial data from akshare.
    report_type: one of '资产负债表', '利润表', '现金流量表'
    """
    df = ak.stock_financial_hk_report_em(
        stock=symbol,
        symbol=report_type,
        indicator="报告期",
    )
    return df


def prepare_for_supabase(df: pd.DataFrame, table_key: str) -> list:
    """Convert DataFrame rows to list of dicts for Supabase upsert."""
    col_map = {
        "SECUCODE": "secucode",
        "SECURITY_CODE": "security_code",
        "SECURITY_NAME_ABBR": "security_name",
        "ORG_CODE": "org_code",
        "REPORT_DATE": "report_date",
        "DATE_TYPE_CODE": "date_type_code",
        "FISCAL_YEAR": "fiscal_year",
        "START_DATE": "start_date",
        "STD_ITEM_CODE": "std_item_code",
        "STD_ITEM_NAME": "std_item_name",
        "AMOUNT": "amount",
        "STD_REPORT_DATE": "std_report_date",
    }

    rows = []
    for _, r in df.iterrows():
        row = {}
        for src, dst in col_map.items():
            if src in r.index:
                val = r[src]
                if pd.isna(val):
                    val = None
                elif isinstance(val, (pd.Timestamp, dt.datetime, dt.date)):
                    val = val.strftime("%Y-%m-%d") if hasattr(val, "strftime") else str(val)[:10]
                else:
                    val = val if val is not None else None
                row[dst] = val
        if row.get("amount") is not None:
            try:
                row["amount"] = float(row["amount"])
            except (ValueError, TypeError):
                row["amount"] = None
        if row.get("date_type_code") is not None:
            try:
                row["date_type_code"] = int(row["date_type_code"])
            except (ValueError, TypeError):
                row["date_type_code"] = None
        if row.get("fiscal_year") is not None:
            try:
                row["fiscal_year"] = int(row["fiscal_year"])
            except (ValueError, TypeError):
                row["fiscal_year"] = None
        rows.append(row)
    return rows


def upsert_to_supabase(supabase, table: str, rows: list, batch_size: int = 500):
    """Upsert rows into Supabase table using on_conflict on the unique constraint."""
    total = len(rows)
    inserted = 0
    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        result = supabase.table(table).upsert(
            batch,
            on_conflict="secucode,report_date,std_item_code",
        ).execute()
        inserted += len(batch)
        print(f"  {table}: {inserted}/{total} rows upserted")
    return inserted


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/fetch_hk_stock_data.py <HK_SYMBOL>")
        print("Example: python scripts/fetch_hk_stock_data.py 01211")
        sys.exit(1)

    raw_symbol = sys.argv[1]
    symbol = normalize_hk_symbol(raw_symbol)
    print(f"=== Fetching HK stock data for {symbol} ===")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
        sys.exit(1)

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    for key, report_cn in REPORT_TYPES.items():
        table_name = TABLES[key]
        print(f"\n--- {report_cn} -> {table_name} ---")

        try:
            df = fetch_hk_financial(symbol, report_cn)
            print(f"  Downloaded {len(df)} rows from akshare")

            if df.empty:
                print(f"  WARNING: No data returned for {symbol} {report_cn}")
                continue

            rows = prepare_for_supabase(df, key)
            print(f"  Prepared {len(rows)} rows for upsert")

            count = upsert_to_supabase(supabase, table_name, rows)
            print(f"  Done: {count} rows upserted to {table_name}")

        except Exception as e:
            print(f"  ERROR fetching {report_cn}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n=== Completed for {symbol} ===")


if __name__ == "__main__":
    main()
