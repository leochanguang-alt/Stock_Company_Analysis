#!/usr/bin/env python3
"""
Migrate temp market data into production tables after confirmation.
"""
import argparse
import os
from typing import List

from dotenv import load_dotenv
from supabase import create_client


TABLE_MAP = {
    "us": {"temp": "us_market_temp", "prod": "us_market"},
    "hk": {"temp": "hkse_market_temp", "prod": "hkse_market"},
    "cn": {"temp": "share_a_market_temp", "prod": "share_a_market"},
}

PROD_COLUMNS = [
    "symbol",
    "description",
    "enterprise_value",
    "enterprise_value_currency",
    "market_capitalization",
    "market_capitalization_currency",
    "total_debt_quarterly",
    "total_debt_quarterly_currency",
    "total_equity_quarterly",
    "total_equity_quarterly_currency",
    "total_assets_quarterly",
    "total_assets_quarterly_currency",
    "beta_5_years",
    "cash_from_operating_activities_trailing_12_months",
    "cash_from_operating_activities_trailing_12_months_currency",
    "cash_from_financing_activities_trailing_12_months",
    "cash_from_financing_activities_trailing_12_months_currency",
    "total_cash_dividends_paid_annual",
    "total_cash_dividends_paid_annual_currency",
    "industry",
    "sector",
    "exchange",
    "index",
    "beta_5_years_1",
    "beta_1_year",
    "simple_moving_average_120_1_day",
    "exponential_moving_average_120_1_day",
    "return_on_invested_capital_percent_trailing_12_months",
    "download_date",
]


def delete_by_date(supabase, table: str, date_value: str) -> None:
    supabase.table(table).delete().eq("download_date", date_value).execute()


def fetch_temp_rows(supabase, table: str, date_value: str) -> List[dict]:
    rows: List[dict] = []
    offset = 0
    while True:
        res = (
            supabase.table(table)
            .select(",".join(PROD_COLUMNS))
            .eq("download_date", date_value)
            .range(offset, offset + 999)
            .execute()
        )
        if not res.data:
            break
        rows.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000
    return rows


def insert_batches(supabase, table: str, rows: List[dict], batch_size: int = 500) -> None:
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        res = supabase.table(table).insert(batch).execute()
        if res.data is None:
            raise RuntimeError(f"{table} 插入失败: {res}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="迁移日期，如 2026-02-28")
    parser.add_argument("--market", choices=["us", "hk", "cn", "all"], default="all")
    parser.add_argument("--replace", action="store_true", help="先删除生产表中同日期记录")
    parser.add_argument("--clear-temp", action="store_true", help="迁移后清空 temp 表同日期记录")
    args = parser.parse_args()

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("缺少 SUPABASE_URL 或 SUPABASE_SERVICE_ROLE_KEY")

    supabase = create_client(url, key)
    markets = ["us", "hk", "cn"] if args.market == "all" else [args.market]
    for market_key in markets:
        temp_table = TABLE_MAP[market_key]["temp"]
        prod_table = TABLE_MAP[market_key]["prod"]
        print(f"迁移 {market_key.upper()} {args.date} -> {prod_table}")

        if args.replace:
            print(f"删除 {prod_table} 里 {args.date} 数据")
            delete_by_date(supabase, prod_table, args.date)

        rows = fetch_temp_rows(supabase, temp_table, args.date)
        if not rows:
            print(f"{temp_table} 没有 {args.date} 数据，跳过")
            continue

        insert_batches(supabase, prod_table, rows)
        print(f"完成插入 {prod_table} ({len(rows)} 条)")

        if args.clear_temp:
            print(f"清空 {temp_table} 里 {args.date} 数据")
            delete_by_date(supabase, temp_table, args.date)


if __name__ == "__main__":
    main()
