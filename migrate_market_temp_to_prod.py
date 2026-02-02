#!/usr/bin/env python3
"""
Migrate temp market data into production tables after confirmation.
"""
import argparse
import os
from typing import List

import httpx
from dotenv import load_dotenv


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


def execute_sql(url: str, key: str, sql: str) -> None:
    endpoint = f"{url}/rest/v1/rpc/exec_sql"
    payload = {"query": sql}
    headers = {
        "Content-Type": "application/json",
        "apikey": key,
        "Authorization": f"Bearer {key}",
    }
    resp = httpx.post(endpoint, json=payload, headers=headers, timeout=60.0)
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"SQL 执行失败: {resp.status_code} - {resp.text}")


def build_insert_sql(temp_table: str, prod_table: str, date_value: str) -> str:
    cols = ", ".join(PROD_COLUMNS)
    return f"""
INSERT INTO public.{prod_table} ({cols})
SELECT {cols}
FROM public.{temp_table}
WHERE download_date = '{date_value}';
"""


def build_delete_sql(prod_table: str, date_value: str) -> str:
    return f"DELETE FROM public.{prod_table} WHERE download_date = '{date_value}';"


def build_clear_temp_sql(temp_table: str, date_value: str) -> str:
    return f"DELETE FROM public.{temp_table} WHERE download_date = '{date_value}';"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="迁移日期，如 2026-01-31")
    parser.add_argument("--market", choices=["us", "hk", "cn", "all"], default="all")
    parser.add_argument("--replace", action="store_true", help="先删除生产表中同日期记录")
    parser.add_argument("--clear-temp", action="store_true", help="迁移后清空 temp 表同日期记录")
    args = parser.parse_args()

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("缺少 SUPABASE_URL 或 SUPABASE_SERVICE_ROLE_KEY")

    markets = ["us", "hk", "cn"] if args.market == "all" else [args.market]
    for market_key in markets:
        temp_table = TABLE_MAP[market_key]["temp"]
        prod_table = TABLE_MAP[market_key]["prod"]
        print(f"迁移 {market_key.upper()} {args.date} -> {prod_table}")

        if args.replace:
            print(f"删除 {prod_table} 里 {args.date} 数据")
            execute_sql(url, key, build_delete_sql(prod_table, args.date))

        execute_sql(url, key, build_insert_sql(temp_table, prod_table, args.date))
        print(f"完成插入 {prod_table}")

        if args.clear_temp:
            print(f"清空 {temp_table} 里 {args.date} 数据")
            execute_sql(url, key, build_clear_temp_sql(temp_table, args.date))


if __name__ == "__main__":
    main()
