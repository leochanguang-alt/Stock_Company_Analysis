#!/usr/bin/env python3
"""
Export company_list + latest share_a_market metrics to JSON for UI search.
"""

import json
import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client


def fetch_all_for_date(client, table, date_str, columns):
    data = []
    offset = 0
    limit = 1000
    while True:
        res = (
            client.table(table)
            .select(columns)
            .eq("download_date", date_str)
            .range(offset, offset + limit - 1)
            .execute()
        )
        if not res.data:
            break
        data.extend(res.data)
        if len(res.data) < limit:
            break
        offset += limit
    return pd.DataFrame(data)


def fetch_all(client, table, columns):
    data = []
    offset = 0
    limit = 1000
    while True:
        res = (
            client.table(table)
            .select(columns)
            .range(offset, offset + limit - 1)
            .execute()
        )
        if not res.data:
            break
        data.extend(res.data)
        if len(res.data) < limit:
            break
        offset += limit
    return pd.DataFrame(data)


def main():
    load_dotenv(dotenv_path=".env")
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set")

    client = create_client(url, key)

    # Latest download_date in share_a_market
    latest = (
        client.table("share_a_market")
        .select("download_date")
        .order("download_date", desc=True)
        .limit(1)
        .execute()
    )
    if not latest.data:
        raise RuntimeError("share_a_market has no data")

    latest_date = latest.data[0]["download_date"]

    # company_list for CN
    company_df = fetch_all(
        client,
        "company_list",
        "symbol,market,description,sector,industry,exchange",
    )
    if not company_df.empty:
        company_df = company_df[company_df["market"] == "cn"].copy()

    # share_a_market metrics for latest date
    market_cols = (
        "symbol,enterprise_value,market_capitalization,beta_5_years,beta_1_year,"
        "cash_from_operating_activities_trailing_12_months,"
        "\"return_on_invested_capital_%_trailing_12_months\",download_date"
    )
    market_df = fetch_all_for_date(client, "share_a_market", latest_date, market_cols)

    if company_df.empty:
        company_df = pd.DataFrame(columns=["symbol", "market", "description", "sector", "industry", "exchange"])

    if market_df.empty:
        market_df = pd.DataFrame(columns=[
            "symbol",
            "enterprise_value",
            "market_capitalization",
            "beta_5_years",
            "beta_1_year",
            "cash_from_operating_activities_trailing_12_months",
            "return_on_invested_capital_%_trailing_12_months",
            "download_date",
        ])

    # Normalize symbols (pad CN symbols to 6 digits when numeric)
    def normalize_symbol(val):
        s = str(val or "").strip()
        if s.isdigit() and len(s) < 6:
            return s.zfill(6)
        return s

    if not company_df.empty:
        company_df["symbol"] = company_df["symbol"].apply(normalize_symbol)
    if not market_df.empty:
        market_df["symbol"] = market_df["symbol"].apply(normalize_symbol)

    # Merge, keeping all market rows even if company_list missing
    merged = pd.merge(market_df, company_df, on="symbol", how="left")
    if merged.empty and not company_df.empty:
        merged = company_df.copy()

    def to_float(val):
        try:
            return float(val)
        except Exception:
            return None

    records = []
    for _, row in merged.iterrows():
        records.append({
            "symbol": str(row.get("symbol") or ""),
            "market": row.get("market"),
            "description": row.get("description"),
            "sector": row.get("sector"),
            "industry": row.get("industry"),
            "exchange": row.get("exchange"),
            "enterprise_value": to_float(row.get("enterprise_value")),
            "market_capitalization": to_float(row.get("market_capitalization")),
            "beta_5_years": to_float(row.get("beta_5_years")),
            "beta_1_year": to_float(row.get("beta_1_year")),
            "ocf_ttm": to_float(row.get("cash_from_operating_activities_trailing_12_months")),
            "roic_ttm_pct": to_float(row.get("return_on_invested_capital_%_trailing_12_months")),
            "download_date": row.get("download_date"),
        })

    os.makedirs("outputs", exist_ok=True)
    out_path = "outputs/company_lookup_cn.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.utcnow().isoformat(),
            "latest_download_date": latest_date,
            "records": records,
        }, f, ensure_ascii=False)

    print(f"Wrote {len(records)} records to {out_path}")


if __name__ == "__main__":
    main()
