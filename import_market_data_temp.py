#!/usr/bin/env python3
"""
Import 2026-01-31 market CSV data into temp tables, dedup, and compare.
"""
import argparse
import json
import os
import re
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client


CSV_DEFAULTS = {
    "us": "original_data_csv/fr_trading_view/US/Finance_Analysis_us_2026-01-31.csv",
    "hk": "original_data_csv/fr_trading_view/HKSE/Finance_Analysis_hk_2026-01-31.csv",
    "cn": "original_data_csv/fr_trading_view/China/Finance_Analysis_cn_2026-01-31.csv",
}

TABLE_MAP = {
    "us": {"temp": "us_market_temp", "prod": "us_market", "pad": None},
    "hk": {"temp": "hkse_market_temp", "prod": "hkse_market", "pad": 5},
    "cn": {"temp": "share_a_market_temp", "prod": "share_a_market", "pad": 6},
}

EXPECTED_COLUMNS = [
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
    "total_liabilities_quarterly",
    "total_liabilities_quarterly_currency",
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
    "analyst_rating",
    "download_date",
]


def extract_date(path: str) -> str:
    match = re.search(r"\d{4}-\d{2}-\d{2}", path)
    if not match:
        raise ValueError(f"无法从文件名提取日期: {path}")
    return match.group(0)


def normalize_column(name: str) -> str:
    cleaned = name.strip().replace("%", "percent")
    cleaned = re.sub(r"[^\w]+", "_", cleaned.lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned


def normalize_columns(columns: List[str]) -> List[str]:
    normalized = []
    seen = {}
    for col in columns:
        suffix = None
        if re.search(r"\.\d+$", col):
            base, dot_suffix = col.rsplit(".", 1)
            suffix = dot_suffix
        else:
            base = col
        name = normalize_column(base)
        if suffix is not None:
            name = f"{name}_{suffix}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        normalized.append(name)
    return normalized


def standardize_symbol(series: pd.Series, pad: Optional[int]) -> pd.Series:
    symbols = series.fillna("").astype(str).str.strip()
    if pad:
        symbols = symbols.str.zfill(pad)
    return symbols


def load_csv(path: str, market_key: str) -> Tuple[pd.DataFrame, str]:
    df = pd.read_csv(path)
    df.columns = normalize_columns(df.columns.tolist())
    download_date = extract_date(path)
    df["download_date"] = download_date
    pad = TABLE_MAP[market_key]["pad"]
    df["symbol"] = standardize_symbol(df["symbol"], pad)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notna(df), None)
    return df, download_date


def filter_columns(df: pd.DataFrame) -> pd.DataFrame:
    available = [c for c in EXPECTED_COLUMNS if c in df.columns]
    return df[available].copy()


# 需要排除的交易所 (US)
EXCLUDED_EXCHANGES = {"OTC", "NYSE Arca", "CBOE"}


def filter_exchanges(df: pd.DataFrame, market_key: str) -> pd.DataFrame:
    """过滤掉不需要的数据"""
    before = len(df)
    
    if market_key == "us":
        # US: 过滤 OTC、NYSE Arca、CBOE
        if "exchange" in df.columns:
            df = df[~df["exchange"].isin(EXCLUDED_EXCHANGES)]
            after = len(df)
            if before != after:
                print(f"  过滤交易所 (OTC/NYSE Arca/CBOE): {before} -> {after} (移除 {before - after} 条)")
    
    elif market_key == "hk":
        # HK: 过滤 80 开头的沪港通代码
        if "symbol" in df.columns:
            df = df[~df["symbol"].astype(str).str.startswith("8")]
            after = len(df)
            if before != after:
                print(f"  过滤沪港通代码 (80xxx): {before} -> {after} (移除 {before - after} 条)")
    
    elif market_key == "cn":
        # CN: 保留所有数据（包括 B股、科创板）
        pass
    
    return df


def clear_temp_table(supabase, table: str, download_date: str) -> None:
    supabase.table(table).delete().eq("download_date", download_date).execute()


def clean_record(record: Dict) -> Dict:
    """Clean a record for JSON serialization."""
    import math
    cleaned = {}
    for k, v in record.items():
        if v is None:
            cleaned[k] = None
        elif isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                cleaned[k] = None
            else:
                cleaned[k] = v
        elif isinstance(v, np.floating):
            if np.isnan(v) or np.isinf(v):
                cleaned[k] = None
            else:
                cleaned[k] = float(v)
        elif isinstance(v, np.integer):
            cleaned[k] = int(v)
        else:
            cleaned[k] = v
    return cleaned


def batch_insert(supabase, table: str, records: List[Dict], batch_size: int = 500) -> int:
    inserted = 0
    for i in range(0, len(records), batch_size):
        batch = [clean_record(r) for r in records[i:i + batch_size]]
        res = supabase.table(table).insert(batch).execute()
        if res.data is None:
            raise RuntimeError(f"{table} 插入失败: {res}")
        inserted += len(batch)
        print(f"{table}: 已插入 {inserted}/{len(records)}")
    return inserted


def is_us_preferred(symbol: str) -> bool:
    if "/" in symbol:
        return True
    if symbol.endswith("U") and len(symbol) > 3:
        return True
    if symbol.endswith("P") and len(symbol) > 3:
        return True
    if symbol.endswith("W") and len(symbol) > 3:
        return True
    if ".A" in symbol or ".B" in symbol:
        return True
    return False


def is_us_non_common_security(symbol: str, description: str) -> bool:
    if is_us_preferred(symbol):
        return True
    desc = str(description or "").lower()
    keywords = [
        "preferred", "depositary", "trust preferred", "capital trust", "trust",
        "notes", "note due", "subordinated", "senior", "perpetual", "debenture",
        "warrant", "unit", "units", "certificate", "certificates",
        "fixed rate", "floating rate", "fixed-to-floating", "cumulative",
        "series "
    ]
    return any(k in desc for k in keywords)


def should_drop_us_finance_row(row: pd.Series) -> bool:
    if str(row.get("sector", "")).strip().lower() != "finance":
        return False
    if is_us_non_common_security(row.get("symbol", ""), row.get("description", "")):
        return True
    metrics = [
        row.get("enterprise_value"),
        row.get("cash_from_operating_activities_trailing_12_months"),
        row.get("total_equity_quarterly"),
        row.get("total_assets_quarterly"),
        row.get("total_debt_quarterly"),
    ]
    return all(pd.isna(m) or m == 0 for m in metrics)


def is_hk_southbound(symbol: str) -> bool:
    return symbol.startswith("8") and len(symbol) == 5


def find_duplicates_by_metrics(df: pd.DataFrame, is_preferred_fn) -> Tuple[set, List[Dict]]:
    df = df.copy()
    df["ev"] = pd.to_numeric(df["enterprise_value"], errors="coerce")
    df["ocf"] = pd.to_numeric(df["cash_from_operating_activities_trailing_12_months"], errors="coerce")
    df["equity"] = pd.to_numeric(df["total_equity_quarterly"], errors="coerce")

    df_sorted = df.sort_values(["ev", "ocf", "equity"]).reset_index(drop=True)
    to_delete = set()
    duplicates_found = []

    for i in range(len(df_sorted) - 1):
        row1 = df_sorted.iloc[i]
        row2 = df_sorted.iloc[i + 1]

        if row1["id"] in to_delete or row2["id"] in to_delete:
            continue
        if pd.isna(row1["ev"]) or pd.isna(row2["ev"]) or row1["ev"] == 0 or row2["ev"] == 0:
            continue
        if pd.isna(row1["equity"]) or pd.isna(row2["equity"]) or row1["equity"] == 0 or row2["equity"] == 0:
            continue

        ev_sim = min(row1["ev"], row2["ev"]) / max(row1["ev"], row2["ev"])
        equity_sim = min(abs(row1["equity"]), abs(row2["equity"])) / max(abs(row1["equity"]), abs(row2["equity"]))

        if pd.isna(row1["ocf"]) or pd.isna(row2["ocf"]):
            ocf_similar = pd.isna(row1["ocf"]) and pd.isna(row2["ocf"])
        elif row1["ocf"] == row2["ocf"]:
            ocf_similar = True
        elif max(abs(row1["ocf"]), abs(row2["ocf"])) > 0:
            ocf_diff = abs(row1["ocf"] - row2["ocf"]) / max(abs(row1["ocf"]), abs(row2["ocf"]))
            ocf_similar = ocf_diff < 0.005
        else:
            ocf_similar = True

        if ev_sim > 0.995 and equity_sim > 0.995 and ocf_similar:
            pref1 = is_preferred_fn(row1["symbol"])
            pref2 = is_preferred_fn(row2["symbol"])
            delete_id = None
            if pref1 and not pref2:
                delete_id = row1["id"]
                duplicates_found.append({"keep": row2["symbol"], "delete": row1["symbol"], "ocf": row1["ocf"]})
            elif pref2 and not pref1:
                delete_id = row2["id"]
                duplicates_found.append({"keep": row1["symbol"], "delete": row2["symbol"], "ocf": row1["ocf"]})
            elif len(row1["symbol"]) > len(row2["symbol"]):
                delete_id = row1["id"]
                duplicates_found.append({"keep": row2["symbol"], "delete": row1["symbol"], "ocf": row1["ocf"]})
            elif len(row2["symbol"]) > len(row1["symbol"]):
                delete_id = row2["id"]
                duplicates_found.append({"keep": row1["symbol"], "delete": row2["symbol"], "ocf": row1["ocf"]})

            if delete_id:
                to_delete.add(delete_id)

    return to_delete, duplicates_found


def fetch_all_for_date(supabase, table: str, date: str, fields: str) -> pd.DataFrame:
    data = []
    offset = 0
    while True:
        res = supabase.table(table).select(fields).eq("download_date", date).range(offset, offset + 999).execute()
        if not res.data:
            break
        data.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000
    return pd.DataFrame(data)


def delete_records(supabase, table: str, ids: List[int]) -> None:
    for i in range(0, len(ids), 100):
        batch = ids[i:i + 100]
        supabase.table(table).delete().in_("id", batch).execute()


def dedup_temp_tables(supabase, market_key: str, date: str) -> Dict:
    table = TABLE_MAP[market_key]["temp"]
    df = fetch_all_for_date(
        supabase,
        table,
        date,
        "id,symbol,description,sector,enterprise_value,cash_from_operating_activities_trailing_12_months,"
        "total_equity_quarterly,total_assets_quarterly,total_debt_quarterly,market_capitalization",
    )
    if df.empty:
        return {"deleted": 0, "duplicates": []}

    to_delete = set()
    duplicates = []

    if market_key == "us":
        finance_drop_ids = set(df[df.apply(should_drop_us_finance_row, axis=1)]["id"].tolist())
        metric_delete, metric_dups = find_duplicates_by_metrics(df, is_us_preferred)
        to_delete = set(metric_delete) | finance_drop_ids
        duplicates = metric_dups
    elif market_key == "hk":
        metric_delete, metric_dups = find_duplicates_by_metrics(df, is_hk_southbound)
        to_delete = set(metric_delete)
        duplicates = metric_dups
    else:
        deduped = df.drop_duplicates(subset=["symbol"], keep="first")
        to_delete = set(df["id"]) - set(deduped["id"])

    if to_delete:
        delete_records(supabase, table, list(to_delete))

    return {"deleted": len(to_delete), "duplicates": duplicates}


def fetch_symbols(supabase, table: str, date: str) -> List[str]:
    symbols = []
    offset = 0
    while True:
        res = supabase.table(table).select("symbol").eq("download_date", date).range(offset, offset + 999).execute()
        if not res.data:
            break
        symbols.extend([r.get("symbol") for r in res.data if r.get("symbol") is not None])
        if len(res.data) < 1000:
            break
        offset += 1000
    return symbols


def fetch_latest_date(supabase, table: str) -> Optional[str]:
    res = supabase.table(table).select("download_date").order("download_date", desc=True).limit(1).execute()
    if not res.data:
        return None
    return res.data[0].get("download_date")


def compare_temp_vs_prod(supabase, temp_date: str, output_path: str) -> Dict:
    report = {"temp_date": temp_date, "markets": {}}
    for market_key, config in TABLE_MAP.items():
        temp_table = config["temp"]
        prod_table = config["prod"]
        prod_date = fetch_latest_date(supabase, prod_table)
        if not prod_date:
            report["markets"][market_key] = {"error": "未找到生产表日期"}
            continue

        temp_symbols = fetch_symbols(supabase, temp_table, temp_date)
        prod_symbols = fetch_symbols(supabase, prod_table, prod_date)

        temp_set = set(temp_symbols)
        prod_set = set(prod_symbols)
        new_symbols = sorted(list(temp_set - prod_set))
        missing_symbols = sorted(list(prod_set - temp_set))

        report["markets"][market_key] = {
            "temp_table": temp_table,
            "prod_table": prod_table,
            "prod_latest_date": prod_date,
            "temp_records": len(temp_symbols),
            "prod_records": len(prod_symbols),
            "temp_unique_symbols": len(temp_set),
            "prod_unique_symbols": len(prod_set),
            "new_symbols": new_symbols,
            "missing_symbols": missing_symbols,
        }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    return report


def write_markdown_report(report: Dict, output_path: str) -> None:
    lines = [f"# 市场数据对比报告 ({report['temp_date']})", ""]
    for market_key, data in report["markets"].items():
        lines.append(f"## {market_key.upper()} Market")
        if "error" in data:
            lines.append(f"- 错误: {data['error']}")
            lines.append("")
            continue
        lines.append(f"- Temp 表: `{data['temp_table']}`")
        lines.append(f"- 正式表: `{data['prod_table']}`")
        lines.append(f"- 正式表最新日期: {data['prod_latest_date']}")
        lines.append(f"- Temp 记录数: {data['temp_records']} (唯一股票 {data['temp_unique_symbols']})")
        lines.append(f"- 正式记录数: {data['prod_records']} (唯一股票 {data['prod_unique_symbols']})")
        lines.append(f"- 新增股票数: {len(data['new_symbols'])}")
        lines.append(f"- 缺失股票数: {len(data['missing_symbols'])}")
        if data["new_symbols"]:
            lines.append(f"- 新增股票: {', '.join(data['new_symbols'][:50])}")
            if len(data["new_symbols"]) > 50:
                lines.append(f"- 新增股票(后续): 还剩 {len(data['new_symbols']) - 50} 条")
        if data["missing_symbols"]:
            lines.append(f"- 缺失股票: {', '.join(data['missing_symbols'][:50])}")
            if len(data["missing_symbols"]) > 50:
                lines.append(f"- 缺失股票(后续): 还剩 {len(data['missing_symbols']) - 50} 条")
        lines.append("")
    with open(output_path, "w") as f:
        f.write("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", choices=["us", "hk", "cn", "all"], default="all")
    parser.add_argument("--truncate", action="store_true")
    parser.add_argument("--dedup", action="store_true")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--report-json", default="outputs/market_temp_compare_2026-01-31.json")
    parser.add_argument("--report-md", default="outputs/market_temp_compare_2026-01-31.md")
    args = parser.parse_args()

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("缺少 SUPABASE_URL 或 SUPABASE_SERVICE_ROLE_KEY")

    supabase = create_client(url, key)

    markets = ["us", "hk", "cn"] if args.market == "all" else [args.market]
    temp_date = None

    for market_key in markets:
        csv_path = CSV_DEFAULTS[market_key]
        table = TABLE_MAP[market_key]["temp"]
        print(f"处理 {market_key.upper()} CSV: {csv_path}")

        df, download_date = load_csv(csv_path, market_key)
        temp_date = download_date
        df = filter_columns(df)
        df = filter_exchanges(df, market_key)

        if args.truncate:
            print(f"清空 {table} (仅 {download_date})")
            clear_temp_table(supabase, table, download_date)

        records = df.to_dict("records")
        print(f"{table}: 准备插入 {len(records)} 条")
        batch_insert(supabase, table, records)

        if args.dedup:
            result = dedup_temp_tables(supabase, market_key, download_date)
            print(f"{table}: 去重删除 {result['deleted']} 条")

    if args.compare and temp_date:
        report = compare_temp_vs_prod(supabase, temp_date, args.report_json)
        write_markdown_report(report, args.report_md)
        print(f"对比报告已生成: {args.report_json}, {args.report_md}")


if __name__ == "__main__":
    main()
