#!/usr/bin/env python3
import argparse
import json
import os
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd


CHINESE_META_COLUMNS = {"报告日", "数据源", "是否审计", "公告日期", "币种", "类型", "更新日期"}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rename: Dict[str, str] = {}
    for c in df.columns:
        if c in CHINESE_META_COLUMNS:
            continue
        if all(ch.isupper() or ch.isdigit() or ch == "_" for ch in c):
            rename[c] = c.lower()
        else:
            rename[c] = c.lower()
    if rename:
        df = df.rename(columns=rename)
    return df


def normalize_csv_columns(csv_path: str) -> Set[str]:
    df = pd.read_csv(csv_path, dtype=str, low_memory=False, nrows=1)
    df = normalize_columns(df)
    cols = set(df.columns)

    # Map Chinese meta columns to normalized meta fields and drop original Chinese names
    if "报告日" in cols:
        cols.remove("报告日")
        cols.add("report_date")
    if "数据源" in cols:
        cols.remove("数据源")
        cols.add("data_source")
    if "是否审计" in cols:
        cols.remove("是否审计")
        cols.add("is_audited")
    if "公告日期" in cols:
        cols.remove("公告日期")
        cols.add("announcement_date")
    if "币种" in cols:
        cols.remove("币种")
        cols.add("currency")
    if "类型" in cols:
        cols.remove("类型")
        cols.add("report_type")
    if "更新日期" in cols:
        cols.remove("更新日期")
        cols.add("updated_at")

    # Prefer normalized security name if abbreviated name present
    if "security_name_abbr" in cols:
        cols.add("security_name")

    # Always ensure canonical columns exist
    cols.update({"symbol", "report_date"})
    return cols


def can_parse_float(value: str) -> bool:
    if value is None:
        return False
    s = str(value).strip()
    if s == "":
        return False
    try:
        float(s.replace(",", ""))
        return True
    except Exception:
        return False


def infer_column_types(csv_paths: Iterable[str], columns: Iterable[str]) -> Dict[str, str]:
    # Default unknown columns to TEXT, then upgrade to NUMERIC if all non-empty values are numeric
    type_map = {c: "NUMERIC" for c in columns}
    for path in csv_paths:
        df = pd.read_csv(path, dtype=str, low_memory=False)
        df = normalize_columns(df)
        # Attach mapped meta columns
        if "报告日" in df.columns:
            df["report_date"] = df["报告日"]
        if "数据源" in df.columns:
            df["data_source"] = df["数据源"]
        if "是否审计" in df.columns:
            df["is_audited"] = df["是否审计"]
        if "公告日期" in df.columns:
            df["announcement_date"] = df["公告日期"]
        if "币种" in df.columns:
            df["currency"] = df["币种"]
        if "类型" in df.columns:
            df["report_type"] = df["类型"]
        if "更新日期" in df.columns:
            df["updated_at"] = df["更新日期"]
        if "security_name_abbr" in df.columns and "security_name" not in df.columns:
            df["security_name"] = df["security_name_abbr"]
        df["symbol"] = df.get("symbol", "")
        df["report_date"] = df.get("report_date", df.get("报告日", ""))

        for col in columns:
            if type_map[col] == "TEXT":
                continue
            if col not in df.columns:
                continue
            series = df[col]
            for v in series:
                if v is None or str(v).strip() == "":
                    continue
                if not can_parse_float(v):
                    type_map[col] = "TEXT"
                    break
    return type_map


def build_add_column_sql(table: str, col: str, col_type: str) -> str:
    safe_col = re.sub(r"[^a-zA-Z0-9_]", "_", col)
    return f'ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS "{safe_col}" {col_type};'


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", nargs="+", required=True, help="CSV paths for balance sheet 10y")
    parser.add_argument("--output-migration", required=True, help="Output migration SQL path")
    parser.add_argument("--output-report", required=True, help="Output JSON report path")
    parser.add_argument("--table", default="cn_balance_sheet_10y", help="Target table name")
    args = parser.parse_args()

    csv_paths = [p for p in args.csv if os.path.exists(p)]
    if not csv_paths:
        raise FileNotFoundError("No CSV files found.")

    all_cols: Set[str] = set()
    for p in csv_paths:
        all_cols.update(normalize_csv_columns(p))

    type_map = infer_column_types(csv_paths, sorted(all_cols))

    # Override types for meta/identifier columns to keep schema consistent
    fixed_types = {
        "report_date": "DATE",
        "symbol": "TEXT",
        "secucode": "TEXT",
        "security_code": "TEXT",
        "security_name": "TEXT",
        "security_name_abbr": "TEXT",
        "report_date_name": "TEXT",
        "data_source": "TEXT",
        "is_audited": "BOOLEAN",
        "announcement_date": "DATE",
        "currency": "TEXT",
        "report_type": "TEXT",
        "updated_at": "TIMESTAMP",
        "org_code": "TEXT",
        "org_type": "TEXT",
        "security_type_code": "TEXT",
        "listing_state": "TEXT",
        "opinion_type": "TEXT",
        "osopinion_type": "TEXT",
    }
    type_map.update({k: v for k, v in fixed_types.items() if k in all_cols})

    # Ensure stable ordering: metadata first, then others
    meta_first = [
        "report_date",
        "symbol",
        "secucode",
        "security_code",
        "security_name",
        "security_name_abbr",
        "report_date_name",
        "data_source",
        "is_audited",
        "announcement_date",
        "currency",
        "report_type",
        "updated_at",
    ]
    ordered_cols = meta_first + sorted([c for c in all_cols if c not in meta_first])

    sql_lines = [
        "-- Auto-generated: sync cn_balance_sheet_10y columns with CSV",
        f"-- Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    for col in ordered_cols:
        sql_lines.append(build_add_column_sql(args.table, col, type_map.get(col, "TEXT")))

    with open(args.output_migration, "w", encoding="utf-8") as f:
        f.write("\n".join(sql_lines) + "\n")

    report = {
        "table": args.table,
        "columns": ordered_cols,
        "types": {c: type_map.get(c, "TEXT") for c in ordered_cols},
        "csv_paths": csv_paths,
    }
    with open(args.output_report, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Generated migration: {args.output_migration}")
    print(f"Generated report: {args.output_report}")


if __name__ == "__main__":
    main()
