#!/usr/bin/env python3
import os
import sys
import math
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client


def parse_args(argv: List[str]) -> Dict[str, str]:
    symbol = "002508"
    years = "20"
    for i, a in enumerate(argv):
        if a == "--symbol" and i + 1 < len(argv):
            symbol = argv[i + 1].strip()
        if a.startswith("--symbol="):
            symbol = a.split("=", 1)[1].strip()
        if a == "--years" and i + 1 < len(argv):
            years = argv[i + 1].strip()
        if a.startswith("--years="):
            years = a.split("=", 1)[1].strip()
    return {"symbol": symbol, "years": years}


def normalize_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if s.endswith(".SZ") or s.endswith(".SH") or s.endswith(".SS"):
        s = s.split(".")[0]
    if s.startswith(("SZ", "SH", "SS")):
        s = s[2:]
    if s.isdigit():
        s = s.zfill(6)
    return s


def ymd8_to_date(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    s = str(val).strip()
    if s.isdigit() and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    # already ISO
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        return None


def clean_value(v: Any) -> Any:
    try:
        import numpy as np
        if isinstance(v, (np.floating, np.integer)):
            v = float(v) if isinstance(v, np.floating) else int(v)
    except Exception:
        pass

    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    return v


def chunked(items: List[Dict[str, Any]], size: int = 500) -> List[List[Dict[str, Any]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def df_from_csv(path: str, nrows: Optional[int] = None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    # keep original column names (codes), but normalize later
    encodings = ["utf-8-sig", "utf-8", "gbk"]
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            return pd.read_csv(path, dtype=str, low_memory=False, nrows=nrows, encoding=enc)
        except UnicodeDecodeError as err:
            last_err = err
            continue
    if last_err:
        raise last_err
    return pd.read_csv(path, dtype=str, low_memory=False, nrows=nrows)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # strip col names
    df.columns = [str(c).strip() for c in df.columns]
    # lower-case code columns; keep Chinese meta columns as-is
    rename = {}
    for c in df.columns:
        if c in ("报告日", "数据源", "是否审计", "公告日期", "币种", "类型", "更新日期"):
            continue
        # for codes like TOTAL_ASSETS -> total_assets
        if all(ch.isupper() or ch.isdigit() or ch == "_" for ch in c):
            rename[c] = c.lower()
        else:
            # also normalize common meta codes
            rename[c] = c.lower()
    if rename:
        df = df.rename(columns=rename)
    return df


def get_balance_sheet_keep_cols(csv_path: str) -> List[str]:
    df = df_from_csv(csv_path, nrows=1)
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
    cols.update({"report_date", "symbol"})
    return sorted(cols)


def get_cash_flow_keep_cols(csv_path: str) -> List[str]:
    df = df_from_csv(csv_path, nrows=1)
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
    cols.update({"report_date", "symbol"})
    return sorted(cols)


def get_income_statement_keep_cols(csv_path: str) -> List[str]:
    df = df_from_csv(csv_path, nrows=1)
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
    cols.update({"report_date", "symbol"})
    return sorted(cols)


def to_records_financial(df: pd.DataFrame, symbol: str, keep_cols: List[str]) -> List[Dict[str, Any]]:
    df = normalize_columns(df)
    # report date
    if "报告日" in df.columns:
        df["report_date"] = df["报告日"].apply(ymd8_to_date)
    elif "report_date" in df.columns:
        df["report_date"] = df["report_date"].apply(ymd8_to_date)
    else:
        def guess_report_date_col(frame: pd.DataFrame) -> Optional[str]:
            for col in frame.columns:
                vals = frame[col].dropna().astype(str).head(5)
                if vals.empty:
                    continue
                ok = True
                for v in vals:
                    s = v.strip()
                    if not (re.match(r"^\d{8}$", s) or re.match(r"^\d{4}-\d{2}-\d{2}", s)):
                        ok = False
                        break
                if ok:
                    return col
            return None

        guessed = guess_report_date_col(df)
        if guessed:
            df["report_date"] = df[guessed].apply(ymd8_to_date)
        else:
            raise ValueError("missing report date column")

    df["symbol"] = symbol

    # Map common meta fields
    if "secucode" not in df.columns and "secucode" in keep_cols:
        if "secucode" in df.columns:
            pass
        elif "secucode" not in df.columns and "secucode" in df.columns:
            pass
    if "security_name" in keep_cols and "security_name" not in df.columns:
        if "security_name_abbr" in df.columns:
            df["security_name"] = df["security_name_abbr"]
    if "report_date_name" in keep_cols and "report_date_name" not in df.columns:
        if "report_date_name" in df.columns:
            pass

    # Chinese meta columns to expected meta fields
    if "data_source" in keep_cols and "data_source" not in df.columns:
        if "数据源" in df.columns:
            df["data_source"] = df["数据源"]
        else:
            df["data_source"] = "EastMoney"
    if "currency" in keep_cols and "currency" not in df.columns:
        if "币种" in df.columns:
            df["currency"] = df["币种"]
        else:
            df["currency"] = "CNY"
    if "report_type" in keep_cols and "report_type" not in df.columns:
        if "类型" in df.columns:
            df["report_type"] = df["类型"]
    if "announcement_date" in keep_cols and "announcement_date" not in df.columns:
        if "公告日期" in df.columns:
            df["announcement_date"] = df["公告日期"].apply(ymd8_to_date)
    if "updated_at" in keep_cols and "updated_at" not in df.columns:
        if "更新日期" in df.columns:
            df["updated_at"] = df["更新日期"].apply(ymd8_to_date)

    df = df[df["report_date"].notna()]

    # Keep only allowed cols
    cols = [c for c in keep_cols if c in df.columns]
    # Drop columns with invalid names (garbled headers)
    cols = [c for c in cols if re.match(r"^[a-z0-9_]+$", str(c))]

    # Decide scaling:
    # - AKShare/EastMoney financial amount fields are in 元
    # - Our cn_* wide tables store amounts in 亿元 (for analysis pages)
    # Heuristic: scale all numeric "amount-like" columns in `cols`, except:
    # - meta fields
    # - EPS / per-share values
    # - ratio / pct / yoy / change fields
    meta_cols = {
        "report_date",
        "symbol",
        "secucode",
        "security_code",
        "security_name",
        "security_name_abbr",
        "report_date_name",
        "data_source",
        "announcement_date",
        "currency",
        "report_type",
        "opinion_type",
        "osopinion_type",
        "updated_at",
        "is_audited",
        "org_code",
        "org_type",
        "security_type_code",
        "listing_state",
    }

    def should_scale(col: str) -> bool:
        if col in meta_cols:
            return False
        if col in ("basic_eps", "diluted_eps"):
            return False
        c = col.lower()
        if "per_share" in c or c.endswith("_ps"):
            return False
        if "ratio" in c or "pct" in c:
            return False
        if c.endswith("_yoy") or c.endswith("_change") or c.endswith("_chg"):
            return False
        if "code" in c or c.endswith("_code") or c.endswith("_state"):
            return False
        if "name" in c:
            return False
        # counts / ranks etc should not be scaled
        # NOTE: don't use naive substring checks because "accounts_*" contains "count"
        if c == "count" or c.endswith("_count") or "_count_" in c:
            return False
        if c == "rank" or c.endswith("_rank") or "_rank_" in c:
            return False
        return True

    scale_cols = {c for c in cols if should_scale(c)}

    def to_float(x):
        try:
            if x is None:
                return None
            s = str(x).strip()
            if s == "":
                return None
            return float(s.replace(",", ""))
        except Exception:
            return None

    out: List[Dict[str, Any]] = []
    for _, r in df[cols].iterrows():
        rec: Dict[str, Any] = {}
        for k in cols:
            v = r.get(k)
            if k in scale_cols:
                n = to_float(v)
                rec[k] = None if n is None else clean_value(n / 1e8)  # 元 -> 亿元
            elif k in ("basic_eps", "diluted_eps"):
                rec[k] = clean_value(to_float(v))
            else:
                # keep as string/number
                rec[k] = clean_value(v)
        out.append(rec)
    return out


def to_records_mkt_cap(df: pd.DataFrame, symbol: str) -> List[Dict[str, Any]]:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    # expected: date, mkt_cap_billion_cny
    if "date" not in df.columns or "mkt_cap_billion_cny" not in df.columns:
        raise ValueError("mkt cap csv must have date,mkt_cap_billion_cny")
    df["trade_date"] = df["date"].apply(ymd8_to_date)
    df["symbol"] = symbol
    df["mkt_cap_billion_cny"] = pd.to_numeric(df["mkt_cap_billion_cny"], errors="coerce")
    df = df[df["trade_date"].notna() & df["mkt_cap_billion_cny"].notna()]
    out = []
    for _, r in df[["symbol", "trade_date", "mkt_cap_billion_cny"]].iterrows():
        out.append(
            {
                "symbol": symbol,
                "trade_date": r["trade_date"],
                "mkt_cap_billion_cny": clean_value(float(r["mkt_cap_billion_cny"])),
            }
        )
    return out


def upsert_batches(client, table: str, records: List[Dict[str, Any]], on_conflict: str):
    if not records:
        print(f"  - {table}: 0 rows")
        return
    for batch in chunked(records, 500):
        resp = client.table(table).upsert(batch, on_conflict=on_conflict).execute()
        if getattr(resp, "error", None):
            raise RuntimeError(f"{table} upsert error: {resp.error}")
    print(f"  - {table}: {len(records)} rows")


def to_records_top10(path: str, symbol: str) -> List[Dict[str, Any]]:
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = [str(c).strip() for c in df.columns]
    # Expected columns from fetch_stock_data.py: 名次,股东名称,股份类型,持股数,占总股本持股比例,增减,变动比率,报告期,股票代码
    for c in ["名次", "股东名称", "报告期"]:
        if c not in df.columns:
            raise ValueError(f"top10 csv missing column: {c}")

    out = []
    for _, r in df.iterrows():
        report_date = ymd8_to_date(r.get("报告期"))
        if not report_date:
            continue
        rank = r.get("名次")
        if rank is None or str(rank).strip() == "":
            continue
        try:
            rank_i = int(float(str(rank)))
        except Exception:
            continue

        def num(x):
            try:
                if x is None or str(x).strip() == "":
                    return None
                return float(str(x).replace(",", ""))
            except Exception:
                return None

        shares = num(r.get("持股数"))
        hold_ratio = num(r.get("占总股本持股比例"))
        change_num = num(r.get("增减"))
        change_ratio = num(r.get("变动比率"))

        out.append(
            {
                "symbol": symbol,
                "report_date": report_date,
                "rank": rank_i,
                "shareholder_name": clean_value(r.get("股东名称")),
                "share_type": clean_value(r.get("股份类型")),
                "hold_num": clean_value(shares),
                "hold_ratio": clean_value(hold_ratio),
                "change_num": clean_value(change_num),
                "change_ratio": clean_value(change_ratio),
            }
        )
    return out


def to_records_holder_count(path: str, symbol: str) -> List[Dict[str, Any]]:
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = [str(c).strip() for c in df.columns]
    # Expected columns: 证券代码,证券简称,变动日期,本期股东人数,上期股东人数,股东人数增幅,本期人均持股数量,上期人均持股数量,人均持股数量增幅
    for c in ["变动日期", "本期股东人数"]:
        if c not in df.columns:
            raise ValueError(f"holder count csv missing column: {c}")

    def num(x):
        try:
            if x is None or str(x).strip() == "":
                return None
            return float(str(x).replace(",", ""))
        except Exception:
            return None

    out = []
    for _, r in df.iterrows():
        report_date = ymd8_to_date(r.get("变动日期"))
        if not report_date:
            continue
        out.append(
            {
                "symbol": symbol,
                "security_name": clean_value(r.get("证券简称") or r.get("name")),
                "report_date": report_date,
                "holder_count": clean_value(num(r.get("本期股东人数"))),
                "holder_count_prev": clean_value(num(r.get("上期股东人数"))),
                "holder_count_change": clean_value(num(r.get("股东人数增幅"))),
                "avg_hold_num": clean_value(num(r.get("本期人均持股数量"))),
                "avg_hold_num_prev": clean_value(num(r.get("上期人均持股数量"))),
                "avg_hold_num_change": clean_value(num(r.get("人均持股数量增幅"))),
            }
        )
    return out


def main():
    args = parse_args(sys.argv[1:])
    symbol = normalize_symbol(args["symbol"])
    years = int(args["years"])

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("缺少 SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")
        sys.exit(1)

    client = create_client(url, key)

    # Use the outputs produced by fetch_stock_data.py
    p_bs = os.path.join("outputs", f"{symbol}_balance_sheet_10y.csv")
    p_is = os.path.join("outputs", f"{symbol}_income_statement_10y.csv")
    p_cf = os.path.join("outputs", f"{symbol}_cash_flow_10y.csv")
    p_mc = os.path.join("outputs", f"{symbol}_mkt_cap_10y.csv")
    p_t10 = os.path.join("outputs", f"{symbol}_top10_shareholders_10y.csv")
    p_hc = os.path.join("outputs", f"{symbol}_holder_count_concentration_10y.csv")

    # Columns to upload: keep all balance sheet columns from CSV (normalized)
    bs_keep = get_balance_sheet_keep_cols(p_bs)
    is_keep = get_income_statement_keep_cols(p_is)
    cf_keep = get_cash_flow_keep_cols(p_cf)

    print(f"Uploading wide tables for {symbol} (years={years})")

    # Read and build records
    df_bs = df_from_csv(p_bs)
    df_is = df_from_csv(p_is)
    df_cf = df_from_csv(p_cf)
    df_mc = df_from_csv(p_mc)

    rec_bs = to_records_financial(df_bs, symbol, bs_keep)
    rec_is = to_records_financial(df_is, symbol, is_keep)
    rec_cf = to_records_financial(df_cf, symbol, cf_keep)
    rec_mc = to_records_mkt_cap(df_mc, symbol)
    rec_t10 = to_records_top10(p_t10, symbol)
    rec_hc = to_records_holder_count(p_hc, symbol)

    # Upsert
    upsert_batches(client, "cn_balance_sheet_10y", rec_bs, "symbol,report_date")
    upsert_batches(client, "cn_income_statement_10y", rec_is, "symbol,report_date")
    upsert_batches(client, "cn_cash_flow_10y", rec_cf, "symbol,report_date")
    upsert_batches(client, "cn_mkt_cap_10y", rec_mc, "symbol,trade_date")
    upsert_batches(client, "cn_top10_shareholders_10y", rec_t10, "symbol,report_date,rank")
    upsert_batches(client, "cn_holder_count_concentration_10y", rec_hc, "symbol,report_date")

    print("Done.")


if __name__ == "__main__":
    main()

