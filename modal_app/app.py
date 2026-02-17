import os
import datetime as dt
import re
from typing import Any, Dict, Optional, Tuple

import modal
import pandas as pd
import akshare as ak
from fastapi import Body, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Define Modal image (v3 - 2026-02-04 fix table names)
image = modal.Image.debian_slim().pip_install(
    "akshare>=1.14.0", "pandas>=2.0.0", "supabase>=2.0.0", "python-dotenv", "fastapi>=0.109.0"
)

app = modal.App("stock-data-fetcher")
web_app = FastAPI()
web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

META_COLS = ["数据源", "是否审计", "公告日期", "币种", "类型", "更新日期"]
REPORT_COL = "报告日"


def normalize_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if s.endswith(".SZ") or s.endswith(".SH") or s.endswith(".SS"):
        s = s.split(".")[0]
    if s.startswith("SZ") or s.startswith("SH"):
        s = s[2:]
    if s.isdigit():
        s = s.zfill(6)
    return s


def market_prefixed_symbol(symbol: str, market: Optional[str]) -> str:
    if market == "SH":
        return f"SH{symbol}"
    if market == "SZ":
        return f"SZ{symbol}"
    if symbol.startswith("6"):
        return f"SH{symbol}"
    return f"SZ{symbol}"


def format_date(val: Any) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    # 如果看起来像股票代码，返回 None
    if s.endswith(".SH") or s.endswith(".SZ") or s.endswith(".SS"):
        return None
    if s.isdigit() and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        # 无法解析则返回 None 而非原字符串
        return None


def clean_value(v: Any) -> Any:
    import math
    import numpy as np
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return None
    if isinstance(v, (np.floating, np.integer)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v) if isinstance(v, np.floating) else int(v)
    return v


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rename = {}
    for c in df.columns:
        s = str(c).strip()
        if all(ch.isupper() or ch.isdigit() or ch == "_" for ch in s):
            rename[c] = s.lower()
        else:
            rename[c] = s.lower()
    if rename:
        df = df.rename(columns=rename)
    return df


def should_scale(col: str) -> bool:
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
    if c == "count" or c.endswith("_count") or "_count_" in c:
        return False
    if c == "rank" or c.endswith("_rank") or "_rank_" in c:
        return False
    return True


def to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return float(s.replace(",", ""))
    except Exception:
        return None


def to_records_wide(df: pd.DataFrame, symbol: str) -> list:
    df = normalize_columns(df)
    # drop temporary/internal columns before upsert
    df = df.drop(columns=[c for c in df.columns if str(c).startswith("_") or str(c) == "date_dt"], errors="ignore")
    # drop non-schema date fields from AKShare
    allowed_dates = {"report_date"}
    drop_date_cols = [c for c in df.columns if str(c).endswith("_date") and str(c) not in allowed_dates]
    if drop_date_cols:
        df = df.drop(columns=drop_date_cols, errors="ignore")
    if "report_date" in df.columns:
        df["report_date"] = df["report_date"].apply(format_date)
    elif "report_date_name" in df.columns and "report_date" not in df.columns:
        df["report_date"] = df["report_date_name"].apply(format_date)
    df["symbol"] = symbol
    # prefer security_name if abbreviation present
    if "security_name" not in df.columns and "security_name_abbr" in df.columns:
        df["security_name"] = df["security_name_abbr"]
    # filter invalid column names
    cols = [c for c in df.columns if re.match(r"^[a-z0-9_]+$", str(c))]
    # extra guard to ensure no temp columns pass through
    cols = [c for c in cols if not str(c).startswith("_") and str(c) != "date_dt"]
    scale_cols = {c for c in cols if should_scale(c)}
    out = []
    for _, r in df[cols].iterrows():
        rec = {}
        for k in cols:
            v = r.get(k)
            if k in scale_cols:
                n = to_float(v)
                rec[k] = None if n is None else clean_value(n / 1e8)  # 元 -> 亿元
            elif k in ("basic_eps", "diluted_eps"):
                rec[k] = clean_value(to_float(v))
            else:
                rec[k] = clean_value(v)
        out.append(rec)
    return out


def fetch_with_fallback(fetch_fn, symbol: str, market: Optional[str]):
    """尝试多种股票代码格式调用 API"""
    # 首先尝试带市场前缀的格式（这是大多数东方财富 API 需要的）
    alt = market_prefixed_symbol(symbol, market)
    try:
        df = fetch_fn(alt)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
    except Exception:
        pass
    # 再尝试纯数字格式
    try:
        df = fetch_fn(symbol)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
    except Exception:
        pass
    return pd.DataFrame()


def get_latest_date(supabase, table: str, symbol: str, date_col: str = "report_date") -> Optional[dt.date]:
    """获取该股票在指定表中的最新日期"""
    try:
        resp = (
            supabase.table(table)
            .select(date_col)
            .eq("symbol", symbol)
            .order(date_col, desc=True)
            .limit(1)
            .execute()
        )
        if resp.data and resp.data[0].get(date_col):
            return pd.to_datetime(resp.data[0][date_col]).date()
    except Exception:
        pass
    return None


def run_fetch(item: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
    from supabase import create_client

    symbol = normalize_symbol(item.get("symbol", ""))
    if not symbol:
        return {"success": False, "message": "缺少股票代码"}, 400
    skip_holder_count = bool(item.get("skip_holder_count"))
    incremental = bool(item.get("incremental", True))  # 默认启用增量下载

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        return {"success": False, "message": "缺少 Supabase 凭证"}, 500

    try:
        supabase = create_client(url, key)
        resp = (
            supabase.table("company_list")
            .select("exchange,description")
            .eq("symbol", symbol)
            .limit(1)
            .execute()
        )

        if not resp.data:
            return {"success": False, "message": f"股票代码 {symbol} 不在公司列表中"}, 404

        exchange = str(resp.data[0].get("exchange") or "").upper()
        company_name = resp.data[0].get("description")
        if exchange not in ["SSE", "SZSE"]:
            return {"success": False, "message": f"股票 {symbol} 不属于中国A股市场"}, 400

        market = "SH" if exchange == "SSE" else "SZ"
        cutoff = dt.datetime.now() - dt.timedelta(days=10 * 365)
        errors = []

        # 获取各表的最新日期（用于增量下载）
        latest_dates = {}
        if incremental:
            latest_dates = {
                "balance_sheet": get_latest_date(supabase, "cn_balance_sheet_10y", symbol),
                "income_statement": get_latest_date(supabase, "cn_income_statement_10y", symbol),
                "cash_flow": get_latest_date(supabase, "cn_cash_flow_10y", symbol),
                "mkt_cap": get_latest_date(supabase, "cn_mkt_cap_10y", symbol, "trade_date"),
                "top10": get_latest_date(supabase, "cn_top10_shareholders_10y", symbol),
                "holder_count": get_latest_date(supabase, "cn_holder_count_concentration_10y", symbol),
            }

        def safe_fetch(label, fn):
            try:
                return fn()
            except Exception as err:
                errors.append(f"{label}: {err}")
                return pd.DataFrame()

        df_bs = safe_fetch(
            "资产负债表",
            lambda: fetch_with_fallback(ak.stock_balance_sheet_by_report_em, symbol, market),
        )
        bs_records = []
        if not df_bs.empty:
            df_bs = normalize_columns(df_bs)
            df_bs["_dt"] = pd.to_datetime(df_bs.get("report_date"), errors="coerce")
            df_bs = df_bs[df_bs["_dt"] >= cutoff]
            latest_bs = latest_dates.get("balance_sheet")
            if latest_bs:
                df_bs = df_bs[df_bs["_dt"].dt.date > latest_bs]
            bs_records = to_records_wide(df_bs, symbol)

        df_is = safe_fetch(
            "利润表",
            lambda: fetch_with_fallback(ak.stock_profit_sheet_by_report_em, symbol, market),
        )
        is_records = []
        if not df_is.empty:
            df_is = normalize_columns(df_is)
            df_is["_dt"] = pd.to_datetime(df_is.get("report_date"), errors="coerce")
            latest_is = latest_dates.get("income_statement")
            if latest_is:
                df_is = df_is[df_is["_dt"].dt.date > latest_is]
            is_records = to_records_wide(df_is, symbol)

        df_cf = safe_fetch(
            "现金流量表",
            lambda: fetch_with_fallback(ak.stock_cash_flow_sheet_by_report_em, symbol, market),
        )
        cf_records = []
        if not df_cf.empty:
            df_cf = normalize_columns(df_cf)
            df_cf["_dt"] = pd.to_datetime(df_cf.get("report_date"), errors="coerce")
            latest_cf = latest_dates.get("cash_flow")
            if latest_cf:
                df_cf = df_cf[df_cf["_dt"].dt.date > latest_cf]
            cf_records = to_records_wide(df_cf, symbol)

        df_mc = safe_fetch("市值历史", lambda: ak.stock_value_em(symbol=symbol))
        mc_records = []
        if not df_mc.empty:
            df_mc = df_mc.rename(columns={"数据日期": "date", "总市值": "total_mv"})
            df_mc["date_dt"] = pd.to_datetime(df_mc["date"], errors="coerce")
            df_mc = df_mc[df_mc["date_dt"] >= cutoff]
            # 增量筛选
            latest_mc = latest_dates.get("mkt_cap")
            if latest_mc:
                df_mc = df_mc[df_mc["date_dt"].dt.date > latest_mc]
            for _, row in df_mc.iterrows():
                mc_records.append(
                    {
                        "symbol": symbol,
                        "trade_date": format_date(row["date"]),
                        "mkt_cap_billion_cny": clean_value(row["total_mv"] / 1e9),
                    }
                )

        df_holders = safe_fetch(
            "前十大股东",
            lambda: fetch_with_fallback(ak.stock_main_stock_holder, symbol, market),
        )
        holder_records = []
        latest_top10 = latest_dates.get("top10")
        if not df_holders.empty:
            for _, row in df_holders.iterrows():
                rank_value = clean_value(row.get("编号"))
                if rank_value is None:
                    continue
                # 尝试获取日期字段（不同版本可能是 截至日期 或 截止日期）
                report_date_raw = row.get("截至日期") or row.get("截止日期") or row.get("报告期")
                report_date = format_date(report_date_raw)
                # 如果解析出的日期包含股票代码，则跳过
                if report_date and (report_date.endswith(".SH") or report_date.endswith(".SZ") or report_date.endswith(".SS")):
                    report_date = None
                # 增量筛选
                if latest_top10 and report_date:
                    try:
                        if pd.to_datetime(report_date).date() <= latest_top10:
                            continue
                    except Exception:
                        pass
                holder_records.append(
                    {
                        "symbol": symbol,
                        "report_date": report_date,
                        "rank": int(rank_value),
                        "shareholder_name": row.get("股东名称"),
                        "hold_num": clean_value(row.get("持股数量")),
                        "hold_ratio": clean_value(row.get("持股比例")),
                    }
                )

        hc_records = []
        latest_hc = latest_dates.get("holder_count")
        if not skip_holder_count:
            # 从最近的季度往回查（2026年2月时，最新应该是2025Q3）
            quarter_dates = ["20250930", "20250630", "20250331", "20241231", "20240930", "20240630"]
            all_hc_data = []
            for d_str in quarter_dates:
                # 增量筛选：如果季度日期已存在，跳过
                if latest_hc:
                    q_date = pd.to_datetime(d_str).date()
                    if q_date <= latest_hc:
                        continue
                try:
                    df_hc = ak.stock_hold_num_cninfo(date=d_str)
                    if df_hc is not None and not df_hc.empty:
                        filtered = df_hc[df_hc["证券代码"] == symbol]
                        if not filtered.empty:
                            all_hc_data.append(filtered)
                except Exception:
                    continue

            if all_hc_data:
                df_hc_all = pd.concat(all_hc_data)
                for _, row in df_hc_all.iterrows():
                    hc_records.append(
                        {
                            "symbol": symbol,
                            "report_date": format_date(row.get("变动日期")),
                            "security_name": row.get("证券简称"),
                            "holder_count": clean_value(row.get("本期股东人数")),
                            "holder_count_prev": clean_value(row.get("上期股东人数")),
                            "holder_count_change": clean_value(row.get("股东人数增幅")),
                            "avg_hold_num": clean_value(row.get("本期人均持股数量")),
                            "avg_hold_num_prev": clean_value(row.get("上期人均持股数量")),
                            "avg_hold_num_change": clean_value(row.get("人均持股数量增幅")),
                        }
                    )

        # 过滤掉日期为 None 的记录
        bs_records = [r for r in bs_records if r.get("report_date")]
        is_records = [r for r in is_records if r.get("report_date")]
        cf_records = [r for r in cf_records if r.get("report_date")]
        mc_records = [r for r in mc_records if r.get("trade_date")]
        holder_records = [r for r in holder_records if r.get("report_date")]
        hc_records = [r for r in hc_records if r.get("report_date")]

        if bs_records:
            supabase.table("cn_balance_sheet_10y").upsert(
                bs_records, on_conflict="symbol,report_date"
            ).execute()
        if is_records:
            supabase.table("cn_income_statement_10y").upsert(
                is_records, on_conflict="symbol,report_date"
            ).execute()
        if cf_records:
            supabase.table("cn_cash_flow_10y").upsert(
                cf_records, on_conflict="symbol,report_date"
            ).execute()
        if mc_records:
            for i in range(0, len(mc_records), 1000):
                supabase.table("cn_mkt_cap_10y").upsert(
                    mc_records[i : i + 1000], on_conflict="symbol,trade_date"
                ).execute()
        if holder_records:
            supabase.table("cn_top10_shareholders_10y").upsert(
                holder_records, on_conflict="symbol,report_date,rank"
            ).execute()
        if hc_records:
            supabase.table("cn_holder_count_concentration_10y").upsert(
                hc_records, on_conflict="symbol,report_date"
            ).execute()

        counts = {
            "balance_sheet": len(bs_records),
            "income_statement": len(is_records),
            "cash_flow": len(cf_records),
            "mkt_cap": len(mc_records),
            "top10_shareholders": len(holder_records),
            "holder_count_concentration": len(hc_records),
        }
        total_new = sum(counts.values())
        if incremental and total_new == 0:
            message = "数据已是最新，无需更新"
        elif incremental:
            message = f"增量同步完成，新增 {total_new} 条记录"
        else:
            message = "数据同步完成"
        if errors:
            message = f"部分数据下载失败: {', '.join(errors)}"

        return (
            {
                "success": len(errors) == 0,
                "message": message,
                "company": company_name,
                "exchange": exchange,
                "counts": counts,
                "incremental": incremental,
                "latest_dates": {k: str(v) if v else None for k, v in latest_dates.items()},
            },
            200,
        )

    except Exception as e:
        return {"success": False, "message": f"发生错误: {str(e)}"}, 500


@web_app.post("/fetch-stock-data")
def fetch_stock_data(item: Dict[str, Any] = Body(default_factory=dict)):
    result, status_code = run_fetch(item)
    return JSONResponse(status_code=status_code, content=result)


@app.function(image=image, secrets=[modal.Secret.from_name("supabase-secrets")], timeout=900)
@modal.asgi_app()
def fastapi_app():
    return web_app
