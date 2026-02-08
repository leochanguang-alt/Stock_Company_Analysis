import os
import datetime as dt
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


def scale_to_billion(v: Any) -> Any:
    val = clean_value(v)
    if val is None:
        return None
    try:
        return float(val) / 1e9
    except (ValueError, TypeError):
        return val


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
            # 使用 REPORT_DATE 列作为日期筛选
            df_bs["_dt"] = pd.to_datetime(df_bs.get("REPORT_DATE"), errors="coerce")
            df_bs = df_bs[df_bs["_dt"] >= cutoff]
            # 增量筛选：只保留比已有数据更新的记录
            latest_bs = latest_dates.get("balance_sheet")
            if latest_bs:
                df_bs = df_bs[df_bs["_dt"].dt.date > latest_bs]
            for _, row in df_bs.iterrows():
                rec = {
                    "symbol": symbol,
                    "report_date": format_date(row.get("REPORT_DATE")),
                    "secucode": row.get("SECUCODE"),
                    "security_name": row.get("SECURITY_NAME_ABBR"),
                    "report_date_name": row.get("REPORT_DATE_NAME"),
                    # 关键资产负债表字段（与数据库列名匹配）
                    "monetaryfunds": scale_to_billion(row.get("MONETARYFUNDS")),
                    "accounts_rece": scale_to_billion(row.get("ACCOUNTS_RECE")),
                    "inventory": scale_to_billion(row.get("INVENTORY")),
                    "total_current_assets": scale_to_billion(row.get("TOTAL_CURRENT_ASSETS")),
                    "fixed_asset": scale_to_billion(row.get("FIXED_ASSET")),
                    "intangible_asset": scale_to_billion(row.get("INTANGIBLE_ASSET")),
                    "total_noncurrent_assets": scale_to_billion(row.get("TOTAL_NONCURRENT_ASSETS")),
                    "total_assets": scale_to_billion(row.get("TOTAL_ASSETS")),
                    "short_loan": scale_to_billion(row.get("SHORT_LOAN")),
                    "accounts_payable": scale_to_billion(row.get("ACCOUNTS_PAYABLE")),
                    "total_current_liab": scale_to_billion(row.get("TOTAL_CURRENT_LIAB")),
                    "long_loan": scale_to_billion(row.get("LONG_LOAN")),
                    "total_noncurrent_liab": scale_to_billion(row.get("TOTAL_NONCURRENT_LIAB")),
                    "total_liabilities": scale_to_billion(row.get("TOTAL_LIABILITIES")),
                    "share_capital": scale_to_billion(row.get("SHARE_CAPITAL")),
                    "capital_reserve": scale_to_billion(row.get("CAPITAL_RESERVE")),
                    "surplus_reserve": scale_to_billion(row.get("SURPLUS_RESERVE")),
                    "unassign_rpofit": scale_to_billion(row.get("UNASSIGN_RPOFIT")),
                    "total_parent_equity": scale_to_billion(row.get("TOTAL_PARENT_EQUITY")),
                    "minority_equity": scale_to_billion(row.get("MINORITY_EQUITY")),
                    "total_equity": scale_to_billion(row.get("TOTAL_EQUITY")),
                    "total_liab_equity": scale_to_billion(row.get("TOTAL_LIAB_EQUITY")),
                }
                bs_records.append(rec)

        df_is = safe_fetch(
            "利润表",
            lambda: fetch_with_fallback(ak.stock_profit_sheet_by_report_em, symbol, market),
        )
        is_records = []
        if not df_is.empty:
            df_is["_dt"] = pd.to_datetime(df_is.get("REPORT_DATE"), errors="coerce")
            # 增量筛选
            latest_is = latest_dates.get("income_statement")
            if latest_is:
                df_is = df_is[df_is["_dt"].dt.date > latest_is]
            for _, row in df_is.iterrows():
                is_records.append(
                    {
                        "symbol": symbol,
                        "report_date": format_date(row.get("REPORT_DATE")),
                        "total_operate_income": scale_to_billion(row.get("TOTAL_OPERATE_INCOME")),
                    }
                )

        df_cf = safe_fetch(
            "现金流量表",
            lambda: fetch_with_fallback(ak.stock_cash_flow_sheet_by_report_em, symbol, market),
        )
        cf_records = []
        if not df_cf.empty:
            df_cf["_dt"] = pd.to_datetime(df_cf.get("REPORT_DATE"), errors="coerce")
            # 增量筛选
            latest_cf = latest_dates.get("cash_flow")
            if latest_cf:
                df_cf = df_cf[df_cf["_dt"].dt.date > latest_cf]
            for _, row in df_cf.iterrows():
                cf_records.append(
                    {
                        "symbol": symbol,
                        "report_date": format_date(row.get("REPORT_DATE")),
                        "netcash_operate": scale_to_billion(row.get("NETCASH_OPERATE")),
                    }
                )

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
