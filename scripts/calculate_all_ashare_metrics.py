#!/usr/bin/env python3
"""
Calculate financial metrics for all A-share stocks (~5820 stocks).

Computes:
  a) 市值 (Market Cap)
  b) EV (Enterprise Value)
  c) EBIT & 税款 (TTM)
  d) 经营性现金流 OCF (TTM)
  e) 有息负债 (Interest-bearing Debt)
  f) 股东权益 (Shareholders' Equity)
  g) PIR (Potential Investment Return)

Formulas:
  Debt  = 短期借款 + 一年内到期的非流动负债 + 长期借款 + 应付债券 + 租赁负债
  EV    = 市值 + Debt - 货币资金 - 交易性金融资产 + 少数股东权益 + 其他权益工具
  EBIT  = 利润总额(TTM) + 利息费用(TTM)
  TTM   = 当期累计 + 上年年报 - 上年同期累计
  ROIC  = (EBIT_TTM - 所得税_TTM) / IC
  PIR   = 过去12个月股息/市值 + IC/EV × ROIC

Usage:
    # Test with 10 stocks
    python scripts/calculate_all_ashare_metrics.py --limit 10

    # Full run in batches (recommended)
    python scripts/calculate_all_ashare_metrics.py --batch-size 200 --batch-sleep 30

    # Resume an interrupted run
    python scripts/calculate_all_ashare_metrics.py --resume

    # Fewer workers if rate-limited
    python scripts/calculate_all_ashare_metrics.py --workers 2 --batch-size 100
"""

import argparse
import csv
import json
import math
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import akshare as ak
except ImportError:
    print("ERROR: akshare is not installed. Run: pip install akshare")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    # Fallback: simple progress bar
    class tqdm:
        def __init__(self, total=0, desc="", **kwargs):
            self.total = total
            self.desc = desc
            self.n = 0
        def update(self, n=1):
            self.n += n
            pct = self.n / self.total * 100 if self.total else 0
            print(f"\r  {self.desc}: {self.n}/{self.total} ({pct:.1f}%)", end="", flush=True)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            print()

# ─────────────────────────── CONFIG ───────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"
CHECKPOINT_DIR = OUTPUT_DIR / "ashare_metrics_checkpoints"

# CSV output columns (ordered)
OUTPUT_COLUMNS = [
    "symbol", "name", "market_cap",
    "enterprise_value", "ebit_ttm", "income_tax_ttm", "ocf_ttm",
    "interest_bearing_debt", "total_equity", "invested_capital",
    "interest_bearing_debt_prev", "total_equity_prev", "invested_capital_prev",
    "prev_bs_date",
    "dividend_ttm", "dividend_yield", "roic_posttax", "pir",
    "latest_bs_date", "ttm_period_type", "ttm_base_date",
    "short_term_debt", "current_portion_lt_debt", "long_term_debt",
    "bonds_payable", "lease_liabilities",
    "cash_equivalents", "short_term_investments",
    "minority_interest", "other_equity_instruments",
]

CHECKPOINT_CSV = CHECKPOINT_DIR / "partial_results.csv"
CHECKPOINT_META = CHECKPOINT_DIR / "progress.json"


# ─────────────── GLOBAL RATE LIMITER (Sina API) ──────────────
# Ensures minimum interval between consecutive Sina API calls across all threads.

_sina_api_lock = threading.Lock()
_sina_last_call_time = 0.0
_SINA_MIN_INTERVAL = 1.5  # seconds between Sina API calls (conservative)


def _rate_limited_sina_call(func, *args, max_retries: int = 3, **kwargs):
    """
    Call a Sina API function with:
      1. Global rate limiting (min interval between calls)
      2. Retry with exponential backoff on failure
    """
    global _sina_last_call_time

    for attempt in range(1, max_retries + 1):
        # Enforce rate limit
        with _sina_api_lock:
            now = time.time()
            wait = _SINA_MIN_INTERVAL - (now - _sina_last_call_time)
            if wait > 0:
                time.sleep(wait)
            _sina_last_call_time = time.time()

        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < max_retries:
                # Exponential backoff with jitter
                delay = 5 * attempt + random.uniform(1, 4)
                time.sleep(delay)
            else:
                raise  # Final attempt failed, propagate exception


# ─────────────────────────── HELPERS ──────────────────────────

def safe_float(val, default=0.0) -> float:
    """Convert a value to float safely. Returns default if conversion fails."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        if math.isnan(val) or math.isinf(val):
            return default
        return float(val)
    s = str(val).strip()
    if s in ("", "--", "None", "nan", "NaN", "inf", "-inf"):
        return default
    try:
        return float(s)
    except (ValueError, TypeError):
        return default


def get_sina_code(symbol: str) -> str:
    """Convert 6-digit stock code to Sina-format (e.g., sh600233, sz000001)."""
    s = str(symbol).zfill(6)
    if s.startswith(("6", "9")):
        return f"sh{s}"
    else:
        return f"sz{s}"


# ─────────────── PHASE 1: STOCK UNIVERSE ──────────────────────

def _fetch_tencent_market_data(symbols: List[str], batch_size: int = 80) -> Dict[str, dict]:
    """
    Fetch market cap and price data from Tencent qt.gtimg.cn API.

    Tencent API fields:
      [1]=name, [3]=price, [44]=total_market_cap(亿), [72]=total_shares(股)

    Returns dict: {symbol: {"market_cap": float(元), "price": float, "name": str}}
    """
    import requests as _req

    results = {}
    tencent_codes = []
    for s in symbols:
        s6 = str(s).zfill(6)
        prefix = "sh" if s6.startswith(("6", "9")) else "sz"
        tencent_codes.append((s6, f"{prefix}{s6}"))

    # Batch query (Tencent supports ~80 per request)
    for i in range(0, len(tencent_codes), batch_size):
        batch = tencent_codes[i : i + batch_size]
        query = ",".join(tc for _, tc in batch)
        try:
            r = _req.get(f"https://qt.gtimg.cn/q={query}", timeout=15)
            lines = [l for l in r.text.strip().split(";") if l.strip()]
            for line in lines:
                parts = line.split("~")
                if len(parts) > 72:
                    code = str(parts[2]).zfill(6)
                    try:
                        mktcap_yi = float(parts[44])
                        price = float(parts[3])
                        results[code] = {
                            "market_cap": mktcap_yi * 1e8,  # 亿 → 元
                            "price": price,
                            "name": parts[1],
                        }
                    except (ValueError, IndexError):
                        pass
        except Exception:
            pass
        if i + batch_size < len(tencent_codes):
            time.sleep(0.3)

    return results


def fetch_stock_universe(max_retries: int = 3, base_delay: int = 15) -> pd.DataFrame:
    """
    Fetch all A-share stocks with their market caps and prices.
    Returns DataFrame with columns: symbol, name, market_cap, current_price, sina_code

    Strategy:
      1. Try stock_zh_a_spot_em() (East Money) — has market cap directly.
      2. If East Money is rate-limited, fall back to stock_zh_a_spot() (Sina)
         for the stock list, then use Tencent API (qt.gtimg.cn) for market cap.
    """
    df = None
    source = None

    # ── Try East Money first ──
    for attempt in range(1, max_retries + 1):
        try:
            print(f"  Calling ak.stock_zh_a_spot_em() (attempt {attempt}/{max_retries}) ...")
            df = ak.stock_zh_a_spot_em()
            source = "eastmoney"
            print(f"  ✓ East Money: {len(df)} stocks")
            break
        except Exception as e:
            if attempt < max_retries:
                delay = base_delay * attempt
                print(f"  ⚠ Attempt {attempt} failed: {type(e).__name__}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"  ✗ East Money unavailable after {max_retries} attempts.")

    # ── Fallback to Sina (stock list) + Tencent (market cap) ──
    if df is None:
        for attempt in range(1, 3):
            try:
                print(f"  Calling ak.stock_zh_a_spot() (attempt {attempt}/2) ...")
                df = ak.stock_zh_a_spot()
                source = "sina"
                print(f"  ✓ Sina: {len(df)} stocks")
                break
            except Exception as e:
                if attempt < 2:
                    print(f"  ⚠ Sina attempt {attempt} failed: {type(e).__name__}. Retrying in 15s...")
                    time.sleep(15)
                else:
                    print(f"  ✗ Sina also unavailable.")

    # ── Fallback 3: Build stock list from known code ranges + Tencent ──
    if df is None:
        print("  Falling back to Tencent-only mode (scanning code ranges) ...")
        source = "tencent"
        # Generate all plausible A-share codes
        code_ranges = (
            [f"{i:06d}" for i in range(600000, 605000)]   # Shanghai main board
            + [f"{i:06d}" for i in range(601000, 602000)]  # Shanghai main board
            + [f"{i:06d}" for i in range(603000, 604000)]  # Shanghai main board
            + [f"{i:06d}" for i in range(605000, 605600)]  # Shanghai main board
            + [f"{i:06d}" for i in range(688000, 689000)]  # STAR Market
            + [f"{i:06d}" for i in range(0, 5000)]         # Shenzhen main board
            + [f"{i:06d}" for i in range(200000, 201000)]  # Shenzhen B
            + [f"{i:06d}" for i in range(300000, 302000)]  # ChiNext
            + [f"{i:06d}" for i in range(920000, 921000)]  # BSE
            + [f"{i:06d}" for i in range(430000, 431000)]  # BSE
            + [f"{i:06d}" for i in range(830000, 840000)]  # BSE
        )
        tencent_data = _fetch_tencent_market_data(code_ranges)
        rows = []
        for sym, td in tencent_data.items():
            if td["market_cap"] > 0 and td["price"] > 0:
                rows.append({
                    "symbol": sym,
                    "name": td["name"],
                    "market_cap": td["market_cap"],
                    "current_price": td["price"],
                })
        df = pd.DataFrame(rows)
        print(f"  ✓ Tencent: found {len(df)} active stocks")

    result = pd.DataFrame()

    if source == "eastmoney":
        result["symbol"] = df["代码"].astype(str).str.zfill(6)
        result["name"] = df["名称"]
        result["market_cap"] = pd.to_numeric(df["总市值"], errors="coerce")
        result["current_price"] = pd.to_numeric(df["最新价"], errors="coerce")
    elif source == "tencent":
        # Already built as DataFrame with correct columns
        result = df.copy()
    else:
        # Sina: code format is like "bj920000", "sh600233", "sz000001"
        raw_codes = df["代码"].astype(str)
        result["symbol"] = raw_codes.str.extract(r"(\d{6})$", expand=False)
        result["name"] = df["名称"]
        result["current_price"] = pd.to_numeric(df["最新价"], errors="coerce")
        result["market_cap"] = np.nan  # Will be filled from Tencent

    result["sina_code"] = result["symbol"].apply(get_sina_code)

    # Drop stocks with no symbol or no price
    result = result.dropna(subset=["symbol"])
    result = result[result["current_price"] > 0]

    # ── Fill market cap from Tencent if missing ──
    if source == "sina":
        symbols_list = result["symbol"].tolist()
        print(f"  Fetching market cap from Tencent API for {len(symbols_list)} stocks ...")
        tencent_data = _fetch_tencent_market_data(symbols_list)
        filled = 0
        for idx, row in result.iterrows():
            sym = row["symbol"]
            if sym in tencent_data:
                td = tencent_data[sym]
                result.at[idx, "market_cap"] = td["market_cap"]
                result.at[idx, "current_price"] = td["price"]
                filled += 1
        print(f"  ✓ Tencent: filled market cap for {filled}/{len(symbols_list)} stocks")

    # Drop stocks with no market cap
    before = len(result)
    result = result.dropna(subset=["market_cap"])
    result = result[result["market_cap"] > 0]
    if len(result) < before:
        print(f"  Dropped {before - len(result)} stocks with no market cap")

    result = result.reset_index(drop=True)
    return result


# ─────────────── PHASE 2: DATA FETCH ─────────────────────────

def get_em_code(symbol: str) -> str:
    """Convert 6-digit stock code to East Money format (e.g., SH600233, SZ000001)."""
    s = str(symbol).zfill(6)
    if s.startswith(("6", "9")):
        return f"SH{s}"
    else:
        return f"SZ{s}"


# East Money report type URL patterns
_EM_REPORT_URLS = {
    "balance": {
        "dates": "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/zcfzbDateAjaxNew",
        "data": "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/zcfzbAjaxNew",
    },
    "income": {
        "dates": "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/lrbDateAjaxNew",
        "data": "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/lrbAjaxNew",
    },
    "cashflow": {
        "dates": "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/xjllbDateAjaxNew",
        "data": "https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/xjllbAjaxNew",
    },
}

# Company type cache (thread-safe dict)
_em_ctype_cache: Dict[str, str] = {}
_em_ctype_lock = threading.Lock()


def _get_em_company_type(em_code: str) -> str:
    """
    Get East Money company type for a stock (cached).
    Types: 1=bank, 2=insurance, 3=securities, 4=general.
    """
    with _em_ctype_lock:
        if em_code in _em_ctype_cache:
            return _em_ctype_cache[em_code]

    try:
        # Use akshare's built-in function (has lru_cache)
        import inspect
        mod = inspect.getmodule(ak.stock_balance_sheet_by_report_em)
        ctype_func = getattr(mod, '_stock_balance_sheet_by_report_ctype_em')
        ctype = ctype_func(symbol=em_code)
    except Exception:
        ctype = "4"  # Default: general company

    with _em_ctype_lock:
        _em_ctype_cache[em_code] = ctype
    return ctype


def _fetch_em_report(
    em_code: str,
    report_type: str,
    company_type: str,
    max_dates: int = 10,
    max_retries: int = 3,
) -> pd.DataFrame:
    """
    Fetch a single financial report from East Money's API.
    Only retrieves the latest `max_dates` report periods for speed.

    For TTM we need at most: current quarter, prev FY, prev same quarter (max 2 years).
    10 dates covers ~2.5 years of quarterly data.
    """
    import requests as _req

    urls = _EM_REPORT_URLS[report_type]

    # Step 1: Get available report dates
    for attempt in range(1, max_retries + 1):
        try:
            r = _req.get(
                urls["dates"],
                params={
                    "companyType": company_type,
                    "reportDateType": "0",
                    "code": em_code,
                },
                timeout=15,
            )
            data_json = r.json()
            break
        except Exception:
            if attempt < max_retries:
                time.sleep(2 * attempt)
            else:
                raise

    if "data" not in data_json or not data_json["data"]:
        return pd.DataFrame()

    # Get only latest N dates
    all_dates = [d["REPORT_DATE"][:10] for d in data_json["data"]]
    dates_to_fetch = all_dates[:max_dates]

    # Step 2: Fetch data in batches of 5
    big_df = pd.DataFrame()
    for i in range(0, len(dates_to_fetch), 5):
        batch_dates = ",".join(dates_to_fetch[i : i + 5])
        for attempt in range(1, max_retries + 1):
            try:
                r = _req.get(
                    urls["data"],
                    params={
                        "companyType": company_type,
                        "reportDateType": "0",
                        "reportType": "1",
                        "dates": batch_dates,
                        "code": em_code,
                    },
                    timeout=15,
                )
                data_json = r.json()
                break
            except Exception:
                if attempt < max_retries:
                    time.sleep(2 * attempt)
                else:
                    raise

        if "data" not in data_json:
            break
        temp_df = pd.DataFrame(data_json["data"])
        if big_df.empty:
            big_df = temp_df
        else:
            big_df = pd.concat([big_df, temp_df], ignore_index=True)

    return big_df


def fetch_stock_financials(em_code: str, max_retries: int = 3) -> Dict[str, pd.DataFrame]:
    """
    Fetch balance sheet, income statement, and cash flow for one stock
    from East Money (东方财富).

    Optimized: only fetches the latest 10 report periods (~2.5 years).
    Direct API calls avoid akshare's full-history pagination (~17s vs ~108s).

    Args:
        em_code: East Money code like "SH600233" or "SZ000001"

    Returns dict with keys: 'balance', 'income', 'cashflow'.
    """
    company_type = _get_em_company_type(em_code)

    bs = _fetch_em_report(em_code, "balance", company_type, max_retries=max_retries)
    inc = _fetch_em_report(em_code, "income", company_type, max_retries=max_retries)
    cf = _fetch_em_report(em_code, "cashflow", company_type, max_retries=max_retries)

    return {"balance": bs, "income": inc, "cashflow": cf}


def fetch_dividend_data(symbol: str) -> pd.DataFrame:
    """
    Fetch dividend history for a stock using stock_history_dividend_detail (Sina API).

    Returns DataFrame with columns: 公告日期, 送股, 转增, 派息, 进度, 除权除息日, ...
    '派息' is cash dividend per 10 shares (元).
    """
    try:
        df = _rate_limited_sina_call(
            ak.stock_history_dividend_detail,
            symbol=symbol, indicator="分红",
            max_retries=2,
        )
        return df
    except Exception:
        return pd.DataFrame()


def compute_dividend_ttm(dividend_df: pd.DataFrame, current_price: float) -> float:
    """
    Compute trailing 12-month dividend per share from dividend history.

    Sums up 派息/10 for all dividends with 除权除息日 within the last 12 months
    and 进度 == '实施'.

    Returns total dividend per share (元), or 0.0 if no dividends.
    """
    if dividend_df.empty or "派息" not in dividend_df.columns:
        return 0.0

    today = datetime.now()
    cutoff = today - timedelta(days=365)

    total_dps = 0.0
    for _, row in dividend_df.iterrows():
        # Only count implemented (实施) dividends
        if str(row.get("进度", "")) != "实施":
            continue

        ex_date = row.get("除权除息日")
        if pd.isna(ex_date):
            continue

        # Parse ex-dividend date
        try:
            if isinstance(ex_date, str):
                ex_dt = datetime.strptime(ex_date, "%Y-%m-%d")
            else:
                ex_dt = pd.Timestamp(ex_date).to_pydatetime()
        except Exception:
            continue

        if cutoff <= ex_dt <= today:
            cash_div = safe_float(row.get("派息", 0))
            total_dps += cash_div / 10.0  # 派息 is per 10 shares

    return total_dps


# ─────────────── PHASE 2: METRIC COMPUTATION ─────────────────

def _normalize_report_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize REPORT_DATE column to string format 'YYYYMMDD'.
    East Money returns REPORT_DATE as datetime like '2025-09-30 00:00:00'.
    """
    df = df.copy()
    if "REPORT_DATE" in df.columns:
        df["_report_date_str"] = pd.to_datetime(df["REPORT_DATE"]).dt.strftime("%Y%m%d")
    return df


def detect_ttm_periods(
    df: pd.DataFrame,
) -> Tuple[Optional[str], str, Optional[str], Optional[str]]:
    """
    Given a financial statement DataFrame with REPORT_DATE column (East Money format),
    detect the latest report date and determine TTM parameters.

    Returns: (latest_date_str, period_type, fy_prev_str, same_q_prev_str)
      - Dates are in 'YYYYMMDD' string format
      - period_type: 'FY', 'Q3', 'Q2', 'Q1', or 'UNKNOWN'
      - If FY, no additional dates needed (fy_prev and same_q_prev are None)
    """
    if "_report_date_str" not in df.columns or df.empty:
        return None, "UNKNOWN", None, None

    dates = df["_report_date_str"].dropna().unique()
    if len(dates) == 0:
        return None, "UNKNOWN", None, None

    # Sort descending (dates are strings like '20250930')
    dates_sorted = sorted(dates, reverse=True)
    latest = dates_sorted[0]

    # Determine period type from month portion
    month_str = str(latest)[4:6]
    month_map = {"12": "FY", "09": "Q3", "06": "Q2", "03": "Q1"}
    period_type = month_map.get(month_str, "UNKNOWN")

    if period_type == "FY":
        return latest, "FY", None, None

    if period_type == "UNKNOWN":
        return latest, "UNKNOWN", None, None

    # Need: FY of previous year, same quarter of previous year
    year = int(str(latest)[:4])
    fy_prev = f"{year - 1}1231"
    same_q_prev = f"{year - 1}{str(latest)[4:]}"

    # Verify these dates exist in the data
    date_set = set(str(d) for d in dates)
    if fy_prev not in date_set:
        fy_prev = None
    if same_q_prev not in date_set:
        same_q_prev = None

    return latest, period_type, fy_prev, same_q_prev


def get_report_value(df: pd.DataFrame, report_date: str, col: str) -> Optional[float]:
    """Get a specific value from a financial statement for a given report date."""
    if col not in df.columns:
        return None
    rows = df[df["_report_date_str"] == report_date]
    if rows.empty:
        return None
    val = rows.iloc[0][col]
    result = safe_float(val, default=None)
    return result


def compute_ttm(
    df: pd.DataFrame,
    col: str,
    latest: str,
    period_type: str,
    fy_prev: Optional[str],
    same_q_prev: Optional[str],
) -> Optional[float]:
    """
    Compute TTM (trailing twelve months) for a flow-type metric.

    Chinese financial statements report cumulative year-to-date values:
      Q1 = Jan-Mar, Q2 = Jan-Jun, Q3 = Jan-Sep, FY = Jan-Dec

    TTM = latest_cumulative + prev_FY - prev_same_quarter_cumulative
    If FY: TTM = FY value directly.
    """
    if period_type == "FY":
        return get_report_value(df, latest, col)

    if period_type == "UNKNOWN" or fy_prev is None or same_q_prev is None:
        # Cannot compute TTM, fall back to latest cumulative value
        return get_report_value(df, latest, col)

    val_latest = get_report_value(df, latest, col)
    val_fy_prev = get_report_value(df, fy_prev, col)
    val_same_q_prev = get_report_value(df, same_q_prev, col)

    if val_latest is None or val_fy_prev is None or val_same_q_prev is None:
        # If any component is missing, return latest cumulative as fallback
        return val_latest

    return val_latest + val_fy_prev - val_same_q_prev


def compute_metrics_for_stock(
    symbol: str, name: str, market_cap: float, current_price: float,
    financials: Dict[str, pd.DataFrame], dividend_df: pd.DataFrame,
) -> dict:
    """
    Compute all target metrics for a single stock.
    Uses East Money (东方财富) column names.

    Returns dict with all output columns populated.
    """
    bs = _normalize_report_dates(financials["balance"])
    inc = _normalize_report_dates(financials["income"])
    cf = _normalize_report_dates(financials["cashflow"])

    result = {
        "symbol": symbol,
        "name": name,
        "market_cap": market_cap,
    }

    # ── Balance Sheet: latest snapshot ──
    if "_report_date_str" not in bs.columns or bs.empty:
        for col in OUTPUT_COLUMNS:
            result.setdefault(col, None)
        return result

    latest_bs = sorted(bs["_report_date_str"].dropna().unique(), reverse=True)[0]
    bs_row = bs[bs["_report_date_str"] == latest_bs].iloc[0]

    # If market_cap is still missing, set to 0 (EV/PIR will be None)
    if market_cap is None or (isinstance(market_cap, float) and math.isnan(market_cap)):
        market_cap = 0
        result["market_cap"] = None

    # East Money column names → our variables
    short_term_debt = safe_float(bs_row.get("SHORT_LOAN"))
    current_portion_lt = safe_float(bs_row.get("NONCURRENT_LIAB_1YEAR"))
    long_term_debt = safe_float(bs_row.get("LONG_LOAN"))
    bonds_payable = safe_float(bs_row.get("BOND_PAYABLE"))
    lease_liabilities = safe_float(bs_row.get("LEASE_LIAB"))

    interest_bearing_debt = (
        short_term_debt
        + current_portion_lt
        + long_term_debt
        + bonds_payable
        + lease_liabilities
    )

    cash_equivalents = safe_float(bs_row.get("MONETARYFUNDS"))
    short_term_investments = safe_float(bs_row.get("TRADE_FINASSET"))
    minority_interest = safe_float(bs_row.get("MINORITY_EQUITY"))
    other_equity_instruments = safe_float(bs_row.get("OTHER_EQUITY_TOOL"))
    total_equity = safe_float(bs_row.get("TOTAL_EQUITY"))

    # Enterprise Value
    ev = (
        market_cap
        + interest_bearing_debt
        - cash_equivalents
        - short_term_investments
        + minority_interest
        + other_equity_instruments
    )

    result.update(
        {
            "enterprise_value": ev,
            "interest_bearing_debt": interest_bearing_debt,
            "total_equity": total_equity,
            "invested_capital": interest_bearing_debt + total_equity,
            "latest_bs_date": str(latest_bs),
            "short_term_debt": short_term_debt,
            "current_portion_lt_debt": current_portion_lt,
            "long_term_debt": long_term_debt,
            "bonds_payable": bonds_payable,
            "lease_liabilities": lease_liabilities,
            "cash_equivalents": cash_equivalents,
            "short_term_investments": short_term_investments,
            "minority_interest": minority_interest,
            "other_equity_instruments": other_equity_instruments,
        }
    )

    # ── Balance Sheet: 12 months ago snapshot ──
    latest_year = int(str(latest_bs)[:4])
    latest_mmdd = str(latest_bs)[4:]
    prev_bs_date = f"{latest_year - 1}{latest_mmdd}"

    bs_dates_set = set(str(d) for d in bs["_report_date_str"].dropna().unique())
    if prev_bs_date in bs_dates_set:
        prev_row = bs[bs["_report_date_str"] == prev_bs_date].iloc[0]
        prev_debt = (
            safe_float(prev_row.get("SHORT_LOAN"))
            + safe_float(prev_row.get("NONCURRENT_LIAB_1YEAR"))
            + safe_float(prev_row.get("LONG_LOAN"))
            + safe_float(prev_row.get("BOND_PAYABLE"))
            + safe_float(prev_row.get("LEASE_LIAB"))
        )
        prev_equity = safe_float(prev_row.get("TOTAL_EQUITY"))
        result["interest_bearing_debt_prev"] = prev_debt
        result["total_equity_prev"] = prev_equity
        result["invested_capital_prev"] = prev_debt + prev_equity
        result["prev_bs_date"] = prev_bs_date
    else:
        result["interest_bearing_debt_prev"] = None
        result["total_equity_prev"] = None
        result["invested_capital_prev"] = None
        result["prev_bs_date"] = None

    # ── Income Statement: TTM ──
    # East Money columns: TOTAL_PROFIT (利润总额), FE_INTEREST_EXPENSE (利息费用),
    #                     INCOME_TAX (所得税费用)
    latest_inc, period_type_inc, fy_prev_inc, sq_prev_inc = detect_ttm_periods(inc)

    ebit_ttm = None
    if latest_inc:
        pretax_ttm = compute_ttm(
            inc, "TOTAL_PROFIT", latest_inc, period_type_inc, fy_prev_inc, sq_prev_inc
        )
        interest_exp_ttm = compute_ttm(
            inc, "FE_INTEREST_EXPENSE", latest_inc, period_type_inc, fy_prev_inc, sq_prev_inc
        )
        if pretax_ttm is not None:
            ebit_ttm = pretax_ttm + (interest_exp_ttm if interest_exp_ttm else 0)

    income_tax_ttm = None
    if latest_inc:
        income_tax_ttm = compute_ttm(
            inc, "INCOME_TAX", latest_inc, period_type_inc, fy_prev_inc, sq_prev_inc
        )

    result["ebit_ttm"] = ebit_ttm
    result["income_tax_ttm"] = income_tax_ttm
    result["ttm_period_type"] = period_type_inc if latest_inc else None
    result["ttm_base_date"] = str(latest_inc) if latest_inc else None

    # ── Cash Flow: TTM ──
    # East Money column: NETCASH_OPERATE (经营活动产生的现金流量净额)
    latest_cf, period_type_cf, fy_prev_cf, sq_prev_cf = detect_ttm_periods(cf)

    ocf_ttm = None
    if latest_cf:
        ocf_ttm = compute_ttm(
            cf,
            "NETCASH_OPERATE",
            latest_cf,
            period_type_cf,
            fy_prev_cf,
            sq_prev_cf,
        )

    result["ocf_ttm"] = ocf_ttm

    # ── Dividend TTM & PIR ──
    dps_ttm = compute_dividend_ttm(dividend_df, current_price)
    # Total dividend = dividend_per_share * total_shares
    # total_shares = market_cap / current_price
    if current_price and current_price > 0:
        total_shares = market_cap / current_price
        dividend_total = dps_ttm * total_shares
    else:
        total_shares = 0
        dividend_total = 0.0

    dividend_yield = dividend_total / market_cap if market_cap > 0 else None

    # ROIC (post-tax) = (EBIT_TTM - income_tax_TTM) / IC
    ic = result.get("invested_capital")
    roic_posttax = None
    if ebit_ttm is not None and ic and ic > 0:
        tax = income_tax_ttm if income_tax_ttm is not None else 0
        nopat = ebit_ttm - tax
        roic_posttax = nopat / ic

    # PIR = dividend_yield + IC/EV * ROIC
    pir = None
    ev = result.get("enterprise_value")
    if dividend_yield is not None and roic_posttax is not None and ev and ev > 0 and ic:
        pir = dividend_yield + (ic / ev) * roic_posttax

    result["dividend_ttm"] = dividend_total
    result["dividend_yield"] = dividend_yield
    result["roic_posttax"] = roic_posttax
    result["pir"] = pir

    return result


# ─────────────── CHECKPOINT SYSTEM ────────────────────────────

def ensure_checkpoint_dir():
    """Create checkpoint directory if needed."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


def load_checkpoint() -> set:
    """Return set of symbols already processed successfully."""
    if not CHECKPOINT_CSV.exists():
        return set()
    try:
        df = pd.read_csv(CHECKPOINT_CSV, dtype={"symbol": str}, usecols=["symbol"])
        return set(df["symbol"].tolist())
    except Exception:
        return set()


def init_checkpoint_csv():
    """Initialize checkpoint CSV with header if it doesn't exist."""
    ensure_checkpoint_dir()
    if not CHECKPOINT_CSV.exists():
        with open(CHECKPOINT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
            writer.writeheader()


def append_checkpoint(result: dict):
    """Append one result row to the checkpoint CSV (thread-safe for append)."""
    with open(CHECKPOINT_CSV, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        row = {col: result.get(col, None) for col in OUTPUT_COLUMNS}
        writer.writerow(row)


def save_progress_meta(total: int, completed: int, failed: int, failed_symbols: list):
    """Save progress metadata for monitoring."""
    ensure_checkpoint_dir()
    meta = {
        "total": total,
        "completed": completed,
        "failed": failed,
        "failed_symbols": failed_symbols[:200],
        "last_updated": datetime.now().isoformat(),
    }
    with open(CHECKPOINT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)


# ─────────────── WORKER ──────────────────────────────────────

def process_single_stock(args: tuple) -> Tuple[str, Optional[dict], Optional[str]]:
    """
    Worker function for ThreadPoolExecutor.
    Returns: (symbol, result_dict_or_None, error_msg_or_None)
    """
    symbol, name, market_cap, current_price = args
    em_code = get_em_code(symbol)
    try:
        financials = fetch_stock_financials(em_code)
        # Dividend data still uses Sina API (rate-limited, but one call per stock)
        dividend_df = fetch_dividend_data(symbol)
        metrics = compute_metrics_for_stock(
            symbol, name, market_cap, current_price,
            financials, dividend_df,
        )
        return (symbol, metrics, None)
    except Exception as e:
        return (symbol, None, f"{type(e).__name__}: {str(e)[:200]}")


# ─────────────── MAIN ────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Calculate financial metrics for all A-share stocks"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of parallel workers (default: 2, conservative for Sina API)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Stocks per batch (default: 50)",
    )
    parser.add_argument(
        "--batch-sleep",
        type=int,
        default=15,
        help="Seconds to sleep between batches (default: 15)",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=1.5,
        help="Min seconds between Sina API calls (default: 1.5)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only first N stocks (for testing)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_DIR / "ashare_financial_metrics.csv"),
        help="Output CSV path",
    )
    args = parser.parse_args()

    start_time = time.time()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Apply rate limit setting
    global _SINA_MIN_INTERVAL
    _SINA_MIN_INTERVAL = args.request_delay
    print(f"  Rate limit: {_SINA_MIN_INTERVAL}s between Sina API calls")

    # ── Phase 1: Get stock universe ──
    print("=" * 60)
    print("Phase 1: Fetching stock universe")
    print("=" * 60)
    universe = fetch_stock_universe()
    print(f"  Total A-share stocks: {len(universe)}")

    if args.limit:
        universe = universe.head(args.limit)
        print(f"  Limited to first {args.limit} stocks")

    # ── Check for resume ──
    completed_symbols = set()
    if args.resume:
        completed_symbols = load_checkpoint()
        print(f"  Checkpoint: {len(completed_symbols)} stocks already completed")
    else:
        # Fresh run: clear old checkpoint
        if CHECKPOINT_CSV.exists():
            CHECKPOINT_CSV.unlink()
        if CHECKPOINT_META.exists():
            CHECKPOINT_META.unlink()

    init_checkpoint_csv()

    # Filter out already-completed
    remaining = universe[~universe["symbol"].isin(completed_symbols)]
    print(f"  Remaining to process: {len(remaining)}")

    if remaining.empty:
        print("  Nothing to process. Use without --resume for a fresh run.")
        # Just consolidate existing checkpoint
        if CHECKPOINT_CSV.exists():
            final_df = pd.read_csv(CHECKPOINT_CSV, dtype={"symbol": str})
            final_df.to_csv(args.output, index=False, encoding="utf-8-sig")
            print(f"  Final CSV: {args.output} ({len(final_df)} rows)")
        return

    # ── Phase 2: Batch processing ──
    print()
    print("=" * 60)
    print(f"Phase 2: Processing {len(remaining)} stocks")
    print(f"  Workers: {args.workers}, Batch size: {args.batch_size}, "
          f"Batch sleep: {args.batch_sleep}s")
    print("=" * 60)

    tasks = [
        (row["symbol"], row["name"], row["market_cap"], row["current_price"])
        for _, row in remaining.iterrows()
    ]

    # Split into batches
    batches = []
    for i in range(0, len(tasks), args.batch_size):
        batches.append(tasks[i : i + args.batch_size])

    total_completed = len(completed_symbols)
    total_failed = 0
    all_errors = []

    for batch_idx, batch in enumerate(batches):
        batch_start = time.time()
        # Estimate remaining time
        if batch_idx > 0:
            avg_batch_time = (time.time() - start_time) / batch_idx
            remaining_batches = len(batches) - batch_idx
            eta_min = (avg_batch_time * remaining_batches) / 60
            eta_str = f", ETA: {eta_min:.0f}min"
        else:
            eta_str = ""
        print(f"\n  Batch {batch_idx + 1}/{len(batches)} "
              f"({len(batch)} stocks, "
              f"total progress: {total_completed}/{len(universe)}{eta_str})")

        batch_results = []
        batch_errors = []

        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {executor.submit(process_single_stock, t): t for t in batch}

            with tqdm(total=len(batch), desc=f"  Batch {batch_idx+1}") as pbar:
                for future in as_completed(futures):
                    symbol, result, error = future.result()
                    if result:
                        batch_results.append(result)
                        append_checkpoint(result)
                        total_completed += 1
                    else:
                        batch_errors.append({"symbol": symbol, "error": error})
                        total_failed += 1
                    pbar.update(1)

        all_errors.extend(batch_errors)

        batch_elapsed = time.time() - batch_start
        print(f"  Batch done: {len(batch_results)} OK, {len(batch_errors)} failed, "
              f"{batch_elapsed:.1f}s")

        # Save progress metadata
        save_progress_meta(
            total=len(universe),
            completed=total_completed,
            failed=total_failed,
            failed_symbols=[e["symbol"] for e in all_errors],
        )

        # Sleep between batches (except after last batch)
        if batch_idx < len(batches) - 1 and args.batch_sleep > 0:
            print(f"  Sleeping {args.batch_sleep}s before next batch...")
            time.sleep(args.batch_sleep)

    # ── Phase 3: Consolidate output ──
    print()
    print("=" * 60)
    print("Phase 3: Consolidating results")
    print("=" * 60)

    if CHECKPOINT_CSV.exists():
        final_df = pd.read_csv(CHECKPOINT_CSV, dtype={"symbol": str})
        # Deduplicate (keep last)
        final_df = final_df.drop_duplicates(subset=["symbol"], keep="last")
        final_df.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"  Saved {len(final_df)} rows to {args.output}")
    else:
        print("  WARNING: No checkpoint data found")
        final_df = pd.DataFrame()

    # Error report
    if all_errors:
        error_path = args.output.replace(".csv", "_errors.csv")
        pd.DataFrame(all_errors).to_csv(error_path, index=False, encoding="utf-8-sig")
        print(f"  {len(all_errors)} errors saved to {error_path}")

    # ── Summary ──
    elapsed = time.time() - start_time
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Total stocks:      {len(universe)}")
    print(f"  Successful:        {total_completed}")
    print(f"  Failed:            {total_failed}")
    if len(universe) > 0:
        print(f"  Success rate:      {total_completed / len(universe) * 100:.1f}%")
    print(f"  Elapsed time:      {elapsed / 60:.1f} minutes")
    print(f"  Output:            {args.output}")
    if all_errors:
        print(f"  Error log:         {args.output.replace('.csv', '_errors.csv')}")
    print()


if __name__ == "__main__":
    main()
