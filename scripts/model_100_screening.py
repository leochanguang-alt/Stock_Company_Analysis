#!/usr/bin/env python3
"""
Model 100 — 量化选股分析流程
=============================
每月运行一次，从 TradingView 市场数据出发，经过多轮筛选找出高质量标的。

依赖:
    pip install supabase python-dotenv

外部服务:
    - Supabase: 存储市场数据 (share_a_market, us_market, hkse_market)
    - OpenBB API: http://127.0.0.1:6900  (本地启动: openbb-api --host 127.0.0.1 --port 6900)

使用:
    python scripts/model_100_screening.py --date 2026-02-28
    python scripts/model_100_screening.py --date 2026-02-28 --skip-fundamentals   # 跳过耗时的基本面获取
"""

import argparse
import csv
import json
import os
import sys
import time
import urllib.request
from collections import Counter
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

OPENBB_BASE = "http://127.0.0.1:6900/api/v1"

MARKET_TABLES = {
    "CN": "share_a_market",
    "US": "us_market",
    "HK": "hkse_market",
}

# 筛选参数 — 修改这里即可调整策略
PARAMS = {
    "fvgap_min": 2.0,
    "roic_min": 0.1,
    "mkt_cap_min_cn_hk": 1_000_000_000,  # 1bn
    "mkt_cap_min_us": 1_000_000_000,
    "exclude_sectors": ["Finance"],
    "sr_period_short": 60,   # 周线 SR 短周期
    "sr_period_long": 120,   # 周线 SR 长周期
}

# Supabase 字段映射
FIELD_MAP = {
    "symbol": {
        "CN": "symbol",
        "US": "symbol",
        "HK": "symbol",
    },
    "ocf_ttm": "cash_from_operating_activities_trailing_12_months",
    "roic": "return_on_invested_capital_percent_trailing_12_months",
    "ev": "enterprise_value_fq",
    "mkt_cap": "market_capitalization",
    "ema": "exponential_moving_average_120_1_day",
    "sma": "simple_moving_average_120_1_day",
    "description": "description",
    "sector": "sector",
    "industry": "industry",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def init_supabase():
    env = dotenv_values(PROJECT_ROOT / ".env")
    url = env.get("SUPABASE_URL") or env.get("NEXT_PUBLIC_SUPABASE_URL")
    key = env.get("SUPABASE_SERVICE_ROLE_KEY") or env.get("SUPABASE_KEY")
    if not url or not key:
        sys.exit("ERROR: SUPABASE_URL / SUPABASE_KEY not found in .env")
    from supabase import create_client
    return create_client(url, key)


def fetch_openbb(endpoint, params, timeout=20):
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{OPENBB_BASE}/{endpoint}?{qs}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read()).get("results", [])
    except Exception:
        return []


def safe_float(v):
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def to_yf_symbol(symbol, market):
    """将代码转成 yfinance 格式"""
    if market == "CN":
        s = str(symbol).zfill(6)
        return s + ".SS" if s[0] in ("6", "9") else s + ".SZ"
    elif market == "HK":
        s = str(symbol).lstrip("0") or "0"
        return s + ".HK"
    return str(symbol)


def fmt_pct(new, old):
    if new is None or old is None or old == 0:
        return ""
    return f"{(new - old) / abs(old) * 100:.1f}%"


def fmt_num(v, scale=1):
    if v is None:
        return ""
    return f"{v / scale:.0f}"


# ---------------------------------------------------------------------------
# Step 1: 从 Supabase 获取市场数据并计算 FV / FvGap
# ---------------------------------------------------------------------------

def step1_fetch_and_calc_fv(sb, download_date):
    """从三大市场表获取数据, 计算 FV 和 FvGap"""
    print("\n" + "=" * 70)
    print("STEP 1: 获取市场数据 & 计算 FV / FvGap")
    print("=" * 70)

    # 使用 select(*) 避免含 % 的列名导致 PostgREST 解析错误
    all_rows = []
    for market, table in MARKET_TABLES.items():
        batch, offset = [], 0
        while True:
            resp = (sb.table(table).select("*")
                    .eq("download_date", download_date)
                    .range(offset, offset + 999)
                    .execute())
            chunk = resp.data or []
            batch.extend(chunk)
            if len(chunk) < 1000:
                break
            offset += 1000

        data = batch
        print(f"  {market} ({table}): {len(data)} rows")
        for r in data:
            ocf = safe_float(r.get("cash_from_operating_activities_trailing_12_months"))
            roic_raw = (safe_float(r.get("return_on_invested_capital_percent_trailing_12_months"))
                        or safe_float(r.get("return_on_invested_capital_%_trailing_12_months")))
            ev = safe_float(r.get("enterprise_value"))
            mkt_cap = safe_float(r.get("market_capitalization"))

            roic = roic_raw / 100 if roic_raw is not None else None
            if None in (ocf, roic, ev, mkt_cap) or mkt_cap == 0:
                continue

            # FV = (OCF_TTM * (ROIC*100 + 5)) - (EV - MktCap)
            fv = (ocf * (roic * 100 + 5)) - (ev - mkt_cap)
            fvgap = fv / mkt_cap

            all_rows.append({
                "market": market,
                "symbol": str(r.get("symbol", "")),
                "description": r.get("description", ""),
                "sector": r.get("sector", ""),
                "industry": r.get("industry", ""),
                "ocf_ttm": ocf,
                "roic": roic,
                "ev": ev,
                "mkt_cap": mkt_cap,
                "fv": fv,
                "fvgap": fvgap,
                "ema": safe_float(r.get("exponential_moving_average_120_1_day")),
                "sma": safe_float(r.get("simple_moving_average_120_1_day")),
                "download_date": download_date,
                "table": table,
            })

    print(f"  合计有效行: {len(all_rows)}")
    return all_rows


# ---------------------------------------------------------------------------
# Step 2: 多轮筛选
# ---------------------------------------------------------------------------

def step2_filter(rows):
    """依次: FvGap, ROIC, Sector, MktCap, EMA/SMA"""
    print("\n" + "=" * 70)
    print("STEP 2: 多轮筛选")
    print("=" * 70)

    # 2a: FvGap > min & ROIC > min
    filtered = [r for r in rows
                if r["fvgap"] > PARAMS["fvgap_min"]
                and r["roic"] > PARAMS["roic_min"]]
    print(f"  2a) FvGap>{PARAMS['fvgap_min']} & ROIC>{PARAMS['roic_min']}: {len(filtered)}")

    # 2b: 排除金融行业
    filtered = [r for r in filtered
                if r["sector"] not in PARAMS["exclude_sectors"]]
    print(f"  2b) 排除金融行业: {len(filtered)}")

    # 2c: CN/HK 市值 > 1bn
    filtered = [r for r in filtered
                if not (r["market"] in ("CN", "HK") and r["mkt_cap"] < PARAMS["mkt_cap_min_cn_hk"])]
    print(f"  2c) CN/HK 市值>1bn: {len(filtered)}")

    # 2d: US 市值 > 1bn
    filtered = [r for r in filtered
                if not (r["market"] == "US" and r["mkt_cap"] < PARAMS["mkt_cap_min_us"])]
    print(f"  2d) US 市值>1bn: {len(filtered)}")

    # 2e: EMA >= SMA
    filtered = [r for r in filtered
                if r["ema"] is not None and r["sma"] is not None and r["ema"] >= r["sma"]]
    print(f"  2e) EMA>=SMA: {len(filtered)}")

    return filtered


# ---------------------------------------------------------------------------
# Step 3: 周线成交量加权动量 (LON / LONMA) — Qianlong 指标
# ---------------------------------------------------------------------------

def _resample_weekly(daily):
    daily.sort(key=lambda x: x["date"])
    weeks = []
    week = None
    for b in daily:
        d = datetime.strptime(b["date"][:10], "%Y-%m-%d")
        wk_key = d.isocalendar()[:2]
        if week is None or week["key"] != wk_key:
            if week is not None:
                weeks.append(week)
            week = {
                "key": wk_key, "date": b["date"][:10],
                "open": b["open"], "high": b["high"],
                "low": b["low"], "close": b["close"], "volume": b["volume"],
            }
        else:
            week["date"] = b["date"][:10]
            week["high"] = max(week["high"], b["high"])
            week["low"] = min(week["low"], b["low"])
            week["close"] = b["close"]
            week["volume"] += b["volume"]
    if week is not None:
        weeks.append(week)
    return weeks


def _calc_lon_lonma(weekly_bars):
    """计算 Qianlong 指标的 LON 和 LONMA (10 日 SMA)"""
    n = len(weekly_bars)
    if n < 12:
        return None, None

    close = [b["close"] for b in weekly_bars]
    high = [b["high"] for b in weekly_bars]
    low = [b["low"] for b in weekly_bars]
    volume = [b["volume"] for b in weekly_bars]

    # VID = (vol + vol[1]) / 100 / ((highest(high,2) - lowest(low,2)) * 100)
    vid = [0.0] * n
    for i in range(1, n):
        vol_sum = volume[i] + volume[i - 1]
        price_range = max(high[i], high[i - 1]) - min(low[i], low[i - 1])
        vid[i] = (vol_sum / 100) / (price_range * 100) if price_range != 0 else 0

    # RC = (close - close[1]) * VID
    rc = [0.0] * n
    for i in range(1, n):
        rc[i] = (close[i] - close[i - 1]) * vid[i]

    # LONG = cumulative sum of RC
    long_val = [0.0] * n
    for i in range(1, n):
        long_val[i] = long_val[i - 1] + rc[i]

    # src = long - long[1] cumulative (same as long itself)
    src = long_val[:]

    # DIFF = SMA(src, 10, 1) — 通达信递推
    diff = [0.0] * n
    diff[0] = src[0]
    for i in range(1, n):
        diff[i] = (1 * src[i] + 9 * diff[i - 1]) / 10

    # DEA = SMA(src, 20, 1) — 通达信递推
    dea = [0.0] * n
    dea[0] = src[0]
    for i in range(1, n):
        dea[i] = (1 * src[i] + 19 * dea[i - 1]) / 20

    # LON = DIFF - DEA
    lon = [diff[i] - dea[i] for i in range(n)]

    # LONMA = SMA(LON, 10)
    if n < 10:
        return lon[-1], None
    lonma = sum(lon[-10:]) / 10
    return lon[-1], lonma


def step3_lon_filter(rows):
    """计算周线 LON/LONMA, 筛掉 lon < lonma"""
    print("\n" + "=" * 70)
    print("STEP 3: 周线 LON/LONMA 筛选 (Qianlong 指标)")
    print("=" * 70)

    kept = []
    skipped = 0
    errors = 0

    for idx, r in enumerate(rows):
        yf = to_yf_symbol(r["symbol"], r["market"])
        daily = fetch_openbb("equity/price/historical", {
            "symbol": yf, "provider": "yfinance",
            "start_date": "2023-03-01", "end_date": datetime.now().strftime("%Y-%m-%d"),
        })
        if not daily or len(daily) < 60:
            errors += 1
            continue

        weekly = _resample_weekly(daily)
        lon, lonma = _calc_lon_lonma(weekly)

        if lon is None or lonma is None:
            errors += 1
            continue

        if lon < lonma:
            skipped += 1
        else:
            r["weekly_lon"] = round(lon, 4)
            r["weekly_lonma"] = round(lonma, 4)
            kept.append(r)

        if (idx + 1) % 50 == 0:
            print(f"    [{idx + 1}/{len(rows)}] kept={len(kept)} skipped={skipped} errors={errors}")

    print(f"  LON>=LONMA: {len(kept)} (筛掉 {skipped}, 错误 {errors})")
    return kept


# ---------------------------------------------------------------------------
# Step 4: 周线 Sharp-Race (SR60 / SR120)
# ---------------------------------------------------------------------------

def _calc_weekly_sr(weekly_bars, period):
    n = len(weekly_bars)
    if n < period + 1:
        return None
    close = [b["close"] for b in weekly_bars]
    ret = [0.0] * n
    for i in range(1, n):
        if close[i - 1] != 0:
            ret[i] = (close[i] - close[i - 1]) / close[i - 1]
    seg = ret[n - period: n]
    mean = sum(seg) / period
    var = sum((x - mean) ** 2 for x in seg) / period
    std = var ** 0.5
    return mean / std if std != 0 else None


def step4_sr_filter(rows):
    """计算周线 SR60/SR120, 筛掉 SR60 < SR120"""
    print("\n" + "=" * 70)
    print("STEP 4: 周线 SR60/SR120 筛选 (Sharp-Race)")
    print("=" * 70)

    kept = []
    skipped = 0
    errors = 0
    sp = PARAMS["sr_period_short"]
    lp = PARAMS["sr_period_long"]

    for idx, r in enumerate(rows):
        yf = to_yf_symbol(r["symbol"], r["market"])
        daily = fetch_openbb("equity/price/historical", {
            "symbol": yf, "provider": "yfinance",
            "start_date": "2023-03-01", "end_date": datetime.now().strftime("%Y-%m-%d"),
        })
        if not daily or len(daily) < 60:
            errors += 1
            continue

        weekly = _resample_weekly(daily)
        sr_short = _calc_weekly_sr(weekly, sp)
        sr_long = _calc_weekly_sr(weekly, lp)

        if sr_short is None or sr_long is None:
            errors += 1
        elif sr_short < sr_long:
            skipped += 1
        else:
            r["weekly_sr60"] = round(sr_short, 6)
            r["weekly_sr120"] = round(sr_long, 6)
            kept.append(r)

        if (idx + 1) % 50 == 0:
            print(f"    [{idx + 1}/{len(rows)}] kept={len(kept)} skipped={skipped} errors={errors}")

    print(f"  SR60>=SR120: {len(kept)} (筛掉 {skipped}, 错误 {errors})")
    return kept


# ---------------------------------------------------------------------------
# Step 5: 获取 3 年基本面 + 6 季度 OCF
# ---------------------------------------------------------------------------

def _extract_ocf(row):
    return (safe_float(row.get("operating_cash_flow")) or
            safe_float(row.get("cash_flowsfromusedin_operating_activities_direct")))


def step5_fundamentals(rows):
    """通过 OpenBB 获取 3 年年报数据 + 6 季度经营现金流"""
    print("\n" + "=" * 70)
    print("STEP 5: 获取 3 年基本面数据 + 6 季度 OCF")
    print("=" * 70)

    M = 1_000_000
    results = []

    for idx, r in enumerate(rows):
        yf = to_yf_symbol(r["symbol"], r["market"])

        cash_a = fetch_openbb("equity/fundamental/cash", {"symbol": yf, "provider": "yfinance", "period": "annual"})
        income_a = fetch_openbb("equity/fundamental/income", {"symbol": yf, "provider": "yfinance", "period": "annual"})
        balance_a = fetch_openbb("equity/fundamental/balance", {"symbol": yf, "provider": "yfinance", "period": "annual"})
        cash_q = fetch_openbb("equity/fundamental/cash", {"symbol": yf, "provider": "yfinance", "period": "quarter"})

        annual = []
        for i in range(min(3, max(len(cash_a), len(income_a), len(balance_a)))):
            ca = cash_a[i] if i < len(cash_a) else {}
            inc = income_a[i] if i < len(income_a) else {}
            bal = balance_a[i] if i < len(balance_a) else {}

            ocf = _extract_ocf(ca)
            rev = safe_float(inc.get("total_revenue")) or safe_float(inc.get("operating_revenue"))
            assets = safe_float(bal.get("total_assets"))
            liab = safe_float(bal.get("total_liabilities_net_minority_interest"))
            debt = safe_float(bal.get("total_debt"))
            equity = (safe_float(bal.get("total_equity_non_controlling_interests"))
                      or safe_float(bal.get("common_stock_equity")))
            ebit = safe_float(inc.get("ebit"))
            tax = safe_float(inc.get("tax_provision"))
            pretax = safe_float(inc.get("total_pre_tax_income"))
            tax_rate = (tax / pretax) if (tax and pretax and pretax != 0) else None

            roic_calc = None
            if ebit is not None and tax_rate is not None and debt is not None and equity is not None:
                nopat = ebit * (1 - tax_rate)
                invested = debt + equity
                if invested != 0:
                    roic_calc = nopat / invested

            date_str = (ca.get("period_ending") or inc.get("period_ending")
                        or bal.get("period_ending") or "")
            annual.append({
                "date": str(date_str)[:10],
                "ocf": ocf, "revenue": rev,
                "total_assets": assets, "total_liabilities": liab,
                "roic": roic_calc,
            })

        q_list = []
        for c in cash_q[:6]:
            ocf = _extract_ocf(c)
            q_list.append({"date": str(c.get("period_ending", ""))[:10], "ocf": ocf})

        out = {
            "market": r["market"], "symbol": r["symbol"],
            "description": r["description"],
            "sector": r.get("sector", ""), "industry": r.get("industry", ""),
            "current_roic": r["roic"], "fvgap": r["fvgap"],
            "weekly_lon": r.get("weekly_lon", ""),
            "weekly_lonma": r.get("weekly_lonma", ""),
            "weekly_sr60": r.get("weekly_sr60", ""),
            "weekly_sr120": r.get("weekly_sr120", ""),
        }

        for i in range(3):
            yr = f"Y{i + 1}"
            if i < len(annual):
                d = annual[i]
                out[f"{yr}_date"] = d["date"]
                out[f"{yr}_ocf_m"] = fmt_num(d["ocf"], M)
                out[f"{yr}_revenue_m"] = fmt_num(d["revenue"], M)
                out[f"{yr}_assets_m"] = fmt_num(d["total_assets"], M)
                out[f"{yr}_liabilities_m"] = fmt_num(d["total_liabilities"], M)
                out[f"{yr}_roic"] = f"{d['roic'] * 100:.1f}%" if d["roic"] is not None else ""
            else:
                for sf in ["date", "ocf_m", "revenue_m", "assets_m", "liabilities_m", "roic"]:
                    out[f"{yr}_{sf}"] = ""

        if len(annual) >= 3:
            out["ocf_3y_chg"] = fmt_pct(annual[0].get("ocf"), annual[2].get("ocf"))
            out["rev_3y_chg"] = fmt_pct(annual[0].get("revenue"), annual[2].get("revenue"))
            out["assets_3y_chg"] = fmt_pct(annual[0].get("total_assets"), annual[2].get("total_assets"))
            out["liab_3y_chg"] = fmt_pct(annual[0].get("total_liabilities"), annual[2].get("total_liabilities"))
        else:
            out["ocf_3y_chg"] = out["rev_3y_chg"] = out["assets_3y_chg"] = out["liab_3y_chg"] = ""

        for i in range(6):
            qi = f"Q{i + 1}"
            if i < len(q_list) and q_list[i]["ocf"] is not None:
                out[f"{qi}_date"] = q_list[i]["date"]
                out[f"{qi}_ocf_m"] = fmt_num(q_list[i]["ocf"], M)
            else:
                out[f"{qi}_date"] = q_list[i]["date"] if i < len(q_list) else ""
                out[f"{qi}_ocf_m"] = ""

        results.append(out)

        if (idx + 1) % 20 == 0:
            print(f"    [{idx + 1}/{len(rows)}] done")

    print(f"  基本面数据获取完成: {len(results)} 家")
    return results


# ---------------------------------------------------------------------------
# Step 6: 输出摘要统计
# ---------------------------------------------------------------------------

def step6_summary(fundamentals, candidates):
    """打印分析摘要"""
    print("\n" + "=" * 70)
    print("STEP 6: 分析摘要")
    print("=" * 70)

    # 市场分布
    mkt_cnt = Counter(r["market"] for r in candidates)
    print(f"\n  最终候选: {len(candidates)} 家")
    for m, n in mkt_cnt.most_common():
        print(f"    {m}: {n}")

    if not fundamentals:
        return

    def parse_pct(s):
        try:
            return float(str(s).replace("%", ""))
        except (ValueError, TypeError):
            return None

    # 3 年变化统计
    print("\n  3 年变化总览:")
    for name, field in [("OCF", "ocf_3y_chg"), ("Revenue", "rev_3y_chg"),
                        ("Assets", "assets_3y_chg"), ("Liabilities", "liab_3y_chg")]:
        vals = [parse_pct(r.get(field)) for r in fundamentals]
        vals = [v for v in vals if v is not None]
        if vals:
            avg = sum(vals) / len(vals)
            med = sorted(vals)[len(vals) // 2]
            pos = sum(1 for v in vals if v > 0)
            print(f"    {name:15s}  均值={avg:+.1f}%  中位数={med:+.1f}%  ↑{pos}家 ↓{len(vals) - pos}家")

    # 明星公司
    stars = []
    for r in fundamentals:
        ocf_c = parse_pct(r.get("ocf_3y_chg"))
        rev_c = parse_pct(r.get("rev_3y_chg"))
        ast_c = parse_pct(r.get("assets_3y_chg"))
        lib_c = parse_pct(r.get("liab_3y_chg"))
        if (ocf_c is not None and rev_c is not None
                and ast_c is not None and lib_c is not None
                and ocf_c > 20 and rev_c > 10 and ast_c > lib_c):
            stars.append(r)
    stars.sort(key=lambda x: parse_pct(x.get("ocf_3y_chg")) or 0, reverse=True)

    print(f"\n  明星公司 (OCF>+20%, Rev>+10%, Assets增速>Liab增速): {len(stars)} 家")
    for r in stars[:15]:
        print(f"    {r['market']:3s} {r['symbol']:8s} {r['description'][:28]:28s}"
              f"  OCF={r.get('ocf_3y_chg', ''):>8s}  Rev={r.get('rev_3y_chg', ''):>8s}"
              f"  ROIC={r.get('Y1_roic', ''):>6s}")


# ---------------------------------------------------------------------------
# Step 7: 保存结果
# ---------------------------------------------------------------------------

def save_csv(rows, path, fieldnames=None):
    if not rows:
        print(f"  无数据, 跳过 {path}")
        return
    fieldnames = fieldnames or list(rows[0].keys())
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  已保存: {path} ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Model 100 — 量化选股分析流程")
    parser.add_argument("--date", required=True, help="数据日期, 如 2026-02-28")
    parser.add_argument("--skip-fundamentals", action="store_true",
                        help="跳过 Step 5 (基本面获取, 耗时 ~2min)")
    parser.add_argument("--skip-indicators", action="store_true",
                        help="跳过 Step 3+4 (LON/SR 计算, 耗时 ~4min)")
    args = parser.parse_args()

    download_date = args.date

    print("=" * 70)
    print(f"  Model 100 量化选股分析")
    print(f"  数据日期: {download_date}")
    print(f"  运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    t0 = time.time()

    # Step 1
    sb = init_supabase()
    all_rows = step1_fetch_and_calc_fv(sb, download_date)

    # Step 2
    filtered = step2_filter(all_rows)

    candidates_path = OUTPUT_DIR / f"fv_gap_candidates_{download_date}.csv"

    if args.skip_indicators:
        print("\n  [跳过 Step 3+4: LON/SR 计算]")
        candidates = filtered
    else:
        # Step 3
        after_lon = step3_lon_filter(filtered)

        # Step 4
        candidates = step4_sr_filter(after_lon)

    # 保存筛选结果
    save_csv(candidates, candidates_path)

    # Step 5
    fundamentals = []
    if not args.skip_fundamentals:
        fundamentals = step5_fundamentals(candidates)
        fund_path = OUTPUT_DIR / f"stock_fundamentals_3y_{len(candidates)}.csv"
        save_csv(fundamentals, fund_path)
    else:
        print("\n  [跳过 Step 5: 基本面获取]")

    # Step 6
    step6_summary(fundamentals, candidates)

    elapsed = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"  完成! 总耗时: {elapsed / 60:.1f} 分钟")
    print(f"  最终候选: {len(candidates)} 家")
    print(f"  输出文件: {candidates_path}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
