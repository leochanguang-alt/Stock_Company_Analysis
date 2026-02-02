#!/usr/bin/env python3
import os
import sys
import datetime as dt
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import akshare as ak
from dotenv import load_dotenv
from supabase import create_client


META_COLS = ["数据源", "是否审计", "公告日期", "币种", "类型", "更新日期"]
REPORT_COL = "报告日"


def parse_args(argv: List[str]) -> Dict[str, str]:
    symbol = "000333"
    years = "10"
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
    if s.startswith("SZ") or s.startswith("SH"):
        s = s[2:]
    # 确保股票代码是 6 位（补前导零）
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


def get_company_info_from_supabase(symbol: str) -> Dict[str, Optional[str]]:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        return {"market": None, "name": None}
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
            return {"market": None, "name": None}
        exchange = str(resp.data[0].get("exchange") or "").upper()
        name = resp.data[0].get("description")
        if exchange == "SSE":
            return {"market": "SH", "name": name}
        if exchange == "SZSE":
            return {"market": "SZ", "name": name}
        return {"market": None, "name": name}
    except Exception:
        return {"market": None, "name": None}


def fetch_with_fallback(fetch_fn, symbol: str, market: Optional[str]):
    try:
        df = fetch_fn(symbol)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
    except Exception:
        pass
    alt = market_prefixed_symbol(symbol, market)
    try:
        df = fetch_fn(alt)
        return df
    except Exception as err:
        raise err


def with_required_cols(df: pd.DataFrame, report_col_candidates: List[str]) -> pd.DataFrame:
    # Normalize report date column
    report_col = None
    for c in report_col_candidates:
        if c in df.columns:
            report_col = c
            break
    if report_col is None:
        raise ValueError("未找到报告日字段")
    if report_col != REPORT_COL:
        df = df.rename(columns={report_col: REPORT_COL})

    # Normalize common AKShare meta columns if present
    rename_map = {}
    if "NOTICE_DATE" in df.columns:
        rename_map["NOTICE_DATE"] = "公告日期"
    if "UPDATE_DATE" in df.columns:
        rename_map["UPDATE_DATE"] = "更新日期"
    if "CURRENCY" in df.columns:
        rename_map["CURRENCY"] = "币种"
    if "REPORT_TYPE" in df.columns:
        rename_map["REPORT_TYPE"] = "类型"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Ensure meta columns exist
    for col in META_COLS:
        if col not in df.columns:
            df[col] = None

    if "数据源" in df.columns:
        df["数据源"] = df["数据源"].fillna("EastMoney")
    else:
        df["数据源"] = "EastMoney"

    # Reorder: report date -> data cols -> meta cols
    data_cols = [c for c in df.columns if c not in ([REPORT_COL] + META_COLS)]
    df = df[[REPORT_COL] + data_cols + META_COLS]
    return df


def format_report_date(val: Optional[str]) -> Optional[str]:
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    if s.isdigit() and len(s) == 8:
        return s
    try:
        d = pd.to_datetime(s)
        return d.strftime("%Y%m%d")
    except Exception:
        return s


def filter_by_years(df: pd.DataFrame, years: int = 10) -> pd.DataFrame:
    """Filter dataframe to keep only data from the last N years"""
    if df.empty or REPORT_COL not in df.columns:
        return df
    df = df.copy()
    # Convert report date to datetime for comparison
    df["_report_date_dt"] = pd.to_datetime(df[REPORT_COL], errors="coerce")
    cutoff = dt.datetime.now() - dt.timedelta(days=years * 365)
    df = df[df["_report_date_dt"] >= cutoff]
    df = df.drop(columns=["_report_date_dt"])
    return df


def save_financial_csv(df: pd.DataFrame, path: str, years: int = 10) -> None:
    df = filter_by_years(df, years)
    df[REPORT_COL] = df[REPORT_COL].apply(format_report_date)
    df.to_csv(path, index=False)


def fetch_balance_sheet(symbol: str, market: Optional[str]) -> pd.DataFrame:
    df = fetch_with_fallback(ak.stock_balance_sheet_by_report_em, symbol, market)
    return with_required_cols(df, ["报告期", "报告日期", "报告日", "REPORT_DATE"])


def fetch_income_statement(symbol: str, market: Optional[str]) -> pd.DataFrame:
    df = fetch_with_fallback(ak.stock_profit_sheet_by_report_em, symbol, market)
    return with_required_cols(df, ["报告期", "报告日期", "报告日", "REPORT_DATE"])


def fetch_cash_flow(symbol: str, market: Optional[str]) -> pd.DataFrame:
    df = fetch_with_fallback(ak.stock_cash_flow_sheet_by_report_em, symbol, market)
    return with_required_cols(df, ["报告期", "报告日期", "报告日", "REPORT_DATE"])


def fetch_market_cap(symbol: str, years: int) -> pd.DataFrame:
    df = ak.stock_value_em(symbol=symbol)
    if df.empty:
        return df
    df = df.rename(columns={"数据日期": "date", "总市值": "total_mv"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    cutoff = dt.datetime.now() - dt.timedelta(days=years * 365)
    df = df[df["date"] >= cutoff]
    df["mkt_cap_billion_cny"] = df["total_mv"] / 1e9
    df = df.sort_values("date")[["date", "mkt_cap_billion_cny"]]
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df


def fetch_main_stock_holder(symbol: str, market: Optional[str]) -> pd.DataFrame:
    df = fetch_with_fallback(ak.stock_main_stock_holder, symbol, market)
    return df


def build_top10_from_main(df: pd.DataFrame, symbol: str, market: Optional[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "名次", "股东名称", "股份类型", "持股数", "占总股本持股比例",
            "增减", "变动比率", "报告期", "股票代码"
        ])
    df = df.copy()
    df["报告期"] = pd.to_datetime(df["截至日期"], errors="coerce").dt.strftime("%Y%m%d")
    df["股票代码"] = market_prefixed_symbol(symbol, market)
    df = df.rename(columns={
        "编号": "名次",
        "股东名称": "股东名称",
        "股本性质": "股份类型",
        "持股数量": "持股数",
        "持股比例": "占总股本持股比例",
    })
    df["增减"] = ""
    df["变动比率"] = ""
    cols = ["名次", "股东名称", "股份类型", "持股数", "占总股本持股比例", "增减", "变动比率", "报告期", "股票代码"]
    return df[cols]


def fetch_holder_count(symbol: str, years: int = 10) -> pd.DataFrame:
    """从巨潮资讯获取股东人数集中度数据（并行加速版）"""
    quarter_ends = []
    today = dt.datetime.now()
    for y in range(years + 1):
        year = today.year - y
        for q in ["1231", "0930", "0630", "0331"]:
            date_str = f"{year}{q}"
            if int(date_str) >= 20170331 and int(date_str) <= int(today.strftime("%Y%m%d")):
                quarter_ends.append(date_str)
    
    def fetch_single_quarter(date_str: str):
        """获取单个季度的数据"""
        try:
            df = ak.stock_hold_num_cninfo(date=date_str)
            if df is not None and not df.empty:
                filtered = df[df["证券代码"] == symbol]
                if not filtered.empty:
                    return filtered
        except Exception:
            pass
        return None
    
    all_data = []
    # 使用线程池并行获取数据，最多8个并发
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_single_quarter, d): d for d in quarter_ends}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                all_data.append(result)
    
    if not all_data:
        return pd.DataFrame(columns=[
            "证券代码", "证券简称", "变动日期", "本期股东人数", "上期股东人数",
            "股东人数增幅", "本期人均持股数量", "上期人均持股数量", "人均持股数量增幅"
        ])
    
    result = pd.concat(all_data, ignore_index=True)
    result = result.drop_duplicates(subset=["证券代码", "变动日期"])
    result = result.sort_values("变动日期")
    return result


def wide_to_long(df: pd.DataFrame, symbol: str, statement_type: str) -> pd.DataFrame:
    # Exclude metadata columns that contain non-numeric data
    exclude_cols = set([REPORT_COL] + META_COLS + [
        "SECUCODE", "SECURITY_CODE", "SECURITY_NAME_ABBR", "ORG_CODE", "ORG_TYPE",
        "REPORT_TYPE_CODE", "DATE_TYPE_CODE", "REPORT_DATE_NAME", "SECURITY_TYPE_CODE",
        "TRADE_MARKET_CODE", "CURRENCY", "STD_ITEM_CODE", "OPINION_TYPE",
        "BZ", "MBI", "YEAR_TYPE",
    ])
    data_cols = [c for c in df.columns if c not in exclude_cols]
    rows = []
    for _, row in df.iterrows():
        report_date = row.get(REPORT_COL)
        for col in data_cols:
            val = row.get(col)
            # Skip non-numeric values
            if isinstance(val, str) and not val.replace('.', '').replace('-', '').replace('e', '').replace('E', '').isdigit():
                continue
            rows.append({
                "股票代码": symbol,
                "报告日": report_date,
                "报表类型": statement_type,
                "财务科目": col,
                "数值": val,
                "数据源": row.get("数据源"),
                "是否审计": row.get("是否审计"),
                "公告日期": row.get("公告日期"),
                "币种": row.get("币种"),
                "类型": row.get("类型"),
                "更新日期": row.get("更新日期"),
            })
    return pd.DataFrame(rows)


def main():
    import time
    start_time = time.time()
    
    args = parse_args(sys.argv[1:])
    symbol = normalize_symbol(args["symbol"])
    years = int(args["years"])
    company_info = get_company_info_from_supabase(symbol)
    market = company_info.get("market")
    name = company_info.get("name")

    os.makedirs("outputs", exist_ok=True)
    
    print(f"=" * 50)
    print(f"开始并行下载 {symbol} ({name or '未知'}) 数据...")
    print(f"=" * 50)

    # 定义所有下载任务
    results = {}
    errors = []
    
    def download_balance_sheet():
        try:
            t0 = time.time()
            df = fetch_balance_sheet(symbol, market)
            path = os.path.join("outputs", f"{symbol}_balance_sheet_10y.csv")
            save_financial_csv(df, path)
            print(f"  [✓] 资产负债表 ({time.time()-t0:.1f}s)", flush=True)
            return ("balance", df, path)
        except Exception as e:
            errors.append(f"资产负债表: {e}")
            return ("balance", pd.DataFrame(), None)
    
    def download_income_statement():
        try:
            t0 = time.time()
            df = fetch_income_statement(symbol, market)
            path = os.path.join("outputs", f"{symbol}_income_statement_10y.csv")
            save_financial_csv(df, path)
            print(f"  [✓] 利润表 ({time.time()-t0:.1f}s)", flush=True)
            return ("income", df, path)
        except Exception as e:
            errors.append(f"利润表: {e}")
            return ("income", pd.DataFrame(), None)
    
    def download_cash_flow():
        try:
            t0 = time.time()
            df = fetch_cash_flow(symbol, market)
            path = os.path.join("outputs", f"{symbol}_cash_flow_10y.csv")
            save_financial_csv(df, path)
            print(f"  [✓] 现金流量表 ({time.time()-t0:.1f}s)", flush=True)
            return ("cash_flow", df, path)
        except Exception as e:
            errors.append(f"现金流量表: {e}")
            return ("cash_flow", pd.DataFrame(), None)
    
    def download_mkt_cap():
        try:
            t0 = time.time()
            df = fetch_market_cap(symbol, years)
            path = os.path.join("outputs", f"{symbol}_mkt_cap_10y.csv")
            df.to_csv(path, index=False)
            print(f"  [✓] 市值历史 ({time.time()-t0:.1f}s)", flush=True)
            return ("mkt_cap", df, path)
        except Exception as e:
            errors.append(f"市值历史: {e}")
            return ("mkt_cap", pd.DataFrame(), None)
    
    def download_top10():
        try:
            t0 = time.time()
            main_holders = fetch_main_stock_holder(symbol, market)
            df = build_top10_from_main(main_holders, symbol, market)
            path = os.path.join("outputs", f"{symbol}_top10_shareholders_10y.csv")
            df.to_csv(path, index=False)
            print(f"  [✓] 前十大股东 ({time.time()-t0:.1f}s)", flush=True)
            return ("top10", df, path)
        except Exception as e:
            errors.append(f"前十大股东: {e}")
            return ("top10", pd.DataFrame(), None)
    
    def download_holder_count():
        try:
            t0 = time.time()
            df = fetch_holder_count(symbol, years)
            path = os.path.join("outputs", f"{symbol}_holder_count_concentration_10y.csv")
            df.to_csv(path, index=False)
            print(f"  [✓] 股东人数集中度 ({time.time()-t0:.1f}s)", flush=True)
            return ("holder_count", df, path)
        except Exception as e:
            errors.append(f"股东人数集中度: {e}")
            return ("holder_count", pd.DataFrame(), None)
    
    # 并行执行所有下载任务
    print("并行下载中...", flush=True)
    download_tasks = [
        download_balance_sheet,
        download_income_statement,
        download_cash_flow,
        download_mkt_cap,
        download_top10,
        download_holder_count,
    ]
    
    paths = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [executor.submit(task) for task in download_tasks]
        for future in as_completed(futures):
            key, df, path = future.result()
            results[key] = df
            if path:
                paths[key] = path
    
    download_time = time.time() - start_time
    print(f"下载完成，耗时: {download_time:.1f}s", flush=True)
    
    if errors:
        print(f"下载错误: {errors}")
    
    # 合并财务报表为长表
    t0 = time.time()
    print("合并财务报表为长表...", end=" ", flush=True)
    combined = pd.concat([
        wide_to_long(results.get("balance", pd.DataFrame()), symbol, "资产负债表"),
        wide_to_long(results.get("income", pd.DataFrame()), symbol, "利润表"),
        wide_to_long(results.get("cash_flow", pd.DataFrame()), symbol, "现金流量表"),
    ], ignore_index=True)
    combined = combined.rename(columns={"报告日": REPORT_COL})
    combined = filter_by_years(combined, years)
    combined = combined.rename(columns={REPORT_COL: "报告日"})
    combined_path = os.path.join("outputs", f"{symbol}_financials_10y_long_combined.csv")
    combined.to_csv(combined_path, index=False)
    print(f"完成 ({time.time()-t0:.1f}s)")

    total_time = time.time() - start_time
    print(f"=" * 50)
    print(f"全部完成！总耗时: {total_time:.1f}秒")
    print(f"=" * 50)
    print(f"已保存文件:")
    for key, path in paths.items():
        print(f"  - {path}")
    print(f"  - {combined_path}")


if __name__ == "__main__":
    main()
