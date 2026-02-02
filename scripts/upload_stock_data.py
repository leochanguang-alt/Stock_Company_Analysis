#!/usr/bin/env python3
import os
import sys
import time
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client


def parse_args(argv: List[str]) -> Dict[str, str]:
    symbol = "000333"
    for i, a in enumerate(argv):
        if a == "--symbol" and i + 1 < len(argv):
            symbol = argv[i + 1].strip()
        if a.startswith("--symbol="):
            symbol = a.split("=", 1)[1].strip()
    return {"symbol": symbol}


def normalize_symbol(symbol: str) -> str:
    s = str(symbol).strip().upper()
    if s.endswith(".SZ") or s.endswith(".SH") or s.endswith(".SS"):
        s = s.split(".")[0]
    if s.startswith("SZ") or s.startswith("SH") or s.startswith("SS"):
        s = s[2:]
    # 确保股票代码是 6 位（补前导零）
    if s.isdigit():
        s = s.zfill(6)
    return s


def chunked(items: List[Dict[str, Any]], size: int = 500) -> List[List[Dict[str, Any]]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def clean_value(v):
    """Clean values for JSON serialization - handle NaN, Inf, -Inf"""
    import math
    import numpy as np
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    if isinstance(v, (np.floating, np.integer)):
        if np.isnan(v) or np.isinf(v):
            return None
        return float(v) if isinstance(v, np.floating) else int(v)
    return v


def clean_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Clean all values in a record for JSON serialization"""
    return {k: clean_value(v) for k, v in record.items()}


def ensure_columns(df: pd.DataFrame, required: List[str], name: str):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} 缺少字段: {missing}")


def format_report_date(val):
    if val is None or pd.isna(val):
        return None
    s = str(val).strip()
    if s.isdigit() and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        return s


def is_numeric_value(val) -> bool:
    """检查值是否可以转换为数值"""
    if val is None or pd.isna(val):
        return True  # None/NaN 是允许的
    if isinstance(val, (int, float)):
        return True
    if isinstance(val, str):
        # 尝试转换为浮点数
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            return False
    return False


def load_financials(symbol: str) -> List[Dict[str, Any]]:
    path = os.path.join("outputs", f"{symbol}_financials_10y_long_combined.csv")
    df = pd.read_csv(path, dtype={"股票代码": str})  # 强制读取为字符串
    ensure_columns(df, ["股票代码", "报告日", "报表类型", "财务科目", "数值"], "财务长表")

    df["report_date"] = df["报告日"].apply(format_report_date)
    df["announcement_date"] = df.get("公告日期", None).apply(format_report_date) if "公告日期" in df.columns else None

    records = []
    skipped = 0
    for _, row in df.iterrows():
        # 跳过非数值类型的 value（如 SECUCODE, SECURITY_NAME_ABBR 等字段）
        val = row.get("数值")
        if not is_numeric_value(val):
            skipped += 1
            continue
        
        # 确保股票代码格式化为 6 位（补前导零）
        raw_symbol = str(row.get("股票代码", "")).strip()
        formatted_symbol = raw_symbol.zfill(6) if raw_symbol.isdigit() else raw_symbol
        records.append({
            "symbol": formatted_symbol,
            "report_date": row.get("report_date"),
            "statement_type": row.get("报表类型"),
            "account": row.get("财务科目"),
            "value": float(val) if pd.notna(val) and val != "" else None,
            "data_source": row.get("数据源"),
            "is_audited": row.get("是否审计"),
            "announcement_date": row.get("announcement_date"),
            "currency": row.get("币种"),
            "report_type": row.get("类型"),
            "updated_at": format_report_date(row.get("更新日期")),
        })
    if skipped > 0:
        print(f"  跳过 {skipped} 条非数值记录")
    return records


def load_mkt_cap(symbol: str) -> List[Dict[str, Any]]:
    path = os.path.join("outputs", f"{symbol}_mkt_cap_10y.csv")
    df = pd.read_csv(path)
    ensure_columns(df, ["date", "mkt_cap_billion_cny"], "市值历史")

    records = []
    for _, row in df.iterrows():
        date = format_report_date(row.get("date"))
        mkt_cap = row.get("mkt_cap_billion_cny")
        if pd.isna(mkt_cap):
            mkt_cap = None
        records.append({
            "id": f"{symbol}_{date}",
            "symbol": symbol,
            "date": date,
            "Market_cap": mkt_cap,
            "unit": "bn",
            "currency": "cny",
        })
    return records


def load_sharehold(symbol: str) -> List[Dict[str, Any]]:
    path = os.path.join("outputs", f"{symbol}_holder_count_concentration_10y.csv")
    df = pd.read_csv(path, dtype={"证券代码": str})  # 强制读取为字符串
    ensure_columns(df, ["证券代码", "变动日期", "本期股东人数"], "股东集中度")

    records = []
    for _, row in df.iterrows():
        raw_symbol = str(row.get("证券代码", "")).strip()
        formatted_symbol = raw_symbol.zfill(6) if raw_symbol.isdigit() else raw_symbol
        records.append({
            "symbol": formatted_symbol,
            "name": row.get("证券简称"),
            "report_date": format_report_date(row.get("变动日期")),
            "current_holder_count": row.get("本期股东人数") if pd.notna(row.get("本期股东人数")) else None,
            "previous_holder_count": row.get("上期股东人数") if pd.notna(row.get("上期股东人数")) else None,
            "holder_count_change_pct": row.get("股东人数增幅") if pd.notna(row.get("股东人数增幅")) else None,
            "current_avg_shares": row.get("本期人均持股数量") if pd.notna(row.get("本期人均持股数量")) else None,
            "previous_avg_shares": row.get("上期人均持股数量") if pd.notna(row.get("上期人均持股数量")) else None,
            "avg_shares_change_pct": row.get("人均持股数量增幅") if pd.notna(row.get("人均持股数量增幅")) else None,
            "report_period": row.get("报告期"),
        })
    return records


def load_top10(symbol: str) -> List[Dict[str, Any]]:
    path = os.path.join("outputs", f"{symbol}_top10_shareholders_10y.csv")
    df = pd.read_csv(path, dtype={"股票代码": str})  # 强制读取为字符串
    ensure_columns(df, ["名次", "股东名称", "报告期"], "前十大股东")

    records = []
    for _, row in df.iterrows():
        # 跳过 rank 为空的记录（数据库要求非空）
        if pd.isna(row.get("名次")):
            continue
        
        # 处理 symbol：去除 SZ/SH 前缀并补齐 6 位
        raw_symbol = str(row.get("股票代码", "")).strip() or row.get("symbol", "")
        # 去除 SZ/SH/SS 前缀
        if raw_symbol.upper().startswith(("SZ", "SH", "SS")):
            raw_symbol = raw_symbol[2:]
        formatted_symbol = raw_symbol.zfill(6) if raw_symbol.isdigit() else raw_symbol
        
        records.append({
            "symbol": formatted_symbol,
            "report_date": row.get("报告期"),
            "rank": int(row.get("名次")),
            "shareholder_name": row.get("股东名称"),
            "share_type": row.get("股份类型"),
            "shares_held": int(row.get("持股数")) if pd.notna(row.get("持股数")) else None,
            "holding_ratio": row.get("占总股本持股比例") if pd.notna(row.get("占总股本持股比例")) else None,
            "change_amount": row.get("增减"),
            "change_ratio": row.get("变动比率") if pd.notna(row.get("变动比率")) else None,
        })
    return records


def main():
    start_time = time.time()
    args = parse_args(sys.argv[1:])
    symbol = normalize_symbol(args["symbol"])

    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("缺少 SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")
        sys.exit(1)
    
    print(f"=" * 50)
    print(f"开始上传 {symbol} 数据...")
    print(f"=" * 50)

    # 并行加载所有 CSV 文件
    print("并行加载 CSV 文件...", flush=True)
    data = {}
    
    def load_data(name, loader):
        try:
            t0 = time.time()
            result = loader(symbol)
            print(f"  [✓] {name}: {len(result)} 条 ({time.time()-t0:.1f}s)", flush=True)
            return (name, result)
        except Exception as e:
            print(f"  [✗] {name}: {e}", flush=True)
            return (name, [])
    
    load_tasks = [
        ("financials", load_financials),
        ("mkt_caps", load_mkt_cap),
        ("sharehold", load_sharehold),
        ("top10", load_top10),
    ]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(load_data, name, loader) for name, loader in load_tasks]
        for future in as_completed(futures):
            name, result = future.result()
            data[name] = result
    
    load_time = time.time() - start_time
    print(f"加载完成，耗时: {load_time:.1f}s", flush=True)

    # 创建 Supabase 客户端（每个线程需要自己的客户端）
    def get_supabase():
        return create_client(url, key)

    # 删除旧数据
    print(f"删除 {symbol} 的旧数据...")
    supabase = get_supabase()
    try:
        supabase.table("company_financials_long").delete().eq("symbol", symbol).execute()
        print("  - company_financials_long: 删除完成")
    except Exception as e:
        print(f"  - company_financials_long: 删除失败 - {str(e)[:50]}")

    # 并行上传到 4 个表
    print("并行上传到 Supabase...", flush=True)
    upload_results = {}
    BATCH_SIZE = 1000  # 增大批次大小
    
    def upload_financials():
        t0 = time.time()
        client = get_supabase()
        records = data.get("financials", [])
        uploaded = 0
        for batch in chunked(records, BATCH_SIZE):
            cleaned_batch = [clean_record(r) for r in batch]
            client.table("company_financials_long").upsert(
                cleaned_batch, on_conflict="symbol,report_date,statement_type,account"
            ).execute()
            uploaded += len(batch)
        print(f"  [✓] company_financials_long: {uploaded} 条 ({time.time()-t0:.1f}s)", flush=True)
        return ("financials", uploaded)
    
    def upload_mkt_caps():
        t0 = time.time()
        client = get_supabase()
        records = data.get("mkt_caps", [])
        uploaded = 0
        for batch in chunked(records, BATCH_SIZE):
            cleaned_batch = [clean_record(r) for r in batch]
            try:
                client.table("stock_valuation_history").insert(cleaned_batch).execute()
                uploaded += len(batch)
            except Exception:
                pass  # 跳过重复记录
        print(f"  [✓] stock_valuation_history: {uploaded} 条 ({time.time()-t0:.1f}s)", flush=True)
        return ("mkt_caps", uploaded)
    
    def upload_sharehold():
        t0 = time.time()
        client = get_supabase()
        records = data.get("sharehold", [])
        uploaded = 0
        for batch in chunked(records, BATCH_SIZE):
            cleaned_batch = [clean_record(r) for r in batch]
            try:
                client.table("cn_sharehold_data").insert(cleaned_batch).execute()
                uploaded += len(batch)
            except Exception:
                pass  # 跳过重复记录
        print(f"  [✓] cn_sharehold_data: {uploaded} 条 ({time.time()-t0:.1f}s)", flush=True)
        return ("sharehold", uploaded)
    
    def upload_top10():
        t0 = time.time()
        client = get_supabase()
        records = data.get("top10", [])
        uploaded = 0
        for batch in chunked(records, BATCH_SIZE):
            cleaned_batch = [clean_record(r) for r in batch]
            try:
                client.table("cn_top10_sharehold").insert(cleaned_batch).execute()
                uploaded += len(batch)
            except Exception:
                pass  # 跳过重复记录
        print(f"  [✓] cn_top10_sharehold: {uploaded} 条 ({time.time()-t0:.1f}s)", flush=True)
        return ("top10", uploaded)
    
    upload_tasks = [upload_financials, upload_mkt_caps, upload_sharehold, upload_top10]
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(task) for task in upload_tasks]
        for future in as_completed(futures):
            name, count = future.result()
            upload_results[name] = count

    total_time = time.time() - start_time
    print(f"=" * 50)
    print(f"上传完成！总耗时: {total_time:.1f}秒")
    print(f"=" * 50)


if __name__ == "__main__":
    main()
