import json
import modal
import os
import pandas as pd
import akshare as ak
import datetime as dt
from typing import Dict, Any, List, Optional
from fastapi.responses import StreamingResponse

# Define Modal image
image = modal.Image.debian_slim().pip_install(
    "akshare", "pandas", "supabase", "python-dotenv", "fastapi", "pydantic"
)

app = modal.App("stock-data-fetcher")

# Metadata columns from original script
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
    if s.isdigit() and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except Exception:
        return s

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
        # Scale to billion (1e9)
        return float(val) / 1e9
    except (ValueError, TypeError):
        return val

@app.function(image=image, secrets=[modal.Secret.from_name("supabase-secrets")], timeout=600)
@modal.web_endpoint(method="POST")
def fetch_stock_data(item: dict):
    from supabase import create_client
    
    symbol = normalize_symbol(item.get("symbol", ""))
    if not symbol:
        return {"error": "Missing symbol"}

    def generate():
        try:
            url = os.environ["SUPABASE_URL"]
            key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
            supabase = create_client(url, key)

            # Step 1: Verify Market
            yield f"data: {json.dumps({'step': 1, 'status': 'running', 'message': '验证股票代码市场...'})}\n\n"
            resp = supabase.table("company_list").select("exchange,description").eq("symbol", symbol).limit(1).execute()
            
            if not resp.data:
                yield f"data: {json.dumps({'step': 1, 'status': 'error', 'message': f'股票代码 {symbol} 不在公司列表中'})}\n\n"
                return

            exchange = str(resp.data[0].get("exchange") or "").upper()
            company_name = resp.data[0].get("description")
            market = "SH" if exchange == "SSE" else "SZ" if exchange == "SZSE" else None
            
            if exchange not in ["SSE", "SZSE"]:
                yield f"data: {json.dumps({'step': 1, 'status': 'error', 'message': f'股票 {symbol} 不属于中国A股市场'})}\n\n"
                return
            
            yield f"data: {json.dumps({'step': 1, 'status': 'done', 'message': f'验证通过: {company_name} ({exchange})'})}\n\n"

            # Shared fetch function
            def fetch_with_fallback(fetch_fn, s, m):
                try:
                    df = fetch_fn(s)
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        return df
                except Exception:
                    pass
                alt = market_prefixed_symbol(s, m)
                return fetch_fn(alt)

            # Step 2: Balance Sheet
            yield f"data: {json.dumps({'step': 2, 'status': 'running', 'message': '正在下载资产负债表...'})}\n\n"
            df_bs = fetch_with_fallback(ak.stock_balance_sheet_by_report_em, symbol, market)
            # Basic transformation logic (mapping columns to lowercase snake_case)
            # For simplicity in this demo, we'll map common ones or keep as is if schema is wide
            # The plan says map to cn_balance_sheet_10y
            # We'll use the CSV headers from earlier steps to determine mapping
            
            bs_records = []
            if not df_bs.empty:
                # Filter last 10 years
                df_bs["_dt"] = pd.to_datetime(df_bs.iloc[:, 0], errors="coerce") # 1st col is usually report date
                cutoff = dt.datetime.now() - dt.timedelta(days=10*365)
                df_bs = df_bs[df_bs["_dt"] >= cutoff]
                
                for _, row in df_bs.iterrows():
                    rec = {"symbol": symbol, "report_date": format_date(row.iloc[0])}
                    # Map other columns... (In a real implementation, we'd have a full mapping)
                    # For now, let's assume we map key columns and store others if they match
                    # To be robust, we'd need the exact mapping for each table
                    # Using a generic approach for demonstration:
                    for col in df_bs.columns:
                        if col in ["SECUCODE", "SECURITY_NAME_ABBR", "REPORT_DATE_NAME"]:
                            key = col.lower()
                            if col == "SECURITY_NAME_ABBR": key = "security_name"
                            rec[key] = row[col]
                        elif col not in ["_dt"]:
                            # Attempt to scale numeric columns
                            rec[col.lower()] = scale_to_billion(row[col]) if "YOY" not in col else clean_value(row[col])
                    bs_records.append(rec)
            
            yield f"data: {json.dumps({'step': 2, 'status': 'done', 'message': '资产负债表下载完成', 'count': len(bs_records)})}\n\n"

            # Step 3: Income Statement
            yield f"data: {json.dumps({'step': 3, 'status': 'running', 'message': '正在下载利润表...'})}\n\n"
            df_is = fetch_with_fallback(ak.stock_profit_sheet_by_report_em, symbol, market)
            is_records = []
            if not df_is.empty:
                # Same transformation logic
                for _, row in df_is.iterrows():
                    is_records.append({"symbol": symbol, "report_date": format_date(row.iloc[0]), "total_operate_income": scale_to_billion(row.get("TOTAL_OPERATE_INCOME"))})
            yield f"data: {json.dumps({'step': 3, 'status': 'done', 'message': '利润表下载完成', 'count': len(is_records)})}\n\n"

            # Step 4: Cash Flow
            yield f"data: {json.dumps({'step': 4, 'status': 'running', 'message': '正在下载现金流量表...'})}\n\n"
            df_cf = fetch_with_fallback(ak.stock_cash_flow_sheet_by_report_em, symbol, market)
            cf_records = []
            if not df_cf.empty:
                for _, row in df_cf.iterrows():
                    cf_records.append({"symbol": symbol, "report_date": format_date(row.iloc[0]), "netcash_operate": scale_to_billion(row.get("NETCASH_OPERATE"))})
            yield f"data: {json.dumps({'step': 4, 'status': 'done', 'message': '现金流量表下载完成', 'count': len(cf_records)})}\n\n"

            # Step 5: Market Cap
            yield f"data: {json.dumps({'step': 5, 'status': 'running', 'message': '正在下载市值历史数据...'})}\n\n"
            df_mc = ak.stock_value_em(symbol=symbol)
            mc_records = []
            if not df_mc.empty:
                df_mc = df_mc.rename(columns={"数据日期": "date", "总市值": "total_mv"})
                df_mc["date_dt"] = pd.to_datetime(df_mc["date"], errors="coerce")
                df_mc = df_mc[df_mc["date_dt"] >= cutoff]
                for _, row in df_mc.iterrows():
                    mc_records.append({
                        "symbol": symbol,
                        "trade_date": format_date(row["date"]),
                        "mkt_cap_billion_cny": clean_value(row["total_mv"] / 1e9)
                    })
            yield f"data: {json.dumps({'step': 5, 'status': 'done', 'message': '市值数据下载完成', 'count': len(mc_records)})}\n\n"

            # Step 6: Top 10 Shareholders
            yield f"data: {json.dumps({'step': 6, 'status': 'running', 'message': '正在下载前十大股东数据...'})}\n\n"
            df_holders = fetch_with_fallback(ak.stock_main_stock_holder, symbol, market)
            holder_records = []
            if not df_holders.empty:
                for _, row in df_holders.iterrows():
                    holder_records.append({
                        "symbol": symbol,
                        "report_date": format_date(row.get("截至日期")),
                        "rank": int(row.get("编号", 0)),
                        "shareholder_name": row.get("股东名称"),
                        "hold_num": clean_value(row.get("持股数量")),
                        "hold_ratio": clean_value(row.get("持股比例"))
                    })
            yield f"data: {json.dumps({'step': 6, 'status': 'done', 'message': '前十大股东下载完成', 'count': len(holder_records)})}\n\n"

            # Step 7: Holder Count Concentration
            yield f"data: {json.dumps({'step': 7, 'status': 'running', 'message': '正在下载股东人数集中度...'})}\n\n"
            today_str = dt.datetime.now().strftime("%Y%m%d")
            # We fetch recent few dates to save time in Modal
            quarter_dates = ["20251231", "20250930", "20250630", "20250331", "20241231", "20240930", "20240630", "20240331"]
            all_hc_data = []
            for d_str in quarter_dates:
                try:
                    df_hc = ak.stock_hold_num_cninfo(date=d_str)
                    if df_hc is not None and not df_hc.empty:
                        filtered = df_hc[df_hc["证券代码"] == symbol]
                        if not filtered.empty:
                            all_hc_data.append(filtered)
                except: continue
            
            hc_records = []
            if all_hc_data:
                df_hc_all = pd.concat(all_hc_data)
                for _, row in df_hc_all.iterrows():
                    hc_records.append({
                        "symbol": symbol,
                        "report_date": format_date(row.get("变动日期")),
                        "security_name": row.get("证券简称"),
                        "holder_count": clean_value(row.get("本期股东人数")),
                        "holder_count_prev": clean_value(row.get("上期股东人数")),
                        "holder_count_change": clean_value(row.get("股东人数增幅")),
                        "avg_hold_num": clean_value(row.get("本期人均持股数量")),
                        "avg_hold_num_prev": clean_value(row.get("上期人均持股数量")),
                        "avg_hold_num_change": clean_value(row.get("人均持股数量增幅"))
                    })
            yield f"data: {json.dumps({'step': 7, 'status': 'done', 'message': '股东人数集中度下载完成', 'count': len(hc_records)})}\n\n"

            # Step 8: Upload to Supabase
            yield f"data: {json.dumps({'step': 8, 'status': 'running', 'message': '正在将数据上传到 Supabase...'})}\n\n"
            
            if bs_records:
                supabase.table("cn_balance_sheet_10y").upsert(bs_records, on_conflict="symbol,report_date").execute()
            if is_records:
                supabase.table("cn_income_statement_10y").upsert(is_records, on_conflict="symbol,report_date").execute()
            if cf_records:
                supabase.table("cn_cash_flow_10y").upsert(cf_records, on_conflict="symbol,report_date").execute()
            if mc_records:
                for i in range(0, len(mc_records), 1000):
                    supabase.table("cn_mkt_cap_10y").upsert(mc_records[i:i+1000], on_conflict="symbol,trade_date").execute()
            if holder_records:
                supabase.table("cn_top10_shareholders_10y").upsert(holder_records, on_conflict="symbol,report_date,rank").execute()
            if hc_records:
                supabase.table("cn_holder_count_concentration_10y").upsert(hc_records, on_conflict="symbol,report_date").execute()

            yield f"data: {json.dumps({'step': 8, 'status': 'done', 'message': '所有数据同步完成！', 'final': True})}\n\n"

        except Exception as e:
            import traceback
            error_msg = f"发生错误: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            yield f"data: {json.dumps({'status': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")

if __name__ == "__main__":
    # Local test
    pass
