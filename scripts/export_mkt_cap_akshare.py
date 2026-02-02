import os
import datetime as dt
import pandas as pd
import akshare as ak


def parse_args(argv):
    symbol = "000333"
    years = 10
    for i, a in enumerate(argv):
        if a == "--symbol" and i + 1 < len(argv):
            symbol = argv[i + 1].strip()
        if a.startswith("--symbol="):
            symbol = a.split("=", 1)[1].strip()
        if a == "--years" and i + 1 < len(argv):
            years = int(argv[i + 1])
        if a.startswith("--years="):
            years = int(a.split("=", 1)[1])
    return symbol, years


def main():
    symbol, years = parse_args(os.sys.argv[1:])
    df = ak.stock_value_em(symbol=symbol)
    if df.empty:
        print("未获取到市值数据")
        return

    df = df.rename(columns={"数据日期": "date", "总市值": "total_mv"})
    df["date"] = pd.to_datetime(df["date"])

    cutoff = dt.datetime.now() - dt.timedelta(days=years * 365)
    df = df[df["date"] >= cutoff]

    df["mkt_cap_billion_cny"] = df["total_mv"] / 1e9
    df = df.sort_values("date")[["date", "mkt_cap_billion_cny"]]
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    out_path = os.path.join("outputs", f"{symbol}_mkt_cap_10y.csv")
    df.to_csv(out_path, index=False)
    print(f"输出 {len(df)} 行到 {out_path}")


if __name__ == "__main__":
    main()
