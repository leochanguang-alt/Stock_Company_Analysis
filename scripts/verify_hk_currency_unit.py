#!/usr/bin/env python3
"""
验证港股财务数据的货币单位
通过对比 A+H 双上市公司的总资产，推断港股数据是 CNY 还是 HKD
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}'
}

# 已知的 A+H 双上市公司
DUAL_LISTED = [
    ('00300', '000333', '美的集团'),
    ('02899', '601899', '紫金矿业'),
    ('01211', '002594', '比亚迪'),
]

def get_hk_total_assets(hk_code, report_date='2024-12-31'):
    """获取港股总资产"""
    url = f"{SUPABASE_URL}/rest/v1/hk_balance_sheet"
    params = {
        'select': 'amount',
        'secucode': f'eq.{hk_code}.HK',
        'std_item_name': 'eq.总资产',
        'report_date': f'eq.{report_date}'
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    data = r.json()
    if data:
        return data[0]['amount'] / 1e8  # 转成亿
    return None

def get_ashare_total_assets(ashare_code, report_date='2024-12-31'):
    """获取A股总资产"""
    url = f"{SUPABASE_URL}/rest/v1/cn_balance_sheet_10y"
    params = {
        'select': 'total_assets',
        'symbol': f'eq.{ashare_code}',
        'report_date': f'eq.{report_date}'
    }
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    data = r.json()
    if data:
        return data[0]['total_assets']  # 已经是亿CNY
    return None

def main():
    print("=" * 80)
    print("验证港股财务数据货币单位")
    print("=" * 80)
    
    CNY_TO_HKD = 1 / 0.901  # 1 CNY = 1.11 HKD
    
    results = []
    for hk_code, ashare_code, name in DUAL_LISTED:
        print(f"\n{name} ({hk_code}.HK / {ashare_code})")
        print("-" * 60)
        
        hk_assets = get_hk_total_assets(hk_code)
        a_assets = get_ashare_total_assets(ashare_code)
        
        if hk_assets is None:
            print(f"  ⚠️  港股数据缺失")
            continue
        if a_assets is None:
            print(f"  ⚠️  A股数据缺失")
            continue
        
        print(f"  H股总资产: {hk_assets:.2f} 亿")
        print(f"  A股总资产: {a_assets:.2f} 亿CNY")
        
        # 计算隐含汇率
        implied_rate = a_assets / hk_assets
        print(f"\n  隐含汇率 (CNY/HKD): {implied_rate:.4f}")
        print(f"  实际汇率 (CNY/HKD): 0.9010")
        
        # 判断
        if abs(implied_rate - 1.0) < 0.01:
            conclusion = "✅ H股数据是 CNY 单位"
        elif abs(implied_rate - 0.901) < 0.05:
            conclusion = "✅ H股数据是 HKD 单位"
        else:
            conclusion = f"⚠️  无法判断（隐含汇率 {implied_rate:.4f}）"
        
        print(f"  结论: {conclusion}")
        results.append((name, hk_code, ashare_code, hk_assets, a_assets, implied_rate, conclusion))
    
    print("\n" + "=" * 80)
    print("汇总")
    print("=" * 80)
    for name, hk, a, hk_val, a_val, rate, conclusion in results:
        print(f"{name:10} | H:{hk_val:8.2f} | A:{a_val:8.2f} | Rate:{rate:.4f} | {conclusion}")
    
    # 最终判断
    print("\n" + "=" * 80)
    avg_rate = sum(r[5] for r in results) / len(results) if results else 0
    if abs(avg_rate - 1.0) < 0.05:
        print("🎯 最终结论: 港股财务数据使用 **CNY 单位**")
        print("   建议: 对双上市公司，直接使用 A 股数据（更完整），前端标注为 CNY")
    elif abs(avg_rate - 0.901) < 0.1:
        print("🎯 最终结论: 港股财务数据使用 **HKD 单位**")
        print(f"   建议: 使用隐含汇率 {avg_rate:.4f} 转换 H 股数据到 CNY 等价值")
    else:
        print(f"⚠️  无法确定（平均隐含汇率: {avg_rate:.4f}）")
    print("=" * 80)

if __name__ == '__main__':
    main()
