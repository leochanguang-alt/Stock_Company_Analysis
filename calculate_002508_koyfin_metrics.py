import pandas as pd
import numpy as np
import os

def process_financials():
    file_path = "outputs/002508_financials_10y_long_combined.csv"
    df = pd.read_csv(file_path)
    df.columns = ['symbol', 'report_date', 'statement_type', 'account', 'value', 'source', 'is_audited', 'announcement_date', 'currency', 'type', 'updated_at']
    df['report_date'] = df['report_date'].astype(str)
    df['dt'] = pd.to_datetime(df['report_date'], format='%Y%m%d')
    
    mapping = {
        # Income Statement - Revenue
        '营业收入': 'Revenue',
        '营业总收入': 'Total_Revenue_Gross',
        '其他业务收入': 'Other_Business_Revenue',
        '其他业务利润': 'Other_Business_Profit',
        '其他业务成本': 'Other_Business_Cost',
        
        # Income Statement - Cost & Profit
        '营业成本': 'COGS',
        '营业总成本': 'Total_Operating_Cost',
        '营业利润': 'Operating_Income',
        '利润总额': 'Pretax_Income',
        '所得税费用': 'Income_Tax_Exp',
        '净利润': 'Net_Income',
        '归属于母公司所有者的净利润': 'Net_Income_Parent',
        '少数股东损益': 'Minority_Interest_Income',
        '持续经营净利润': 'Continuing_Operations_Income',
        
        # Income Statement - Operating Expenses
        '销售费用': 'Selling_Exp',
        '管理费用': 'Admin_Exp',
        '研发费用': 'RD_Exp',
        '财务费用': 'Fin_Exp',
        '利息收入': 'Interest_Inc',
        '利息支出': 'Interest_Exp',
        '利息费用': 'Interest_Exp_Alt',
        '营业税金及附加': 'Taxes_Surcharges',
        
        # Income Statement - Non-Operating Items
        '资产减值损失': 'Asset_Impairment',
        '信用减值损失': 'Credit_Impairment',
        '资产处置收益': 'Asset_Disposal_Gain',
        '非流动资产处置利得': 'NonCurrent_Asset_Disposal_Gain',
        '非流动资产处置损失': 'NonCurrent_Asset_Disposal_Loss',
        '投资收益': 'Investment_Income',
        '对联营企业和合营企业的投资收益': 'Equity_Method_Income',
        '公允价值变动收益': 'FV_Change_Income',
        '其他收益': 'Other_Income',
        '营业外收入': 'Non_Operating_Income',
        '营业外支出': 'Non_Operating_Exp',
        '汇兑收益': 'FX_Gain',
        
        # EPS
        '基本每股收益': 'EPS',
        '稀释每股收益': 'Diluted_EPS',
        
        # Balance Sheet - Assets
        '资产总计': 'Total_Assets',
        '流动资产合计': 'Total_Current_Assets',
        '货币资金': 'Cash_Equivalents',
        '交易性金融资产': 'Short_Term_Investments',
        '应收账款': 'Accounts_Receivable',
        '应收票据': 'Notes_Receivable',
        '应收票据及应收账款': 'Notes_AR_Combined',
        '应收款项融资': 'Financing_Receivables',
        '预付款项': 'Prepaid_Expenses',
        '其他应收款': 'Other_Receivables',
        '其他应收款(合计)': 'Other_Receivables_Total',
        '存货': 'Inventory',
        '合同资产': 'Contract_Assets',
        '其他流动资产': 'Other_Current_Assets',
        '非流动资产合计': 'Total_NonCurrent_Assets',
        '固定资产净额': 'Net_PPE',
        '固定资产原值': 'Gross_PPE',
        '累计折旧': 'Accumulated_Depreciation',
        '在建工程': 'Construction_In_Progress',
        '在建工程合计': 'Construction_In_Progress_Total',
        '无形资产': 'Intangible_Assets',
        '开发支出': 'Development_Costs',
        '商誉': 'Goodwill',
        '长期股权投资': 'LT_Equity_Investment',
        '其他权益工具投资': 'Other_Equity_Investments',
        '其他非流动金融资产': 'Other_NonCurrent_Financial_Assets',
        '投资性房地产': 'Investment_Property',
        '递延所得税资产': 'Deferred_Tax_Asset',
        '其他非流动资产': 'Other_NonCurrent_Assets',
        '使用权资产': 'Right_Of_Use_Assets',
        '长期待摊费用': 'LT_Prepaid_Expenses',
        '长期应收款': 'LT_Receivables',
        
        # Balance Sheet - Liabilities
        '负债合计': 'Total_Liabilities',
        '流动负债合计': 'Total_Current_Liabilities',
        '短期借款': 'Short_Term_Debt',
        '应付账款': 'Accounts_Payable',
        '应付票据': 'Notes_Payable',
        '应付票据及应付账款': 'Notes_AP_Combined',
        '预收款项': 'Unearned_Revenue',
        '合同负债': 'Contract_Liabilities',
        '应付职工薪酬': 'Employee_Benefits_Payable',
        '应交税费': 'Taxes_Payable',
        '其他应付款': 'Other_Payables',
        '其他应付款合计': 'Other_Payables_Total',
        '一年内到期的非流动负债': 'Current_Portion_LT_Debt',
        '其他流动负债': 'Other_Current_Liabilities',
        '非流动负债合计': 'Total_NonCurrent_Liabilities',
        '长期借款': 'Long_Term_Debt',
        '应付债券': 'Bonds_Payable',
        '租赁负债': 'Lease_Liabilities',
        '长期应付款': 'LT_Payables',
        '长期应付款合计': 'LT_Payables_Total',
        '长期应付职工薪酬': 'LT_Employee_Benefits',
        '递延所得税负债': 'Deferred_Tax_Liability',
        '递延收益': 'Deferred_Revenue',
        '长期递延收益': 'LT_Deferred_Revenue',
        '其他非流动负债': 'Other_NonCurrent_Liabilities',
        '预计流动负债': 'Accrued_Liabilities',
        
        # Balance Sheet - Equity
        '所有者权益(或股东权益)合计': 'Total_Equity',
        '归属于母公司股东权益合计': 'Equity_Parent',
        '少数股东权益': 'Minority_Interest',
        '实收资本(或股本)': 'Common_Stock',
        '资本公积': 'Additional_Paid_In_Capital',
        '盈余公积': 'Surplus_Reserve',
        '未分配利润': 'Retained_Earnings',
        '减:库存股': 'Treasury_Stock',
        '其他综合收益': 'Other_Comprehensive_Income',
        
        # Cash Flow
        '经营活动产生的现金流量净额': 'OCF',
        '投资活动产生的现金流量净额': 'ICF',
        '筹资活动产生的现金流量净额': 'CFF',
        '购建固定资产、无形资产和其他长期资产所支付的现金': 'CapEx',
        '分配股利、利润或偿付利息所支付的现金': 'Dividends_Paid',
        '销售商品、提供劳务收到的现金': 'Cash_From_Sales',
        '收到的税费返还': 'Tax_Refunds',
        '支付给职工以及为职工支付的现金': 'Employee_Cash_Paid',
        '支付的各项税费': 'Taxes_Paid',
        '取得借款收到的现金': 'Proceeds_From_Borrowings',
        '偿还债务支付的现金': 'Repayment_Of_Debt',
        '吸收投资收到的现金': 'Proceeds_From_Equity',
        '现金及现金等价物净增加额': 'Net_Change_In_Cash',
        '处置固定资产、无形资产和其他长期资产所收回的现金净额': 'Proceeds_From_Asset_Sales',
        '投资所支付的现金': 'Cash_For_Investments',
        '收回投资所收到的现金': 'Proceeds_From_Investment_Sales',
        '取得投资收益收到的现金': 'Cash_From_Investment_Income',
        
        # Additional Cash Flow items
        '汇率变动对现金及现金等价物的影响': 'FX_Effect',
        '期初现金及现金等价物余额': 'Beginning_Cash',
        '期末现金及现金等价物余额': 'Ending_Cash',
        '收到的其他与经营活动有关的现金': 'Other_Operating_Cash_In',
        '支付的其他与经营活动有关的现金': 'Other_Operating_Cash_Out',
        '购买商品、接受劳务支付的现金': 'Cash_Paid_For_Goods',
        '收到的其他与投资活动有关的现金': 'Other_Investing_Cash_In',
        '支付的其他与投资活动有关的现金': 'Other_Investing_Cash_Out',
        '取得子公司及其他营业单位支付的现金净额': 'Cash_Acquisitions',
        '处置子公司及其他营业单位收到的现金净额': 'Cash_Divestitures',
        '收到其他与筹资活动有关的现金': 'Other_Financing_Cash_In',
        '支付其他与筹资活动有关的现金': 'Other_Financing_Cash_Out',
        '子公司吸收少数股东投资收到的现金': 'Minority_Investment_Received',
        '子公司支付给少数股东的股利、利润': 'Minority_Dividends_Paid',
        '发行债券收到的现金': 'Bond_Issuance',
        '经营活动现金流入小计': 'Operating_Cash_Inflow',
        '经营活动现金流出小计': 'Operating_Cash_Outflow',
        '投资活动现金流入小计': 'Investing_Cash_Inflow',
        '投资活动现金流出小计': 'Investing_Cash_Outflow',
        '筹资活动现金流入小计': 'Financing_Cash_Inflow',
        '筹资活动现金流出小计': 'Financing_Cash_Outflow',
        '累计折旧': 'Accumulated_Depreciation',
    }
    
    df_filtered = df[df['account'].isin(mapping.keys())].copy()
    df_filtered['account_en'] = df_filtered['account'].map(mapping)
    df_filtered = df_filtered.sort_values(['report_date', 'account_en', 'updated_at'], ascending=[True, True, False])
    df_wide = df_filtered.drop_duplicates(['report_date', 'account_en']).pivot(index='report_date', columns='account_en', values='value')
    df_wide.index = pd.to_datetime(df_wide.index, format='%Y%m%d')
    df_wide = df_wide.sort_index()
    
    # Status columns (balance sheet items - point-in-time)
    status_cols = [
        'Total_Assets', 'Total_Liabilities', 'Total_Equity', 'Equity_Parent', 'Minority_Interest',
        'Cash_Equivalents', 'Short_Term_Investments', 'Accounts_Receivable', 'Notes_Receivable',
        'Notes_AR_Combined', 'Prepaid_Expenses', 'Other_Receivables', 'Inventory', 'Other_Current_Assets',
        'Total_Current_Assets', 'Total_NonCurrent_Assets', 'Net_PPE', 'Construction_In_Progress',
        'Intangible_Assets', 'Goodwill', 'LT_Equity_Investment', 'Deferred_Tax_Asset',
        'Other_NonCurrent_Assets', 'Right_Of_Use_Assets', 'LT_Prepaid_Expenses',
        'Total_Current_Liabilities', 'Total_NonCurrent_Liabilities', 'Short_Term_Debt',
        'Accounts_Payable', 'Notes_Payable', 'Notes_AP_Combined', 'Unearned_Revenue',
        'Contract_Liabilities', 'Employee_Benefits_Payable', 'Taxes_Payable', 'Other_Payables',
        'Current_Portion_LT_Debt', 'Other_Current_Liabilities', 'Long_Term_Debt', 'Bonds_Payable',
        'Lease_Liabilities', 'LT_Payables', 'Deferred_Tax_Liability', 'Deferred_Revenue',
        'LT_Deferred_Revenue', 'Common_Stock', 'Additional_Paid_In_Capital', 'Surplus_Reserve',
        'Retained_Earnings', 'Treasury_Stock', 'Other_Comprehensive_Income'
    ]
    flow_cols = [c for c in df_wide.columns if c not in status_cols]
    
    # Ensure all columns exist
    for col in mapping.values():
        if col not in df_wide.columns:
            df_wide[col] = 0.0

    # LTM calculation for flow items
    res_ltm = df_wide.copy()
    for col in flow_cols:
        if col not in df_wide.columns:
            continue
        for dt in df_wide.index:
            if dt.month == 12:
                continue
            p_ye = next((d for d in df_wide.index if d.year == dt.year - 1 and d.month == 12), None)
            p_p = next((d for d in df_wide.index if d.year == dt.year - 1 and d.month == dt.month), None)
            if p_ye and p_p:
                res_ltm.at[dt, col] = df_wide.at[dt, col] + df_wide.at[p_ye, col] - df_wide.at[p_p, col]
            else:
                res_ltm.at[dt, col] = np.nan

    res_annual = df_wide[df_wide.index.month == 12].copy()
    
    # Load market cap data
    mkt_cap_df = pd.read_csv("outputs/002508_mkt_cap_10y.csv")
    mkt_cap_df['date'] = pd.to_datetime(mkt_cap_df['date'])
    mkt_cap_df = mkt_cap_df.sort_values('date')
    
    def get_mkt_cap(report_date):
        match = mkt_cap_df[mkt_cap_df['date'] <= report_date]
        # Note: mkt_cap_billion_cny is actually in 亿 (100 millions), not billions
        return match.iloc[-1]['mkt_cap_billion_cny'] * 1e8 if not match.empty else np.nan

    res_ltm['Market_Cap'] = res_ltm.index.map(get_mkt_cap)
    res_annual['Market_Cap'] = res_annual.index.map(get_mkt_cap)
    
    def calculate_derived(df):
        # Helper to safely fill
        def safe_fill(col, default=0.0):
            if col not in df.columns:
                df[col] = default
            else:
                df[col] = df[col].fillna(default)
            return df[col]
        
        # ============ DEBT & CASH ============
        safe_fill('Short_Term_Debt')
        safe_fill('Current_Portion_LT_Debt')
        safe_fill('Long_Term_Debt')
        safe_fill('Bonds_Payable')
        safe_fill('Lease_Liabilities')
        
        df['Total_Debt'] = (df['Short_Term_Debt'] + df['Current_Portion_LT_Debt'] + 
                           df['Long_Term_Debt'] + df['Bonds_Payable'])
        
        safe_fill('Cash_Equivalents')
        safe_fill('Short_Term_Investments')
        df['Total_Cash'] = df['Cash_Equivalents'] + df['Short_Term_Investments']
        df['Net_Debt'] = df['Total_Debt'] - df['Total_Cash']
        
        # ============ ENTERPRISE VALUE ============
        safe_fill('Minority_Interest')
        df['EV'] = df['Market_Cap'] + df['Total_Debt'] - df['Total_Cash'] + df['Minority_Interest']
        
        # ============ INCOME STATEMENT METRICS ============
        safe_fill('Revenue')
        safe_fill('COGS')
        safe_fill('Operating_Income')
        safe_fill('Net_Income')
        safe_fill('Net_Income_Parent')
        safe_fill('Pretax_Income')
        safe_fill('Income_Tax_Exp')
        safe_fill('Selling_Exp')
        safe_fill('Admin_Exp')
        safe_fill('RD_Exp')
        safe_fill('Fin_Exp')
        safe_fill('Interest_Exp')
        safe_fill('Interest_Inc')
        safe_fill('Investment_Income')
        safe_fill('FV_Change_Income')
        safe_fill('Other_Income')
        safe_fill('Non_Operating_Income')
        safe_fill('Non_Operating_Exp')
        safe_fill('Asset_Impairment')
        safe_fill('Credit_Impairment')
        safe_fill('Asset_Disposal_Gain')
        safe_fill('Taxes_Surcharges')
        safe_fill('Other_Business_Revenue')
        safe_fill('Minority_Interest_Income')
        safe_fill('Equity_Method_Income')
        
        # Revenue breakdown
        df['Other_Revenue'] = df['Other_Business_Revenue'].fillna(0)
        df['Main_Revenue'] = df['Revenue'] - df['Other_Revenue']
        
        # Gross Profit
        df['Gross_Profit'] = df['Revenue'] - df['COGS']
        
        # Operating Expenses
        df['SGA_Exp'] = df['Selling_Exp'] + df['Admin_Exp']
        df['Operating_Expenses'] = df['Selling_Exp'] + df['Admin_Exp'] + df['RD_Exp']
        df['Other_Operating_Exp'] = df['Taxes_Surcharges'].fillna(0)
        
        # EBIT = Gross Profit - Operating Expenses (Koyfin method)
        df['EBIT'] = (df['Gross_Profit'] - 
                      df['Selling_Exp'].fillna(0) - 
                      df['Admin_Exp'].fillna(0) - 
                      df['RD_Exp'].fillna(0))
        
        # EBITDA - will be recalculated later with D&A
        df['DA_Estimated'] = df['Revenue'] * 0.015  # Initial estimate
        df['EBITDA'] = df['EBIT'] + df['DA_Estimated']
        
        # Net Interest (Koyfin shows as positive = income)
        df['Net_Interest_Exp'] = df['Fin_Exp'].fillna(0) * -1  # 财务费用为负表示净利息收入
        df['Interest_And_Investment_Income'] = df['Interest_Inc'].fillna(0) + df['Investment_Income'].fillna(0)
        
        # Non-Operating Items
        df['Non_Operating_Net'] = (df['Non_Operating_Income'].fillna(0) - 
                                   df['Non_Operating_Exp'].fillna(0))
        df['Gain_On_Asset_Sale'] = (df['Asset_Disposal_Gain'].fillna(0) + 
                                    df.get('NonCurrent_Asset_Disposal_Gain', pd.Series([0]*len(df))).fillna(0) -
                                    df.get('NonCurrent_Asset_Disposal_Loss', pd.Series([0]*len(df))).fillna(0))
        df['Gain_On_Investment_Sale'] = df['Investment_Income'].fillna(0)
        
        # Unusual Items
        df['Total_Impairment'] = df['Asset_Impairment'].fillna(0) + df['Credit_Impairment'].fillna(0)
        df['Other_Unusual_Items'] = (df['Other_Income'].fillna(0) + 
                                     df['FV_Change_Income'].fillna(0))
        
        # EBT breakdown
        df['EBT_Excl_Unusual'] = df['EBIT'] + df['Net_Interest_Exp']
        df['EBT_Incl_Unusual'] = df['Pretax_Income']
        
        # Earnings from continuing operations
        df['Earnings_Continuing'] = df['Net_Income']
        df['Net_Income_Common'] = df['Net_Income_Parent'].where(
            df['Net_Income_Parent'] != 0, df['Net_Income'])
        
        # ============ MARGINS ============
        rev_safe = df['Revenue'].replace(0, np.nan)
        df['Gross_Margin'] = df['Gross_Profit'] / rev_safe
        df['Operating_Margin'] = df['Operating_Income'] / rev_safe
        df['EBITDA_Margin'] = df['EBITDA'] / rev_safe
        df['EBIT_Margin'] = df['EBIT'] / rev_safe
        df['EBT_Margin'] = df['Pretax_Income'] / rev_safe
        df['EBT_Excl_Unusual_Margin'] = df['EBT_Excl_Unusual'] / rev_safe
        df['SGA_Margin'] = df['SGA_Exp'] / rev_safe
        df['Net_Margin'] = df['Net_Income'] / rev_safe
        df['Net_Avail_Common_Margin'] = df['Net_Income_Common'] / rev_safe
        df['Normalized_Net_Income'] = (
            df['Net_Income'].fillna(0) -
            df['Total_Impairment'].fillna(0) -
            df['Other_Unusual_Items'].fillna(0) -
            df['Gain_On_Asset_Sale'].fillna(0)
        )
        df['Normalized_Net_Income_Margin'] = df['Normalized_Net_Income'] / rev_safe
        
        # ============ BALANCE SHEET ============
        df['Total_Equity'] = df['Total_Equity'].ffill()
        df['Total_Assets'] = df['Total_Assets'].ffill()
        df['Total_Liabilities'] = df['Total_Liabilities'].ffill()
        df['Total_Current_Assets'] = df['Total_Current_Assets'].ffill()
        df['Total_Current_Liabilities'] = df['Total_Current_Liabilities'].ffill()
        
        # Working Capital
        df['Working_Capital'] = df['Total_Current_Assets'] - df['Total_Current_Liabilities']
        df['Working_Capital_Change'] = df['Working_Capital'].diff()
        
        # ============ CASH FLOW CHANGES (for reconciliation) ============
        # Changes in working capital components
        safe_fill('Accounts_Receivable')
        safe_fill('Notes_Receivable')
        safe_fill('Notes_AR_Combined')
        safe_fill('Inventory')
        safe_fill('Accounts_Payable')
        safe_fill('Notes_Payable')
        safe_fill('Notes_AP_Combined')
        safe_fill('Prepaid_Expenses')
        safe_fill('Other_Receivables')
        safe_fill('Other_Payables')
        safe_fill('Unearned_Revenue')
        safe_fill('Contract_Liabilities')
        
        # Calculate AR (use combined if separate not available)
        df['AR_Total'] = df['Accounts_Receivable'].where(df['Accounts_Receivable'] != 0, 
                         df['Notes_AR_Combined'])
        df['AP_Total'] = df['Accounts_Payable'].where(df['Accounts_Payable'] != 0,
                         df['Notes_AP_Combined'])
        
        # Changes (negative means increase = cash outflow)
        df['Change_In_AR'] = -df['AR_Total'].diff()  # Increase in AR = cash outflow
        df['Change_In_Inventory'] = -df['Inventory'].diff()  # Increase = cash outflow
        df['Change_In_AP'] = df['AP_Total'].diff()  # Increase in AP = cash inflow
        df['Change_In_Prepaid'] = -df['Prepaid_Expenses'].diff()
        df['Change_In_Other_Receivables'] = -df['Other_Receivables'].diff()
        df['Change_In_Other_Payables'] = df['Other_Payables'].diff()
        df['Change_In_Unearned'] = df['Unearned_Revenue'].diff() + df['Contract_Liabilities'].diff()
        
        # ============ BALANCE SHEET DERIVED ITEMS ============
        # Total Receivables
        safe_fill('Notes_Receivable')
        safe_fill('Financing_Receivables')
        df['Total_Receivables'] = (df['Accounts_Receivable'].fillna(0) + 
                                   df['Notes_Receivable'].fillna(0) + 
                                   df['Financing_Receivables'].fillna(0))
        df['Total_Receivables'] = df['Total_Receivables'].where(df['Total_Receivables'] != 0, 
                                   df['Notes_AR_Combined'])
        
        # Gross PPE and Net PPE
        safe_fill('Gross_PPE')
        safe_fill('Construction_In_Progress')
        safe_fill('Construction_In_Progress_Total')
        df['Gross_PPE'] = df['Gross_PPE'].ffill()
        df['Accumulated_Depreciation'] = df['Accumulated_Depreciation'].ffill()
        
        # Use construction in progress total if available
        df['CIP'] = df['Construction_In_Progress_Total'].where(
            df['Construction_In_Progress_Total'] != 0, 
            df['Construction_In_Progress'])
        
        # Total PPE (Koyfin style = Net PPE + CIP)
        df['Total_PPE_Koyfin'] = df['Net_PPE'].fillna(0) + df['CIP'].fillna(0)
        
        # Total Unearned Revenue (current + non-current)
        safe_fill('LT_Deferred_Revenue')
        df['Unearned_Revenue_Total'] = (df['Unearned_Revenue'].fillna(0) + 
                                        df['Contract_Liabilities'].fillna(0))
        df['Unearned_Revenue_NonCurrent'] = df['LT_Deferred_Revenue'].fillna(0)
        
        # Other receivables (use total if available)
        safe_fill('Other_Receivables_Total')
        df['Other_Receivables_Final'] = df['Other_Receivables_Total'].where(
            df['Other_Receivables_Total'] != 0,
            df['Other_Receivables'])
        
        # Common Equity (parent)
        safe_fill('Equity_Parent')
        df['Common_Equity'] = df['Equity_Parent'].where(
            df['Equity_Parent'] != 0,
            df['Total_Equity'] - df['Minority_Interest'].fillna(0))
        
        # Total Capital = Equity + Total Debt
        df['Total_Capital'] = df['Total_Equity'] + df['Total_Debt']
        
        # Book Value metrics
        safe_fill('Goodwill')
        safe_fill('Intangible_Assets')
        df['Tangible_Book_Value'] = df['Total_Equity'] - df['Goodwill'] - df['Intangible_Assets']
        
        # Shares outstanding (estimate from EPS if available)
        safe_fill('EPS')
        safe_fill('Diluted_EPS')
        df['Shares_Outstanding'] = df['Net_Income'] / df['EPS'].replace(0, np.nan)
        df['Book_Value_Per_Share'] = df['Total_Equity'] / df['Shares_Outstanding'].replace(0, np.nan)
        df['Tangible_BV_Per_Share'] = df['Tangible_Book_Value'] / df['Shares_Outstanding'].replace(0, np.nan)
        
        # ============ RETURNS ============
        df['ROE'] = df['Net_Income'] / df['Total_Equity'].replace(0, np.nan)
        df['ROA'] = df['Net_Income'] / df['Total_Assets'].replace(0, np.nan)
        df['Return_On_Capital'] = df['Net_Income'] / df['Total_Capital'].replace(0, np.nan)
        df['Return_On_Common_Equity'] = df['Net_Income_Common'] / df['Common_Equity'].replace(0, np.nan)

        # ============ TURNOVERS & DAYS ============
        avg_receivables = (df['Total_Receivables'] + df['Total_Receivables'].shift(1)) / 2
        avg_inventory = (df['Inventory'] + df['Inventory'].shift(1)) / 2
        avg_assets = (df['Total_Assets'] + df['Total_Assets'].shift(1)) / 2
        avg_fixed_assets = (df['Total_PPE_Koyfin'] + df['Total_PPE_Koyfin'].shift(1)) / 2
        avg_payables = (df['AP_Total'] + df['AP_Total'].shift(1)) / 2

        df['Receivables_Turnover'] = df['Revenue'] / avg_receivables.replace(0, np.nan)
        df['Fixed_Assets_Turnover'] = df['Revenue'] / avg_fixed_assets.replace(0, np.nan)
        df['Inventory_Turnover'] = df['COGS'] / avg_inventory.replace(0, np.nan)
        df['Asset_Turnover'] = df['Revenue'] / avg_assets.replace(0, np.nan)
        df['Days_Outstanding_Inventory'] = 365 / df['Inventory_Turnover']
        df['Days_Sales_Outstanding'] = 365 / df['Receivables_Turnover']
        df['Days_Payable_Outstanding'] = 365 / (df['COGS'] / avg_payables.replace(0, np.nan))
        df['Cash_Conversion_Cycle'] = (
            df['Days_Sales_Outstanding'] + df['Days_Outstanding_Inventory'] - df['Days_Payable_Outstanding']
        )
        
        # ROIC Calculation (Koyfin method)
        # NOPAT = EBIT - Actual Income Tax Expense
        df['NOPAT'] = df['EBIT'] - df['Income_Tax_Exp'].fillna(0)
        
        # Invested Capital = Total Debt + Total Equity + Lease Liabilities
        # This is the capital structure approach used by Koyfin
        df['Invested_Capital'] = (df['Total_Debt'].fillna(0) + 
                                  df['Total_Equity'].fillna(0) + 
                                  df['Lease_Liabilities'].fillna(0))
        
        # Average Invested Capital
        df['Avg_Invested_Capital'] = (df['Invested_Capital'] + df['Invested_Capital'].shift(1)) / 2
        
        # ROIC = NOPAT / Average Invested Capital
        df['ROIC'] = df['NOPAT'] / df['Avg_Invested_Capital'].replace(0, np.nan)
        
        # ============ MULTIPLES ============
        df['PE'] = df['Market_Cap'] / df['Net_Income'].replace(0, np.nan)
        df['PS'] = df['Market_Cap'] / df['Revenue'].replace(0, np.nan)
        df['PB'] = df['Market_Cap'] / df['Total_Equity'].replace(0, np.nan)
        df['P_TangibleBV'] = df['Market_Cap'] / df['Tangible_Book_Value'].replace(0, np.nan)
        
        df['EV_Sales'] = df['EV'] / df['Revenue'].replace(0, np.nan)
        df['EV_EBITDA'] = df['EV'] / df['EBITDA'].replace(0, np.nan)
        df['EV_EBIT'] = df['EV'] / df['EBIT'].replace(0, np.nan)
        
        # ============ CASH FLOW ============
        safe_fill('OCF')
        safe_fill('ICF')
        safe_fill('CFF')
        safe_fill('CapEx')
        safe_fill('Dividends_Paid')
        safe_fill('Net_Change_In_Cash')
        safe_fill('FX_Effect')
        safe_fill('Proceeds_From_Borrowings')
        safe_fill('Repayment_Of_Debt')
        safe_fill('Bond_Issuance')
        safe_fill('Proceeds_From_Equity')
        safe_fill('Proceeds_From_Asset_Sales')
        safe_fill('Cash_For_Investments')
        safe_fill('Proceeds_From_Investment_Sales')
        safe_fill('Cash_Acquisitions')
        safe_fill('Cash_Divestitures')
        safe_fill('Other_Operating_Cash_In')
        safe_fill('Other_Operating_Cash_Out')
        safe_fill('Other_Investing_Cash_In')
        safe_fill('Other_Investing_Cash_Out')
        safe_fill('Other_Financing_Cash_In')
        safe_fill('Other_Financing_Cash_Out')
        safe_fill('Minority_Investment_Received')
        safe_fill('Minority_Dividends_Paid')
        
        # D&A estimation - try multiple methods
        safe_fill('Net_PPE')
        safe_fill('Construction_In_Progress')
        safe_fill('Accumulated_Depreciation')
        safe_fill('Intangible_Assets')
        
        # Method 1: From accumulated depreciation change
        df['Accumulated_Depreciation'] = df['Accumulated_Depreciation'].ffill()
        df['DA_From_Accum'] = df['Accumulated_Depreciation'].diff()
        
        # Method 2: From PPE + CapEx
        df['Total_PPE'] = df['Net_PPE'] + df['Construction_In_Progress']
        df['DA_From_PPE'] = df['Total_PPE'].shift(1) + df['CapEx'].abs() - df['Total_PPE']
        
        # Use accumulated depreciation change if available, otherwise PPE method
        df['DA'] = df['DA_From_Accum'].where(df['DA_From_Accum'] > 0, df['DA_From_PPE'])
        df['DA'] = df['DA'].clip(lower=0).fillna(df['DA_Estimated'])

        # Split Depreciation and Amortization (best-effort)
        df['Depreciation'] = df['DA_From_Accum'].clip(lower=0)
        df['Amortization'] = (df['DA'] - df['Depreciation']).clip(lower=0)

        # EBITA (EBIT + Amortization)
        df['EBITA'] = df['EBIT'] + df['Amortization']
        df['EBITA_Margin'] = df['EBITA'] / df['Revenue'].replace(0, np.nan)
        
        # Recalculate EBITDA with better D&A
        df['EBITDA'] = df['EBIT'] + df['DA']
        
        # Free Cash Flow
        df['FCF'] = df['OCF'] - df['CapEx'].abs()
        df['FCF_Per_Share'] = df['FCF'] / df['Shares_Outstanding'].replace(0, np.nan)
        df['FCF_Yield'] = df['FCF'] / df['Market_Cap'].replace(0, np.nan)
        df['EV_OCF'] = df['EV'] / df['OCF'].replace(0, np.nan)
        
        # Debt Issued / Repaid
        df['Total_Debt_Issued'] = df['Proceeds_From_Borrowings'] + df['Bond_Issuance']
        df['Total_Debt_Repaid'] = df['Repayment_Of_Debt']
        df['Net_Debt_Issued'] = df['Total_Debt_Issued'] - df['Total_Debt_Repaid'].abs()
        
        # Common Dividends (exclude minority)
        df['Common_Dividends_Paid'] = df['Dividends_Paid'] - df['Minority_Dividends_Paid'].abs()
        
        # Other activities totals
        df['Other_Operating_Activities'] = df['Other_Operating_Cash_In'] - df['Other_Operating_Cash_Out'].abs()
        df['Other_Investing_Activities'] = df['Other_Investing_Cash_In'] - df['Other_Investing_Cash_Out'].abs()
        df['Other_Financing_Activities'] = df['Other_Financing_Cash_In'] - df['Other_Financing_Cash_Out'].abs()
        
        # Cash from Investing components
        df['Investment_In_Securities'] = df['Cash_For_Investments'] - df['Proceeds_From_Investment_Sales']
        
        # ============ SOLVENCY / LEVERAGE ============
        df['Debt_to_Equity'] = df['Total_Debt'] / df['Total_Equity'].replace(0, np.nan)
        df['Debt_to_Capital'] = df['Total_Debt'] / df['Total_Capital'].replace(0, np.nan)
        df['LT_Debt_to_Equity'] = df['Long_Term_Debt'] / df['Total_Equity'].replace(0, np.nan)
        df['LT_Debt_to_Capital'] = df['Long_Term_Debt'] / df['Total_Capital'].replace(0, np.nan)
        df['Liabilities_to_Assets'] = df['Total_Liabilities'] / df['Total_Assets'].replace(0, np.nan)
        
        # Coverage Ratios
        int_exp_safe = df['Interest_Exp'].replace(0, np.nan)
        df['Interest_Coverage_EBIT'] = df['EBIT'] / int_exp_safe
        df['Interest_Coverage_EBITDA'] = df['EBITDA'] / int_exp_safe
        df['Interest_Coverage_EBITDA_CapEx'] = (df['EBITDA'] - df['CapEx'].abs()) / int_exp_safe
        
        # Debt Coverage
        ebitda_safe = df['EBITDA'].replace(0, np.nan)
        df['Debt_to_EBITDA'] = df['Total_Debt'] / ebitda_safe
        df['Net_Debt_to_EBITDA'] = df['Net_Debt'] / ebitda_safe
        
        df['Current_Ratio'] = df['Total_Current_Assets'] / df['Total_Current_Liabilities'].replace(0, np.nan)
        df['Quick_Ratio'] = (df['Total_Current_Assets'] - df['Inventory']) / df['Total_Current_Liabilities'].replace(0, np.nan)
        df['Operating_Cash_Flow_to_Current_Liabilities'] = df['OCF'] / df['Total_Current_Liabilities'].replace(0, np.nan)
        
        # Altman Z-Score (Z'' formula for emerging markets - closer to Koyfin)
        # Z'' = 6.56*X1 + 3.26*X2 + 6.72*X3 + 1.05*X4
        # X1 = Working Capital / Total Assets
        # X2 = Retained Earnings / Total Assets
        # X3 = EBIT / Total Assets
        # X4 = Book Value of Equity / Total Liabilities
        safe_fill('Retained_Earnings')
        X1 = df['Working_Capital'] / df['Total_Assets'].replace(0, np.nan)
        X2 = df['Retained_Earnings'] / df['Total_Assets'].replace(0, np.nan)
        X3 = df['EBIT'] / df['Total_Assets'].replace(0, np.nan)
        X4 = df['Total_Equity'] / df['Total_Liabilities'].replace(0, np.nan)
        df['Altman_Z_Score'] = 6.56 * X1 + 3.26 * X2 + 6.72 * X3 + 1.05 * X4
        
        return df

    def add_yoy(df, is_ltm=True):
        periods = 4 if is_ltm else 1
        df['Rev_YoY'] = df['Revenue'].pct_change(periods)
        df['Gross_Profit_YoY'] = df['Gross_Profit'].pct_change(periods)
        df['EBITDA_YoY'] = df['EBITDA'].pct_change(periods)
        df['NetInc_YoY'] = df['Net_Income'].pct_change(periods)
        df['EPS_YoY'] = df['EPS'].pct_change(periods)
        df['OCF_YoY'] = df['OCF'].pct_change(periods)
        df['CapEx_YoY'] = df['CapEx'].pct_change(periods)
        return df

    res_ltm = calculate_derived(res_ltm)
    res_ltm = add_yoy(res_ltm, True)
    res_annual = calculate_derived(res_annual)
    res_annual = add_yoy(res_annual, False)
    
    os.makedirs("outputs/002508_analysis", exist_ok=True)
    res_ltm.to_csv("outputs/002508_analysis/ltm_metrics.csv")
    res_annual.to_csv("outputs/002508_analysis/annual_metrics.csv")
    print(f"Generated LTM metrics: {len(res_ltm)} rows, {len(res_ltm.columns)} columns")
    print(f"Generated Annual metrics: {len(res_annual)} rows, {len(res_annual.columns)} columns")

if __name__ == "__main__":
    process_financials()
