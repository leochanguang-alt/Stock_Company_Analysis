-- 资产负债表 (10年) 宽表
-- 存储从 AKShare 下载的完整资产负债表数据

CREATE TABLE IF NOT EXISTS public.balance_sheet_10y (
    id BIGSERIAL PRIMARY KEY,
    
    -- 基本信息
    report_date DATE NOT NULL,                    -- 报告日 (20250930 -> 2025-09-30)
    symbol VARCHAR(20) NOT NULL,                  -- 股票代码 (600031)
    secucode VARCHAR(20),                         -- 证券代码 (600031.SH)
    security_name VARCHAR(100),                   -- 证券简称
    report_date_name VARCHAR(50),                 -- 报告期名称 (2025三季报)
    
    -- 流动资产
    monetaryfunds NUMERIC,                        -- 货币资金
    note_rece NUMERIC,                            -- 应收票据
    accounts_rece NUMERIC,                        -- 应收账款
    prepayment NUMERIC,                           -- 预付款项
    other_rece NUMERIC,                           -- 其他应收款
    inventory NUMERIC,                            -- 存货
    contract_asset NUMERIC,                       -- 合同资产
    holdsale_asset NUMERIC,                       -- 持有待售资产
    noncurrent_asset_1year NUMERIC,               -- 一年内到期的非流动资产
    other_current_asset NUMERIC,                  -- 其他流动资产
    total_current_assets NUMERIC,                 -- 流动资产合计
    
    -- 非流动资产
    long_rece NUMERIC,                            -- 长期应收款
    long_equity_invest NUMERIC,                   -- 长期股权投资
    other_equity_invest NUMERIC,                  -- 其他权益工具投资
    other_noncurrent_finasset NUMERIC,            -- 其他非流动金融资产
    invest_realestate NUMERIC,                    -- 投资性房地产
    fixed_asset NUMERIC,                          -- 固定资产
    cip NUMERIC,                                  -- 在建工程
    useright_asset NUMERIC,                       -- 使用权资产
    intangible_asset NUMERIC,                     -- 无形资产
    develop_expense NUMERIC,                      -- 开发支出
    goodwill NUMERIC,                             -- 商誉
    long_prepaid_expense NUMERIC,                 -- 长期待摊费用
    defer_tax_asset NUMERIC,                      -- 递延所得税资产
    other_noncurrent_asset NUMERIC,               -- 其他非流动资产
    total_noncurrent_assets NUMERIC,              -- 非流动资产合计
    
    -- 资产总计
    total_assets NUMERIC,                         -- 资产总计
    
    -- 流动负债
    short_loan NUMERIC,                           -- 短期借款
    note_payable NUMERIC,                         -- 应付票据
    accounts_payable NUMERIC,                     -- 应付账款
    contract_liab NUMERIC,                        -- 合同负债
    staff_salary_payable NUMERIC,                 -- 应付职工薪酬
    tax_payable NUMERIC,                          -- 应交税费
    other_payable NUMERIC,                        -- 其他应付款
    noncurrent_liab_1year NUMERIC,                -- 一年内到期的非流动负债
    other_current_liab NUMERIC,                   -- 其他流动负债
    total_current_liab NUMERIC,                   -- 流动负债合计
    
    -- 非流动负债
    long_loan NUMERIC,                            -- 长期借款
    bond_payable NUMERIC,                         -- 应付债券
    lease_liab NUMERIC,                           -- 租赁负债
    long_payable NUMERIC,                         -- 长期应付款
    predict_liab NUMERIC,                         -- 预计负债
    defer_income NUMERIC,                         -- 递延收益
    defer_tax_liab NUMERIC,                       -- 递延所得税负债
    other_noncurrent_liab NUMERIC,                -- 其他非流动负债
    total_noncurrent_liab NUMERIC,                -- 非流动负债合计
    
    -- 负债合计
    total_liabilities NUMERIC,                    -- 负债合计
    
    -- 股东权益
    share_capital NUMERIC,                        -- 股本
    capital_reserve NUMERIC,                      -- 资本公积
    treasury_shares NUMERIC,                      -- 库存股
    other_compre_income NUMERIC,                  -- 其他综合收益
    special_reserve NUMERIC,                      -- 专项储备
    surplus_reserve NUMERIC,                      -- 盈余公积
    unassign_rpofit NUMERIC,                      -- 未分配利润
    total_parent_equity NUMERIC,                  -- 归属于母公司股东权益合计
    minority_equity NUMERIC,                      -- 少数股东权益
    total_equity NUMERIC,                         -- 股东权益合计
    
    -- 负债和股东权益总计
    total_liab_equity NUMERIC,                    -- 负债和股东权益总计
    
    -- 同比增长率 (YOY - Year over Year)
    total_assets_yoy NUMERIC,                     -- 资产总计同比
    total_liabilities_yoy NUMERIC,                -- 负债合计同比
    total_equity_yoy NUMERIC,                     -- 股东权益合计同比
    total_current_assets_yoy NUMERIC,             -- 流动资产同比
    total_noncurrent_assets_yoy NUMERIC,          -- 非流动资产同比
    
    -- 元数据
    data_source VARCHAR(50) DEFAULT 'EastMoney',  -- 数据源
    is_audited BOOLEAN,                           -- 是否审计
    announcement_date DATE,                       -- 公告日期
    currency VARCHAR(10) DEFAULT 'CNY',           -- 币种
    report_type VARCHAR(20),                      -- 报告类型 (年报/中报/季报)
    updated_at TIMESTAMP DEFAULT NOW(),           -- 更新日期
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 唯一约束：同一股票同一报告期只能有一条记录
    CONSTRAINT balance_sheet_10y_unique UNIQUE (symbol, report_date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol ON public.balance_sheet_10y(symbol);
CREATE INDEX IF NOT EXISTS idx_balance_sheet_report_date ON public.balance_sheet_10y(report_date);
CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol_date ON public.balance_sheet_10y(symbol, report_date DESC);

-- 添加注释
COMMENT ON TABLE public.balance_sheet_10y IS '资产负债表（10年历史数据）';
COMMENT ON COLUMN public.balance_sheet_10y.report_date IS '报告日期';
COMMENT ON COLUMN public.balance_sheet_10y.symbol IS '股票代码（纯数字6位）';
COMMENT ON COLUMN public.balance_sheet_10y.total_assets IS '资产总计';
COMMENT ON COLUMN public.balance_sheet_10y.total_liabilities IS '负债合计';
COMMENT ON COLUMN public.balance_sheet_10y.total_equity IS '股东权益合计';

-- 启用 RLS (Row Level Security)
ALTER TABLE public.balance_sheet_10y ENABLE ROW LEVEL SECURITY;

-- 创建公开读取策略
CREATE POLICY "Allow public read access" ON public.balance_sheet_10y
    FOR SELECT USING (true);

-- 创建服务角色写入策略
CREATE POLICY "Allow service role full access" ON public.balance_sheet_10y
    FOR ALL USING (auth.role() = 'service_role');
