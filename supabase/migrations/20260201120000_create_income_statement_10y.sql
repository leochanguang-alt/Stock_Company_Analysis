-- 利润表 (10年) 宽表
-- 存储从 AKShare 下载的完整利润表数据

CREATE TABLE IF NOT EXISTS public.income_statement_10y (
    id BIGSERIAL PRIMARY KEY,
    
    -- 基本信息
    report_date DATE NOT NULL,                    -- 报告日 (20250930 -> 2025-09-30)
    symbol VARCHAR(20) NOT NULL,                  -- 股票代码 (600031)
    secucode VARCHAR(20),                         -- 证券代码 (600031.SH)
    security_name VARCHAR(100),                   -- 证券简称
    report_date_name VARCHAR(50),                 -- 报告期名称 (2025三季报)
    
    -- 一、营业总收入
    total_operate_income NUMERIC,                 -- 营业总收入
    operate_income NUMERIC,                       -- 营业收入
    interest_income NUMERIC,                      -- 利息收入
    earned_premium NUMERIC,                       -- 已赚保费
    fee_commission_income NUMERIC,                -- 手续费及佣金收入
    other_business_income NUMERIC,                -- 其他业务收入
    
    -- 二、营业总成本
    total_operate_cost NUMERIC,                   -- 营业总成本
    operate_cost NUMERIC,                         -- 营业成本
    interest_expense NUMERIC,                     -- 利息支出
    fee_commission_expense NUMERIC,               -- 手续费及佣金支出
    research_expense NUMERIC,                     -- 研发费用
    operate_tax_add NUMERIC,                      -- 税金及附加
    sale_expense NUMERIC,                         -- 销售费用
    manage_expense NUMERIC,                       -- 管理费用
    finance_expense NUMERIC,                      -- 财务费用
    fe_interest_expense NUMERIC,                  -- 其中：利息费用
    fe_interest_income NUMERIC,                   -- 其中：利息收入
    asset_impairment_loss NUMERIC,                -- 资产减值损失
    credit_impairment_loss NUMERIC,               -- 信用减值损失
    
    -- 三、其他收益
    fairvalue_change_income NUMERIC,              -- 公允价值变动收益
    invest_income NUMERIC,                        -- 投资收益
    invest_joint_income NUMERIC,                  -- 其中：对联营企业和合营企业的投资收益
    exchange_income NUMERIC,                      -- 汇兑收益
    asset_disposal_income NUMERIC,                -- 资产处置收益
    asset_impairment_income NUMERIC,              -- 资产减值收益（冲回）
    credit_impairment_income NUMERIC,             -- 信用减值收益（冲回）
    other_income NUMERIC,                         -- 其他收益
    
    -- 四、营业利润
    operate_profit NUMERIC,                       -- 营业利润
    
    -- 五、营业外收支
    nonbusiness_income NUMERIC,                   -- 营业外收入
    noncurrent_disposal_income NUMERIC,           -- 其中：非流动资产处置利得
    nonbusiness_expense NUMERIC,                  -- 营业外支出
    noncurrent_disposal_loss NUMERIC,             -- 其中：非流动资产处置损失
    
    -- 六、利润总额
    total_profit NUMERIC,                         -- 利润总额
    
    -- 七、所得税费用
    income_tax NUMERIC,                           -- 所得税费用
    
    -- 八、净利润
    netprofit NUMERIC,                            -- 净利润
    continued_netprofit NUMERIC,                  -- 持续经营净利润
    discontinued_netprofit NUMERIC,               -- 终止经营净利润
    parent_netprofit NUMERIC,                     -- 归属于母公司所有者的净利润
    minority_interest NUMERIC,                    -- 少数股东损益
    deduct_parent_netprofit NUMERIC,              -- 扣除非经常性损益后的净利润
    
    -- 九、每股收益
    basic_eps NUMERIC,                            -- 基本每股收益
    diluted_eps NUMERIC,                          -- 稀释每股收益
    
    -- 十、其他综合收益
    other_compre_income NUMERIC,                  -- 其他综合收益
    parent_oci NUMERIC,                           -- 归属母公司所有者的其他综合收益
    minority_oci NUMERIC,                         -- 归属少数股东的其他综合收益
    total_compre_income NUMERIC,                  -- 综合收益总额
    parent_tci NUMERIC,                           -- 归属于母公司所有者的综合收益总额
    minority_tci NUMERIC,                         -- 归属于少数股东的综合收益总额
    
    -- 同比增长率 (YOY - Year over Year) - 关键指标
    total_operate_income_yoy NUMERIC,             -- 营业总收入同比
    operate_income_yoy NUMERIC,                   -- 营业收入同比
    total_operate_cost_yoy NUMERIC,               -- 营业总成本同比
    operate_cost_yoy NUMERIC,                     -- 营业成本同比
    operate_profit_yoy NUMERIC,                   -- 营业利润同比
    total_profit_yoy NUMERIC,                     -- 利润总额同比
    netprofit_yoy NUMERIC,                        -- 净利润同比
    parent_netprofit_yoy NUMERIC,                 -- 归母净利润同比
    deduct_parent_netprofit_yoy NUMERIC,          -- 扣非净利润同比
    basic_eps_yoy NUMERIC,                        -- 基本每股收益同比
    
    -- 元数据
    data_source VARCHAR(50) DEFAULT 'EastMoney',  -- 数据源
    opinion_type VARCHAR(50),                     -- 审计意见类型
    is_audited BOOLEAN,                           -- 是否审计
    announcement_date DATE,                       -- 公告日期
    currency VARCHAR(10) DEFAULT 'CNY',           -- 币种
    report_type VARCHAR(20),                      -- 报告类型 (年报/中报/季报)
    updated_at TIMESTAMP DEFAULT NOW(),           -- 更新日期
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 唯一约束：同一股票同一报告期只能有一条记录
    CONSTRAINT income_statement_10y_unique UNIQUE (symbol, report_date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_income_stmt_symbol ON public.income_statement_10y(symbol);
CREATE INDEX IF NOT EXISTS idx_income_stmt_report_date ON public.income_statement_10y(report_date);
CREATE INDEX IF NOT EXISTS idx_income_stmt_symbol_date ON public.income_statement_10y(symbol, report_date DESC);

-- 添加注释
COMMENT ON TABLE public.income_statement_10y IS '利润表（10年历史数据）';
COMMENT ON COLUMN public.income_statement_10y.report_date IS '报告日期';
COMMENT ON COLUMN public.income_statement_10y.symbol IS '股票代码（纯数字6位）';
COMMENT ON COLUMN public.income_statement_10y.total_operate_income IS '营业总收入';
COMMENT ON COLUMN public.income_statement_10y.operate_profit IS '营业利润';
COMMENT ON COLUMN public.income_statement_10y.netprofit IS '净利润';
COMMENT ON COLUMN public.income_statement_10y.parent_netprofit IS '归属于母公司所有者的净利润';

-- 启用 RLS (Row Level Security)
ALTER TABLE public.income_statement_10y ENABLE ROW LEVEL SECURITY;

-- 创建公开读取策略
CREATE POLICY "Allow public read access" ON public.income_statement_10y
    FOR SELECT USING (true);

-- 创建服务角色写入策略
CREATE POLICY "Allow service role full access" ON public.income_statement_10y
    FOR ALL USING (auth.role() = 'service_role');
