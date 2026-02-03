-- 现金流量表 (10年) 宽表
-- 存储从 AKShare 下载的完整现金流量表数据

CREATE TABLE IF NOT EXISTS public.cash_flow_10y (
    id BIGSERIAL PRIMARY KEY,
    
    -- 基本信息
    report_date DATE NOT NULL,                    -- 报告日 (20250930 -> 2025-09-30)
    symbol VARCHAR(20) NOT NULL,                  -- 股票代码 (600031)
    secucode VARCHAR(20),                         -- 证券代码 (600031.SH)
    security_name VARCHAR(100),                   -- 证券简称
    report_date_name VARCHAR(50),                 -- 报告期名称 (2025三季报)
    
    -- 一、经营活动产生的现金流量
    sales_services NUMERIC,                       -- 销售商品、提供劳务收到的现金
    receive_tax_refund NUMERIC,                   -- 收到的税费返还
    receive_other_operate NUMERIC,                -- 收到其他与经营活动有关的现金
    total_operate_inflow NUMERIC,                 -- 经营活动现金流入小计
    
    buy_services NUMERIC,                         -- 购买商品、接受劳务支付的现金
    pay_staff_cash NUMERIC,                       -- 支付给职工以及为职工支付的现金
    pay_all_tax NUMERIC,                          -- 支付的各项税费
    pay_other_operate NUMERIC,                    -- 支付其他与经营活动有关的现金
    total_operate_outflow NUMERIC,                -- 经营活动现金流出小计
    
    netcash_operate NUMERIC,                      -- 经营活动产生的现金流量净额
    
    -- 二、投资活动产生的现金流量
    withdraw_invest NUMERIC,                      -- 收回投资收到的现金
    receive_invest_income NUMERIC,                -- 取得投资收益收到的现金
    disposal_long_asset NUMERIC,                  -- 处置固定资产、无形资产和其他长期资产收回的现金净额
    disposal_subsidiary_other NUMERIC,            -- 处置子公司及其他营业单位收到的现金净额
    receive_other_invest NUMERIC,                 -- 收到其他与投资活动有关的现金
    total_invest_inflow NUMERIC,                  -- 投资活动现金流入小计
    
    construct_long_asset NUMERIC,                 -- 购建固定资产、无形资产和其他长期资产支付的现金
    invest_pay_cash NUMERIC,                      -- 投资支付的现金
    obtain_subsidiary_other NUMERIC,              -- 取得子公司及其他营业单位支付的现金净额
    pay_other_invest NUMERIC,                     -- 支付其他与投资活动有关的现金
    total_invest_outflow NUMERIC,                 -- 投资活动现金流出小计
    
    netcash_invest NUMERIC,                       -- 投资活动产生的现金流量净额
    
    -- 三、筹资活动产生的现金流量
    accept_invest_cash NUMERIC,                   -- 吸收投资收到的现金
    subsidiary_accept_invest NUMERIC,             -- 其中：子公司吸收少数股东投资收到的现金
    receive_loan_cash NUMERIC,                    -- 取得借款收到的现金
    issue_bond NUMERIC,                           -- 发行债券收到的现金
    receive_other_finance NUMERIC,                -- 收到其他与筹资活动有关的现金
    total_finance_inflow NUMERIC,                 -- 筹资活动现金流入小计
    
    pay_debt_cash NUMERIC,                        -- 偿还债务支付的现金
    assign_dividend_porfit NUMERIC,               -- 分配股利、利润或偿付利息支付的现金
    subsidiary_pay_dividend NUMERIC,              -- 其中：子公司支付给少数股东的股利、利润
    buy_subsidiary_equity NUMERIC,                -- 购买子公司少数股东股权支付的现金
    pay_other_finance NUMERIC,                    -- 支付其他与筹资活动有关的现金
    total_finance_outflow NUMERIC,                -- 筹资活动现金流出小计
    
    netcash_finance NUMERIC,                      -- 筹资活动产生的现金流量净额
    
    -- 四、汇率变动对现金的影响
    rate_change_effect NUMERIC,                   -- 汇率变动对现金及现金等价物的影响
    
    -- 五、现金及现金等价物净增加额
    cce_add NUMERIC,                              -- 现金及现金等价物净增加额
    begin_cce NUMERIC,                            -- 期初现金及现金等价物余额
    end_cce NUMERIC,                              -- 期末现金及现金等价物余额
    
    -- 补充资料
    netprofit NUMERIC,                            -- 净利润
    asset_impairment NUMERIC,                     -- 资产减值准备
    fa_ir_depr NUMERIC,                           -- 固定资产折旧、油气资产折耗、生产性生物资产折旧
    ia_amortize NUMERIC,                          -- 无形资产摊销
    lpe_amortize NUMERIC,                         -- 长期待摊费用摊销
    defer_income_amortize NUMERIC,                -- 递延收益摊销（负数）
    disposal_longasset_loss NUMERIC,              -- 处置固定资产、无形资产和其他长期资产的损失
    fa_scrap_loss NUMERIC,                        -- 固定资产报废损失
    fairvalue_change_loss NUMERIC,                -- 公允价值变动损失
    finance_expense NUMERIC,                      -- 财务费用
    invest_loss NUMERIC,                          -- 投资损失
    defer_tax NUMERIC,                            -- 递延所得税
    inventory_reduce NUMERIC,                     -- 存货的减少
    operate_rece_reduce NUMERIC,                  -- 经营性应收项目的减少
    operate_payable_add NUMERIC,                  -- 经营性应付项目的增加
    
    -- 同比增长率 (YOY - Year over Year)
    netcash_operate_yoy NUMERIC,                  -- 经营活动现金流量净额同比
    netcash_invest_yoy NUMERIC,                   -- 投资活动现金流量净额同比
    netcash_finance_yoy NUMERIC,                  -- 筹资活动现金流量净额同比
    cce_add_yoy NUMERIC,                          -- 现金净增加额同比
    total_operate_inflow_yoy NUMERIC,             -- 经营活动现金流入同比
    total_operate_outflow_yoy NUMERIC,            -- 经营活动现金流出同比
    
    -- 元数据
    data_source VARCHAR(50) DEFAULT 'EastMoney',  -- 数据源
    is_audited BOOLEAN,                           -- 是否审计
    announcement_date DATE,                       -- 公告日期
    currency VARCHAR(10) DEFAULT 'CNY',           -- 币种
    report_type VARCHAR(20),                      -- 报告类型 (年报/中报/季报)
    updated_at TIMESTAMP DEFAULT NOW(),           -- 更新日期
    
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- 唯一约束：同一股票同一报告期只能有一条记录
    CONSTRAINT cash_flow_10y_unique UNIQUE (symbol, report_date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol ON public.cash_flow_10y(symbol);
CREATE INDEX IF NOT EXISTS idx_cash_flow_report_date ON public.cash_flow_10y(report_date);
CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol_date ON public.cash_flow_10y(symbol, report_date DESC);

-- 添加注释
COMMENT ON TABLE public.cash_flow_10y IS '现金流量表（10年历史数据）';
COMMENT ON COLUMN public.cash_flow_10y.report_date IS '报告日期';
COMMENT ON COLUMN public.cash_flow_10y.symbol IS '股票代码（纯数字6位）';
COMMENT ON COLUMN public.cash_flow_10y.netcash_operate IS '经营活动产生的现金流量净额';
COMMENT ON COLUMN public.cash_flow_10y.netcash_invest IS '投资活动产生的现金流量净额';
COMMENT ON COLUMN public.cash_flow_10y.netcash_finance IS '筹资活动产生的现金流量净额';

-- 启用 RLS (Row Level Security)
ALTER TABLE public.cash_flow_10y ENABLE ROW LEVEL SECURITY;

-- 创建公开读取策略
CREATE POLICY "Allow public read access" ON public.cash_flow_10y
    FOR SELECT USING (true);

-- 创建服务角色写入策略
CREATE POLICY "Allow service role full access" ON public.cash_flow_10y
    FOR ALL USING (auth.role() = 'service_role');
