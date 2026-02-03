-- 重命名 income_statement_10y 为 cn_income_statement_10y
-- 保持与其他表命名一致

-- 重命名表
ALTER TABLE IF EXISTS public.income_statement_10y 
    RENAME TO cn_income_statement_10y;

-- 重命名约束
ALTER TABLE IF EXISTS public.cn_income_statement_10y 
    RENAME CONSTRAINT income_statement_10y_unique TO cn_income_statement_10y_unique;

-- 重命名索引
ALTER INDEX IF EXISTS idx_income_stmt_symbol RENAME TO idx_cn_income_stmt_symbol;
ALTER INDEX IF EXISTS idx_income_stmt_report_date RENAME TO idx_cn_income_stmt_report_date;
ALTER INDEX IF EXISTS idx_income_stmt_symbol_date RENAME TO idx_cn_income_stmt_symbol_date;

-- 更新表注释
COMMENT ON TABLE public.cn_income_statement_10y IS '中国A股利润表（10年历史数据）';
