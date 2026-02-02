-- 在 public schema 中创建 cn_sharehold_data 的视图，使其可通过 REST API 访问
-- 同时创建触发器允许通过视图插入数据

-- 创建可更新视图
CREATE OR REPLACE VIEW public.cn_sharehold_data AS
SELECT * FROM stock_analysis.cn_sharehold_data;

-- 创建 INSTEAD OF INSERT 触发器函数
CREATE OR REPLACE FUNCTION public.cn_sharehold_data_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stock_analysis.cn_sharehold_data (
        symbol, name, report_date, current_holder_count, previous_holder_count,
        holder_count_change_pct, current_avg_shares, previous_avg_shares,
        avg_shares_change_pct, report_period
    ) VALUES (
        NEW.symbol, NEW.name, NEW.report_date, NEW.current_holder_count, NEW.previous_holder_count,
        NEW.holder_count_change_pct, NEW.current_avg_shares, NEW.previous_avg_shares,
        NEW.avg_shares_change_pct, NEW.report_period
    )
    ON CONFLICT (symbol, report_date) DO UPDATE SET
        name = EXCLUDED.name,
        current_holder_count = EXCLUDED.current_holder_count,
        previous_holder_count = EXCLUDED.previous_holder_count,
        holder_count_change_pct = EXCLUDED.holder_count_change_pct,
        current_avg_shares = EXCLUDED.current_avg_shares,
        previous_avg_shares = EXCLUDED.previous_avg_shares,
        avg_shares_change_pct = EXCLUDED.avg_shares_change_pct,
        report_period = EXCLUDED.report_period,
        updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 创建触发器
DROP TRIGGER IF EXISTS cn_sharehold_data_insert_trigger ON public.cn_sharehold_data;
CREATE TRIGGER cn_sharehold_data_insert_trigger
    INSTEAD OF INSERT ON public.cn_sharehold_data
    FOR EACH ROW
    EXECUTE FUNCTION public.cn_sharehold_data_insert();

-- 授予权限
GRANT SELECT, INSERT ON public.cn_sharehold_data TO anon, authenticated, service_role;
