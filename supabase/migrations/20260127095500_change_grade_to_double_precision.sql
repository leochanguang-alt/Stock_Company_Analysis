-- 将 cn_company_news.grade 从 TEXT 调整为数值（DOUBLE PRECISION）
-- 便于后续计算/排序/统计

-- 0) 移除依赖（view/trigger/function）
DROP TRIGGER IF EXISTS cn_company_news_insert_trigger ON public.cn_company_news;
DROP FUNCTION IF EXISTS public.cn_company_news_insert();
DROP VIEW IF EXISTS public.cn_company_news;

-- 1) 修改底层表字段类型
ALTER TABLE IF EXISTS stock_analysis.cn_company_news
  ALTER COLUMN grade TYPE DOUBLE PRECISION
  USING NULLIF(grade::text, '')::double precision;

COMMENT ON COLUMN stock_analysis.cn_company_news.grade IS '新闻影响评分（-10~10，可为空）';

-- 2) 重建 public 视图与 upsert 触发器
CREATE OR REPLACE VIEW public.cn_company_news AS
SELECT * FROM stock_analysis.cn_company_news;

CREATE OR REPLACE FUNCTION public.cn_company_news_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stock_analysis.cn_company_news (
        symbol,
        news_title,
        news_content,
        published_at,
        source,
        news_url,
        grade,
        mkt_change_1_month,
        reason
    ) VALUES (
        NEW.symbol,
        NEW.news_title,
        NEW.news_content,
        NEW.published_at,
        NEW.source,
        NEW.news_url,
        NEW.grade,
        NEW.mkt_change_1_month,
        NEW.reason
    )
    ON CONFLICT (news_url) DO UPDATE SET
        symbol = EXCLUDED.symbol,
        news_title = EXCLUDED.news_title,
        news_content = EXCLUDED.news_content,
        published_at = EXCLUDED.published_at,
        source = EXCLUDED.source,
        grade = EXCLUDED.grade,
        mkt_change_1_month = EXCLUDED.mkt_change_1_month,
        reason = EXCLUDED.reason,
        updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE TRIGGER cn_company_news_insert_trigger
    INSTEAD OF INSERT ON public.cn_company_news
    FOR EACH ROW
    EXECUTE FUNCTION public.cn_company_news_insert();

GRANT SELECT, INSERT ON public.cn_company_news TO anon, authenticated, service_role;

