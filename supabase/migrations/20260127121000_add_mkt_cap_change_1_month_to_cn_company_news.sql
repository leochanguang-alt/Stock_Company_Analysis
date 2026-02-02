-- 给 cn_company_news 增加字段：mkt_cap_change_1_month（文本：百分比或 n/a）

-- 0) 移除依赖（view/trigger/function）
DROP TRIGGER IF EXISTS cn_company_news_insert_trigger ON public.cn_company_news;
DROP FUNCTION IF EXISTS public.cn_company_news_insert();
DROP VIEW IF EXISTS public.cn_company_news;

-- 1) 修改底层表：新增字段
ALTER TABLE IF EXISTS stock_analysis.cn_company_news
  ADD COLUMN IF NOT EXISTS mkt_cap_change_1_month TEXT;

COMMENT ON COLUMN stock_analysis.cn_company_news.mkt_cap_change_1_month IS '发布后1个月市值涨跌幅（%），不足1个月则 n/a';

-- 2) 重建 public 视图与 upsert 触发器（包含新字段）
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
        reason,
        mkt_cap_change_1_month
    ) VALUES (
        NEW.symbol,
        NEW.news_title,
        NEW.news_content,
        NEW.published_at,
        NEW.source,
        NEW.news_url,
        NEW.grade,
        NEW.mkt_change_1_month,
        NEW.reason,
        NEW.mkt_cap_change_1_month
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
        mkt_cap_change_1_month = EXCLUDED.mkt_cap_change_1_month,
        updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE TRIGGER cn_company_news_insert_trigger
    INSTEAD OF INSERT ON public.cn_company_news
    FOR EACH ROW
    EXECUTE FUNCTION public.cn_company_news_insert();

GRANT SELECT, INSERT ON public.cn_company_news TO anon, authenticated, service_role;

