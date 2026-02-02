-- 修正 cn_company_news 字段：keyword -> symbol (text)

-- 0) 先移除依赖视图（否则无法改列类型）
DROP TRIGGER IF EXISTS cn_company_news_insert_trigger ON public.cn_company_news;
DROP FUNCTION IF EXISTS public.cn_company_news_insert();
DROP VIEW IF EXISTS public.cn_company_news;

-- 1) 修改底层表字段
ALTER TABLE IF EXISTS stock_analysis.cn_company_news
  RENAME COLUMN keyword TO symbol;

ALTER TABLE IF EXISTS stock_analysis.cn_company_news
  ALTER COLUMN symbol TYPE TEXT USING symbol::text;

COMMENT ON COLUMN stock_analysis.cn_company_news.symbol IS '证券代码/关键词（如 002508）';

-- 2) 重新创建索引（列名变化需要重建）
DROP INDEX IF EXISTS stock_analysis.idx_cn_company_news_keyword;
CREATE INDEX IF NOT EXISTS idx_cn_company_news_symbol
  ON stock_analysis.cn_company_news(symbol);

-- 3) 更新 public 视图与触发器函数（字段名变化）
CREATE OR REPLACE VIEW public.cn_company_news AS
SELECT * FROM stock_analysis.cn_company_news;

CREATE OR REPLACE FUNCTION public.cn_company_news_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stock_analysis.cn_company_news (
        symbol, news_title, news_content, published_at, source, news_url
    ) VALUES (
        NEW.symbol, NEW.news_title, NEW.news_content, NEW.published_at, NEW.source, NEW.news_url
    )
    ON CONFLICT (news_url) DO UPDATE SET
        symbol = EXCLUDED.symbol,
        news_title = EXCLUDED.news_title,
        news_content = EXCLUDED.news_content,
        published_at = EXCLUDED.published_at,
        source = EXCLUDED.source,
        updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE TRIGGER cn_company_news_insert_trigger
    INSTEAD OF INSERT ON public.cn_company_news
    FOR EACH ROW
    EXECUTE FUNCTION public.cn_company_news_insert();

GRANT SELECT, INSERT ON public.cn_company_news TO anon, authenticated, service_role;

