-- 创建中国A股公司新闻表
-- 数据来源: outputs/002508_stock_news_em_6m.csv
--
-- CSV 列:
-- 关键词, 新闻标题, 新闻内容, 发布时间, 文章来源, 新闻链接

-- 确保 stock_analysis schema 存在
CREATE SCHEMA IF NOT EXISTS stock_analysis;

CREATE TABLE IF NOT EXISTS stock_analysis.cn_company_news (
    id BIGSERIAL PRIMARY KEY,
    keyword VARCHAR(32) NOT NULL,          -- 关键词/证券代码 (如 002508)
    news_title TEXT NOT NULL,              -- 新闻标题
    news_content TEXT,                     -- 新闻内容
    published_at TIMESTAMPTZ,              -- 发布时间
    source TEXT,                           -- 文章来源
    news_url TEXT NOT NULL,                -- 新闻链接
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- 唯一约束: 同一条新闻链接只存一条
    UNIQUE(news_url)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_cn_company_news_keyword ON stock_analysis.cn_company_news(keyword);
CREATE INDEX IF NOT EXISTS idx_cn_company_news_published_at ON stock_analysis.cn_company_news(published_at DESC);

-- 注释
COMMENT ON TABLE stock_analysis.cn_company_news IS '中国A股公司新闻（东方财富等来源抓取）';
COMMENT ON COLUMN stock_analysis.cn_company_news.keyword IS '关键词/证券代码';
COMMENT ON COLUMN stock_analysis.cn_company_news.news_title IS '新闻标题';
COMMENT ON COLUMN stock_analysis.cn_company_news.news_content IS '新闻内容';
COMMENT ON COLUMN stock_analysis.cn_company_news.published_at IS '发布时间';
COMMENT ON COLUMN stock_analysis.cn_company_news.source IS '文章来源';
COMMENT ON COLUMN stock_analysis.cn_company_news.news_url IS '新闻链接';

-- 启用 RLS
ALTER TABLE stock_analysis.cn_company_news ENABLE ROW LEVEL SECURITY;

-- 权限（与其它 stock_analysis 表一致）
GRANT USAGE ON SCHEMA stock_analysis TO anon, authenticated, service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON stock_analysis.cn_company_news TO anon, authenticated, service_role;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA stock_analysis TO anon, authenticated, service_role;

-- 允许公开读取
CREATE POLICY "Allow public read access" ON stock_analysis.cn_company_news
    FOR SELECT USING (true);

-- 允许认证写入（如需更严格可后续收紧）
CREATE POLICY "Allow authenticated insert" ON stock_analysis.cn_company_news
    FOR INSERT WITH CHECK (true);

CREATE POLICY "Allow authenticated update" ON stock_analysis.cn_company_news
    FOR UPDATE USING (true);

-- 在 public schema 创建视图以便通过 PostgREST 访问
CREATE OR REPLACE VIEW public.cn_company_news AS
SELECT * FROM stock_analysis.cn_company_news;

-- 允许通过视图 INSERT（INSTEAD OF）并实现 upsert
CREATE OR REPLACE FUNCTION public.cn_company_news_insert()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO stock_analysis.cn_company_news (
        keyword, news_title, news_content, published_at, source, news_url
    ) VALUES (
        NEW.keyword, NEW.news_title, NEW.news_content, NEW.published_at, NEW.source, NEW.news_url
    )
    ON CONFLICT (news_url) DO UPDATE SET
        keyword = EXCLUDED.keyword,
        news_title = EXCLUDED.news_title,
        news_content = EXCLUDED.news_content,
        published_at = EXCLUDED.published_at,
        source = EXCLUDED.source,
        updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

DROP TRIGGER IF EXISTS cn_company_news_insert_trigger ON public.cn_company_news;
CREATE TRIGGER cn_company_news_insert_trigger
    INSTEAD OF INSERT ON public.cn_company_news
    FOR EACH ROW
    EXECUTE FUNCTION public.cn_company_news_insert();

GRANT SELECT, INSERT ON public.cn_company_news TO anon, authenticated, service_role;

