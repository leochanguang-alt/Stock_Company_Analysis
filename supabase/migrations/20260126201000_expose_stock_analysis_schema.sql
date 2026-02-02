-- 将 stock_analysis schema 暴露给 PostgREST API
-- 方法: 授予 anon 和 authenticated 角色访问权限

-- 授予 schema 使用权限
GRANT USAGE ON SCHEMA stock_analysis TO anon, authenticated, service_role;

-- 授予表的访问权限
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA stock_analysis TO anon, authenticated, service_role;

-- 授予序列使用权限 (用于 SERIAL 字段)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA stock_analysis TO anon, authenticated, service_role;

-- 设置默认权限，确保未来创建的表也有相同权限
ALTER DEFAULT PRIVILEGES IN SCHEMA stock_analysis
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO anon, authenticated, service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA stock_analysis
    GRANT USAGE, SELECT ON SEQUENCES TO anon, authenticated, service_role;

-- 注意: 还需要在 Supabase Dashboard -> API Settings 中添加 stock_analysis 到 Exposed schemas
-- 或者通过 SQL 更新 postgrest 配置 (需要超级用户权限)
