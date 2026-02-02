/**
 * 执行 SQL 迁移 - 创建 cn_sharehold_data 表
 * 使用 Supabase Database 直连
 */

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

// Supabase 数据库连接信息
// 格式: postgresql://postgres.[project-ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres
// 你需要提供数据库密码

// 加载 .env 文件
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const DB_PASSWORD = process.env.SUPABASE_DB_PASSWORD;
const PROJECT_REF = 'fsyxnkzrgozmjyoxcvvh';

if (!DB_PASSWORD) {
    console.error('❌ 请在 .env 文件中设置 SUPABASE_DB_PASSWORD');
    process.exit(1);
}

// Supabase 数据库连接
// 直连格式: postgresql://postgres:[PASSWORD]@[HOST]:5432/postgres
// Host 格式: [PROJECT_REF].supabase.co 或 db.[PROJECT_REF].supabase.co
const connectionString = `postgresql://postgres:${DB_PASSWORD}@${PROJECT_REF}.supabase.co:5432/postgres`;

// 或使用 Session mode (端口 5432)
// const connectionString = `postgresql://postgres.${PROJECT_REF}:${DB_PASSWORD}@aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres`;

async function executeMigration() {
    const pool = new Pool({
        connectionString,
        ssl: { rejectUnauthorized: false }
    });

    try {
        // 读取迁移文件
        const migrationPath = path.join(__dirname, '../supabase/migrations/20260126200000_create_cn_sharehold_data.sql');
        const sql = fs.readFileSync(migrationPath, 'utf8');

        console.log('连接数据库...');
        const client = await pool.connect();
        
        console.log('执行迁移 SQL...');
        await client.query(sql);
        
        console.log('✅ 迁移成功! cn_sharehold_data 表已创建');
        
        // 验证表是否创建成功
        const result = await client.query(`
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'stock_analysis' AND table_name = 'cn_sharehold_data'
        `);
        
        if (result.rows.length > 0) {
            console.log('✅ 验证成功: stock_analysis.cn_sharehold_data 表存在');
        }
        
        client.release();
    } catch (err) {
        console.error('❌ 错误:', err.message);
        if (err.message.includes('password')) {
            console.log('\n请设置数据库密码:');
            console.log('export SUPABASE_DB_PASSWORD="your_password"');
            console.log('\n或在 Supabase Dashboard 重置密码:');
            console.log('https://supabase.com/dashboard/project/fsyxnkzrgozmjyoxcvvh/settings/database');
        }
    } finally {
        await pool.end();
    }
}

executeMigration();
