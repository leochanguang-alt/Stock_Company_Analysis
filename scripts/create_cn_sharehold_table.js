/**
 * 通过 Supabase 创建 cn_sharehold_data 表
 * 使用 pg 直连或 supabase-js
 */

const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = 'https://fsyxnkzrgozmjyoxcvvh.supabase.co';
const SUPABASE_SERVICE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZzeXhua3pyZ296bWp5b3hjdnZoIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODExODA1MSwiZXhwIjoyMDgzNjk0MDUxfQ.k5c89GZOeM3w5ZhrhUc_7y4A9LYz-FsimHxwXcR2oyU';

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

async function createTable() {
    // Supabase JS client 不支持直接执行 DDL
    // 需要通过 Management API 或 psql
    
    // 检查表是否存在
    const { data, error } = await supabase
        .from('cn_sharehold_data')
        .select('*')
        .limit(1);
    
    if (error && error.code === '42P01') {
        console.log('表不存在，需要通过 SQL Editor 创建');
        console.log('请访问: https://supabase.com/dashboard/project/fsyxnkzrgozmjyoxcvvh/sql/new');
        console.log('并执行 supabase/migrations/20260126200000_create_cn_sharehold_data.sql 中的 SQL');
    } else if (error) {
        console.log('错误:', error.message);
    } else {
        console.log('表已存在');
    }
}

createTable();
