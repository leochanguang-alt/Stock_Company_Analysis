/**
 * 导入股东人数及集中度数据到 Supabase
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    console.error('❌ 请在 .env 文件中设置 SUPABASE_URL 和 SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// CSV 列名映射到数据库字段
const columnMapping = {
    '证券代码': 'symbol',
    '证券简称': 'name',
    '变动日期': 'report_date',
    '本期股东人数': 'current_holder_count',
    '上期股东人数': 'previous_holder_count',
    '股东人数增幅': 'holder_count_change_pct',
    '本期人均持股数量': 'current_avg_shares',
    '上期人均持股数量': 'previous_avg_shares',
    '人均持股数量增幅': 'avg_shares_change_pct',
    '报告期': 'report_period'
};

function parseCSV(csvContent) {
    const lines = csvContent.trim().split('\n');
    const headers = lines[0].split(',');
    const rows = [];

    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        const values = line.split(',');
        const row = {};
        
        headers.forEach((header, index) => {
            const dbField = columnMapping[header];
            if (dbField) {
                let value = values[index];
                
                // 处理数值类型
                if (['current_holder_count', 'previous_holder_count', 'holder_count_change_pct',
                     'current_avg_shares', 'previous_avg_shares', 'avg_shares_change_pct'].includes(dbField)) {
                    value = value ? parseFloat(value) : null;
                }
                
                row[dbField] = value;
            }
        });
        
        rows.push(row);
    }
    
    return rows;
}

async function importData() {
    const csvPath = path.join(__dirname, '../outputs/002508_holder_count_concentration_10y.csv');
    
    console.log('📖 读取 CSV 文件...');
    const csvContent = fs.readFileSync(csvPath, 'utf-8');
    const rows = parseCSV(csvContent);
    
    console.log(`📊 解析到 ${rows.length} 条记录`);
    
    console.log('⬆️ 导入数据到 Supabase...');
    
    // 使用 insert (视图会通过触发器处理 upsert)
    const { data, error } = await supabase
        .from('cn_sharehold_data')
        .insert(rows)
        .select();
    
    if (error) {
        console.error('❌ 导入失败:', error.message);
        console.error('详细错误:', error);
        process.exit(1);
    }
    
    console.log(`✅ 成功导入 ${data ? data.length : rows.length} 条记录`);
    
    // 验证数据
    const { data: verifyData, error: verifyError } = await supabase
        .from('cn_sharehold_data')
        .select('*')
        .eq('symbol', '002508')
        .order('report_date', { ascending: false })
        .limit(5);
    
    if (verifyError) {
        console.error('验证失败:', verifyError.message);
    } else {
        console.log('\n📋 最新 5 条记录:');
        verifyData.forEach(row => {
            console.log(`  ${row.report_date} | 股东人数: ${row.current_holder_count} | 人均持股: ${row.current_avg_shares}`);
        });
    }
}

importData().catch(console.error);
