/**
 * 导入前十大股东数据到 Supabase
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
// 名次,股东名称,股份类型,持股数,占总股本持股比例,增减,变动比率,报告期,股票代码
const columnMapping = {
    '名次': 'rank',
    '股东名称': 'shareholder_name',
    '股份类型': 'share_type',
    '持股数': 'shares_held',
    '占总股本持股比例': 'holding_ratio',
    '增减': 'change_amount',
    '变动比率': 'change_ratio',
    '报告期': 'report_date',
    '股票代码': 'symbol'
};

function parseCSV(csvContent) {
    const lines = csvContent.trim().split('\n');
    const headers = lines[0].split(',');
    const rows = [];

    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        
        // 处理包含逗号的字段（如 "流通A股,限售流通A股"）
        const values = [];
        let inQuotes = false;
        let currentValue = '';
        
        for (let j = 0; j < line.length; j++) {
            const char = line[j];
            if (char === '"') {
                inQuotes = !inQuotes;
            } else if (char === ',' && !inQuotes) {
                values.push(currentValue);
                currentValue = '';
            } else {
                currentValue += char;
            }
        }
        values.push(currentValue);
        
        const row = {};
        
        headers.forEach((header, index) => {
            const dbField = columnMapping[header];
            if (dbField && values[index] !== undefined) {
                let value = values[index].trim();
                
                // 处理数值类型
                if (dbField === 'rank') {
                    value = parseInt(value) || null;
                } else if (dbField === 'shares_held') {
                    value = parseInt(value) || null;
                } else if (dbField === 'holding_ratio') {
                    value = parseFloat(value) || null;
                } else if (dbField === 'change_ratio') {
                    // 变动比率可能为空
                    value = value ? parseFloat(value) : null;
                } else if (dbField === 'change_amount') {
                    // 增减可能是 "新进"、"不变" 或数字
                    value = value || null;
                }
                
                row[dbField] = value;
            }
        });
        
        // 只添加有效行
        if (row.symbol && row.report_date && row.rank) {
            rows.push(row);
        }
    }
    
    return rows;
}

async function importData() {
    const csvPath = path.join(__dirname, '../outputs/002508_top10_shareholders_10y.csv');
    
    console.log('📖 读取 CSV 文件...');
    const csvContent = fs.readFileSync(csvPath, 'utf-8');
    const rows = parseCSV(csvContent);
    
    console.log(`📊 解析到 ${rows.length} 条记录`);
    
    console.log('⬆️ 导入数据到 Supabase...');
    
    // 分批导入，每批 50 条
    const batchSize = 50;
    let totalInserted = 0;
    
    for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        
        const { data, error } = await supabase
            .from('cn_top10_sharehold')
            .insert(batch)
            .select();
        
        if (error) {
            console.error(`❌ 批次 ${Math.floor(i/batchSize) + 1} 导入失败:`, error.message);
            // 继续下一批
        } else {
            totalInserted += batch.length;
            process.stdout.write(`\r  已导入: ${totalInserted}/${rows.length}`);
        }
    }
    
    console.log(`\n✅ 成功导入 ${totalInserted} 条记录`);
    
    // 验证数据
    const { data: verifyData, error: verifyError } = await supabase
        .from('cn_top10_sharehold')
        .select('*')
        .eq('symbol', 'SZ002508')
        .order('report_date', { ascending: false })
        .order('rank', { ascending: true })
        .limit(10);
    
    if (verifyError) {
        console.error('验证失败:', verifyError.message);
    } else {
        console.log('\n📋 最新一期前10大股东:');
        const latestDate = verifyData[0]?.report_date;
        verifyData
            .filter(r => r.report_date === latestDate)
            .forEach(row => {
                console.log(`  ${row.rank}. ${row.shareholder_name.substring(0, 20)}... | ${row.holding_ratio}%`);
            });
    }
}

importData().catch(console.error);
