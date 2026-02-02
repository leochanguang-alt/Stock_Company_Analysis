/**
 * 使用 PostgreSQL 直连导入股东数据
 */

require('dotenv').config({ path: require('path').join(__dirname, '../.env') });
const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const DB_PASSWORD = process.env.SUPABASE_DB_PASSWORD;
const PROJECT_REF = 'fsyxnkzrgozmjyoxcvvh';

if (!DB_PASSWORD) {
    console.error('❌ 请在 .env 文件中设置 SUPABASE_DB_PASSWORD');
    process.exit(1);
}

// Supabase Pooler 连接 (Session mode)
const connectionString = `postgresql://postgres.${PROJECT_REF}:${DB_PASSWORD}@aws-0-ap-northeast-1.pooler.supabase.com:5432/postgres`;

const pool = new Pool({
    connectionString,
    ssl: { rejectUnauthorized: false }
});

// CSV 列名映射
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
    
    const client = await pool.connect();
    
    try {
        console.log('⬆️ 导入数据到 Supabase...');
        
        let insertedCount = 0;
        
        for (const row of rows) {
            const query = `
                INSERT INTO stock_analysis.cn_sharehold_data 
                (symbol, name, report_date, current_holder_count, previous_holder_count, 
                 holder_count_change_pct, current_avg_shares, previous_avg_shares, 
                 avg_shares_change_pct, report_period)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (symbol, report_date) 
                DO UPDATE SET
                    name = EXCLUDED.name,
                    current_holder_count = EXCLUDED.current_holder_count,
                    previous_holder_count = EXCLUDED.previous_holder_count,
                    holder_count_change_pct = EXCLUDED.holder_count_change_pct,
                    current_avg_shares = EXCLUDED.current_avg_shares,
                    previous_avg_shares = EXCLUDED.previous_avg_shares,
                    avg_shares_change_pct = EXCLUDED.avg_shares_change_pct,
                    report_period = EXCLUDED.report_period,
                    updated_at = NOW()
            `;
            
            await client.query(query, [
                row.symbol,
                row.name,
                row.report_date,
                row.current_holder_count,
                row.previous_holder_count,
                row.holder_count_change_pct,
                row.current_avg_shares,
                row.previous_avg_shares,
                row.avg_shares_change_pct,
                row.report_period
            ]);
            
            insertedCount++;
        }
        
        console.log(`✅ 成功导入 ${insertedCount} 条记录`);
        
        // 验证数据
        const result = await client.query(`
            SELECT symbol, name, report_date, current_holder_count, current_avg_shares 
            FROM stock_analysis.cn_sharehold_data 
            WHERE symbol = '002508' 
            ORDER BY report_date DESC 
            LIMIT 5
        `);
        
        console.log('\n📋 最新 5 条记录:');
        result.rows.forEach(row => {
            console.log(`  ${row.report_date.toISOString().split('T')[0]} | 股东人数: ${row.current_holder_count} | 人均持股: ${row.current_avg_shares}`);
        });
        
    } finally {
        client.release();
        await pool.end();
    }
}

importData().catch(err => {
    console.error('❌ 错误:', err.message);
    process.exit(1);
});
