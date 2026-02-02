/**
 * 导入市场指数历史数据到 Supabase
 * 用法: node scripts/import_market_index_history.js
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error('缺少 SUPABASE_URL 或 SUPABASE_SERVICE_ROLE_KEY 环境变量');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// 配置
const CSV_PATH = path.join(__dirname, '../outputs/000001_sse_index_daily.csv');
const SYMBOL = '000001';
const MARKET = 'SSE';
const BATCH_SIZE = 500;

function parseCSV(content) {
    const lines = content.trim().split('\n');
    const headers = lines[0].split(',');
    const rows = [];
    
    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',');
        if (values.length !== headers.length) continue;
        
        const row = {};
        headers.forEach((h, idx) => {
            row[h.trim()] = values[idx].trim();
        });
        rows.push(row);
    }
    return rows;
}

async function main() {
    console.log('读取 CSV 文件:', CSV_PATH);
    const content = fs.readFileSync(CSV_PATH, 'utf-8');
    const rows = parseCSV(content);
    console.log(`解析到 ${rows.length} 条记录`);
    
    // 转换数据格式
    const records = rows.map(row => ({
        symbol: SYMBOL,
        market: MARKET,
        date: row.date,
        open: parseFloat(row.open) || null,
        high: parseFloat(row.high) || null,
        low: parseFloat(row.low) || null,
        close: parseFloat(row.close) || null,
        volume: parseInt(row.volume) || null
    }));
    
    console.log(`开始导入，批次大小: ${BATCH_SIZE}`);
    
    let inserted = 0;
    let errors = 0;
    
    for (let i = 0; i < records.length; i += BATCH_SIZE) {
        const batch = records.slice(i, i + BATCH_SIZE);
        const batchNum = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(records.length / BATCH_SIZE);
        
        try {
            const { data, error } = await supabase
                .from('market_index_history')
                .insert(batch);
            
            if (error) {
                console.error(`批次 ${batchNum}/${totalBatches} 失败:`, error.message);
                errors += batch.length;
            } else {
                inserted += batch.length;
                console.log(`批次 ${batchNum}/${totalBatches} 完成 (${inserted}/${records.length})`);
            }
        } catch (e) {
            console.error(`批次 ${batchNum} 异常:`, e.message);
            errors += batch.length;
        }
    }
    
    console.log('');
    console.log('=== 导入完成 ===');
    console.log(`成功: ${inserted} 条`);
    console.log(`失败: ${errors} 条`);
    console.log(`Symbol: ${SYMBOL}`);
    console.log(`Market: ${MARKET}`);
}

main().catch(console.error);
