
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
    console.error('缺少 SUPABASE_URL 或 SUPABASE_SERVICE_ROLE_KEY');
    process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

async function checkData() {
    const symbol = '000001';
    const market = 'SSE';

    console.log(`Checking data for ${market}:${symbol}...`);

    // 1. Check count
    const { count, error: countError } = await supabase
        .from('market_index_history')
        .select('*', { count: 'exact', head: true })
        .eq('symbol', symbol)
        .eq('market', market);

    if (countError) {
        console.error('Error getting count:', countError);
        return;
    }
    console.log(`Total records: ${count}`);

    // 2. Check latest records (to ensure we have 2024/2025 data)
    const { data: latestData, error: latestError } = await supabase
        .from('market_index_history')
        .select('*')
        .eq('symbol', symbol)
        .eq('market', market)
        .order('date', { ascending: false })
        .limit(5);

    if (latestError) {
        console.error('Error getting latest data:', latestError);
        return;
    }

    console.log('Latest 5 records:');
    console.table(latestData);

    // 3. Check earliest records
    const { data: earliestData, error: earliestError } = await supabase
        .from('market_index_history')
        .select('*')
        .eq('symbol', symbol)
        .eq('market', market)
        .order('date', { ascending: true })
        .limit(5);
        
    if (earliestError) {
        console.error('Error getting earliest data:', earliestError);
        return;
    }
    
    console.log('Earliest 5 records:');
    console.table(earliestData);
}

checkData().catch(console.error);
