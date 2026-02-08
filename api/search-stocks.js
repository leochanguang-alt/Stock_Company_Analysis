import { createClient } from '@supabase/supabase-js';

export default async function handler(req, res) {
  const { q } = req.query;
  
  if (!q || q.length < 1) {
    return res.status(200).json({ results: [] });
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseAnonKey) {
    return res.status(500).json({ results: [], error: '服务器配置错误' });
  }

  const supabase = createClient(supabaseUrl, supabaseAnonKey);
  
  try {
    let data, error;
    
    // 如果是完整的6位数字代码，精确匹配
    if (/^\d{6}$/.test(q)) {
      const result = await supabase
        .from('company_list')
        .select('symbol, description, exchange')
        .eq('symbol', q)
        .limit(1);
      data = result.data;
      error = result.error;
    } else {
      // 否则模糊搜索股票代码或公司名称
      const result = await supabase
        .from('company_list')
        .select('symbol, description, exchange')
        .or(`symbol.ilike.%${q}%,description.ilike.%${q}%`)
        .order('symbol')
        .limit(20);
      data = result.data;
      error = result.error;
    }
    
    if (error) throw error;

    // 格式化结果为 AKShare 标准格式
    const results = (data || []).map(item => {
      let suffix = '';
      const exchange = (item.exchange || '').toUpperCase();
      if (exchange === 'SSE') suffix = '.SH';
      else if (exchange === 'SZSE') suffix = '.SZ';
      else if (exchange === 'HKEX') suffix = '.HK';
      else if (exchange === 'NYSE' || exchange === 'NASDAQ') suffix = '.US';
      
      return {
        symbol: item.symbol,
        code: `${item.symbol}${suffix}`,
        name: item.description,
        exchange: item.exchange,
        // 标记是否为中国A股
        isCN: ['SSE', 'SZSE'].includes(exchange)
      };
    });

    return res.status(200).json({ results });
  } catch (err) {
    console.error('Search stocks error:', err);
    return res.status(500).json({ results: [], error: '搜索失败' });
  }
}
