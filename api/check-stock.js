import { createClient } from '@supabase/supabase-js';

export default async function handler(req, res) {
  const { symbol } = req.query;
  
  if (!symbol) {
    return res.status(400).json({ valid: false, error: '缺少股票代码' });
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseAnonKey) {
    return res.status(500).json({ valid: false, error: '服务器配置错误: 缺少 Supabase 凭证' });
  }

  const supabase = createClient(supabaseUrl, supabaseAnonKey);
  
  try {
    const { data, error } = await supabase
      .from('company_list')
      .select('symbol, description, exchange')
      .eq('symbol', symbol)
      .limit(1)
      .maybeSingle();
    
    if (error) throw error;

    if (!data) {
      return res.status(404).json({ valid: false, error: '股票代码不存在' });
    }
    
    const isCN = ['SSE', 'SZSE'].includes(data.exchange?.toUpperCase());
    
    return res.status(200).json({ 
      valid: isCN, 
      company: data.description,
      exchange: data.exchange,
      error: isCN ? null : '非A股市场股票'
    });
  } catch (err) {
    console.error('Check stock error:', err);
    return res.status(500).json({ valid: false, error: '查询数据库失败' });
  }
}
