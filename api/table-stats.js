import { createClient } from '@supabase/supabase-js';

export default async function handler(req, res) {
  const supabaseUrl = process.env.SUPABASE_URL;
  // 服务端优先使用 service role（避免某些表的 RLS 造成“统计不准”）
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: '服务器配置错误' });
  }

  const supabase = createClient(supabaseUrl, supabaseKey);

  const defaultTables = [
    'cn_balance_sheet_10y',
    'cn_income_statement_10y',
    'cn_cash_flow_10y',
    'cn_mkt_cap_10y',
    'cn_top10_shareholders_10y',
    'cn_holder_count_concentration_10y',
  ];

  const rawTables = req.query?.tables;
  const tables =
    typeof rawTables === 'string'
      ? rawTables
          .split(',')
          .map(s => s.trim())
          .filter(Boolean)
      : Array.isArray(rawTables)
        ? rawTables.map(s => String(s).trim()).filter(Boolean)
        : defaultTables;

  const includeCompanies = String(req.query?.include_companies || '').toLowerCase() === '1';

  // 兼容常见拼写/命名差异：先尝试请求的表名，再按候选表名回退
  const fallbacksByRequested = {
    cn_top10_shareholder_10y: ['cn_top10_shareholders_10y'],
    cn_holder_court_concentration_10y: ['cn_holder_count_concentration_10y'],
    cn_balance_sheet_1y: ['cn_balance_sheet_10y'],
  };

  const isMissingTableError = (err) => {
    const msg = String(err?.message || err || '');
    const code = String(err?.code || '');
    // Postgres / PostgREST 常见缺表错误码或信息
    return (
      code === '42P01' ||
      msg.toLowerCase().includes('does not exist') ||
      msg.toLowerCase().includes('relation') ||
      msg.toLowerCase().includes('not found') ||
      msg.toLowerCase().includes('could not find the') ||
      msg.toLowerCase().includes('404')
    );
  };

  const results = {};

  const getTableStats = async (tableName) => {
    // 获取总记录数
    const { count, error: countError } = await supabase
      .from(tableName)
      .select('*', { count: 'exact', head: true });

    if (countError) throw countError;

    // 获取不同 symbol 数量 - 通过查询所有 symbol 并去重
    const allSymbols = new Set();
    let offset = 0;
    const batchSize = 1000;

    while (true) {
      const { data, error } = await supabase
        .from(tableName)
        .select('symbol')
        .range(offset, offset + batchSize - 1);

      if (error) throw error;
      if (!data || data.length === 0) break;

      data.forEach(row => {
        if (row.symbol) allSymbols.add(row.symbol);
      });

      if (data.length < batchSize) break;
      offset += batchSize;
    }

    return {
      total_records: count,
      unique_companies: allSymbols.size,
      companies: includeCompanies ? Array.from(allSymbols).sort() : undefined,
    };
  };

  for (const requested of tables) {
    try {
      const primaryStats = await getTableStats(requested);
      results[requested] = { resolved_table: requested, ...primaryStats };
    } catch (err) {
      // 如果是缺表错误，则尝试回退表名
      if (isMissingTableError(err)) {
        const fallbacks = fallbacksByRequested[requested] || [];
        for (const candidate of fallbacks) {
          try {
            const stats = await getTableStats(candidate);
            results[requested] = {
              resolved_table: candidate,
              warning: `未找到表 ${requested}，已回退查询 ${candidate}`,
              ...stats,
            };
            break;
          } catch (fallbackErr) {
            // 继续尝试下一个候选
          }
        }
      }

      if (!results[requested]) {
        results[requested] = { error: err?.message || String(err) };
      }
    }
  }

  return res.status(200).json(results);
}
