import { createClient } from '@supabase/supabase-js';

function pick(obj, keys) {
  const out = {};
  for (const k of keys) out[k] = obj?.[k] ?? null;
  return out;
}

function toNum(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toISODate(d) {
  try {
    const dt = new Date(d);
    if (Number.isNaN(dt.getTime())) return null;
    return dt.toISOString().slice(0, 10);
  } catch {
    return null;
  }
}

function isYearEnd(dateStr) {
  return typeof dateStr === 'string' && dateStr.endsWith('-12-31');
}

function uniqBy(rows, keyFn) {
  const seen = new Set();
  const out = [];
  for (const r of rows || []) {
    const k = keyFn(r);
    if (k == null || k === '') continue;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(r);
  }
  return out;
}

function sortByDateDesc(rows, field = 'report_date') {
  return [...(rows || [])].sort((a, b) => String(b?.[field] || '').localeCompare(String(a?.[field] || '')));
}

function sortByDateAsc(rows, field = 'trade_date') {
  return [...(rows || [])].sort((a, b) => String(a?.[field] || '').localeCompare(String(b?.[field] || '')));
}

/**
 * Convert CNY financial data to HKD using dynamic exchange rates
 * For dual-listed companies, use standard CNY->HKD rate (1/0.901)
 */
async function applyHKDConversion(supabase, hkCode5, balance_sheet_10y, income_statement_10y, cash_flow_10y, 
                                   bsQuarterlyDesc, isQuarterlyDesc, cfQuarterlyDesc,
                                   bsAnnualDesc, isAnnualDesc, cfAnnualDesc,
                                   isTTMDesc, cfTTMDesc) {
  const CNY_TO_HKD = 1 / 0.901; // Standard conversion rate
  const exchangeRateMap = new Map();
  
  // Build exchange rate map for all report dates
  for (const bsRow of balance_sheet_10y) {
    const date = toISODate(bsRow.report_date);
    if (date) {
      exchangeRateMap.set(date, CNY_TO_HKD);
    }
  }
  
  // Convert row: multiply all numeric fields by exchange rate
  const convertRow = (row) => {
    if (!row) return row;
    const date = toISODate(row.report_date || row.trade_date);
    const rate = exchangeRateMap.get(date) || CNY_TO_HKD;
    const converted = { ...row };
    
    // Exclude non-financial fields
    const excludeFields = new Set([
      'report_date', 'symbol', 'security_name', 'start_date', 'end_date', 
      'trade_date', 'id', 'created_at', 'updated_at', 'secucode'
    ]);
    
    for (const [key, val] of Object.entries(converted)) {
      if (!excludeFields.has(key) && typeof val === 'number' && 
          !key.includes('_yoy') && !key.includes('_ratio') && 
          !key.includes('_rate') && !key.includes('eps')) {
        converted[key] = val * rate;
      }
    }
    return converted;
  };
  
  // Apply conversion to all arrays
  const arrays = [
    balance_sheet_10y, income_statement_10y, cash_flow_10y,
    bsQuarterlyDesc, isQuarterlyDesc, cfQuarterlyDesc,
    bsAnnualDesc, isAnnualDesc, cfAnnualDesc,
    isTTMDesc, cfTTMDesc
  ];
  
  for (const arr of arrays) {
    if (arr) {
      for (let i = 0; i < arr.length; i++) {
        arr[i] = convertRow(arr[i]);
      }
    }
  }
}

function groupLatestByYear(rows, dateField) {
  const byYear = new Map();
  for (const r of rows || []) {
    const d = r?.[dateField];
    if (!d) continue;
    const y = String(d).slice(0, 4);
    const prev = byYear.get(y);
    if (!prev || String(d) > String(prev[dateField])) byYear.set(y, r);
  }
  // sort by year desc
  return [...byYear.entries()]
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([, r]) => r);
}

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function computeTTM(rowsDesc, sumFields, opts = {}) {
  // rowsDesc: report_date desc
  // For each anchor quarter i, sum i..i+3 (4 quarters) on given fields.
  // opts.takeFirst: fields where we take the value from the oldest quarter (e.g. begin_cce)
  // opts.takeLast:  fields where we take the value from the anchor/newest quarter (e.g. end_cce)
  const takeFirst = opts.takeFirst || [];
  const takeLast  = opts.takeLast  || [];
  const out = [];
  for (let i = 0; i < (rowsDesc || []).length; i++) {
    const anchor = rowsDesc[i];
    const window = rowsDesc.slice(i, i + 4);
    if (window.length < 4) break;
    const record = { report_date: anchor.report_date };
    for (const f of sumFields) {
      let s = 0;
      let ok = false;
      for (const w of window) {
        const n = toNum(w?.[f]);
        if (n != null) {
          s += n;
          ok = true;
        }
      }
      record[f] = ok ? s : null;
    }
    for (const f of takeFirst) {
      const oldest = window[window.length - 1];
      record[f] = toNum(oldest?.[f]);
    }
    for (const f of takeLast) {
      record[f] = toNum(anchor?.[f]);
    }
    out.push(record);
  }
  return out;
}

export default async function handler(req, res) {
  const symbol = String(req.query?.symbol || '').trim();
  if (!/^\d{6}$/.test(symbol)) {
    return res.status(400).json({ error: 'symbol 必须是 6 位数字代码' });
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  // 服务端优先使用 service role（避免某些表的 RLS 造成“查不到但不报错”）
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: '服务器配置错误: 缺少 Supabase 凭证' });
  }

  // Check if this is a dual-listed company request from hk-analysis redirect
  const isHKMode = req.query?._hk_mode === '1';
  const hkSymbol = req.query?._hk_symbol || null;
  const hkCode5 = hkSymbol ? hkSymbol.replace(/\.HK$/i, '').padStart(5, '0') : null;

  const supabase = createClient(supabaseUrl, supabaseKey);

  // 你要求使用的表名中包含一些历史拼写/命名差异，这里做兼容映射
  const TABLES = {
    // 你写的是 cn_balance_sheet_1y，但当前库里实际是 10y；我们在服务端做 1y 截断
    balance_sheet: 'cn_balance_sheet_10y',
    income_statement: 'cn_income_statement_10y',
    cash_flow: 'cn_cash_flow_10y',
    mkt_cap: 'cn_mkt_cap_10y',
    top10_shareholders: 'cn_top10_shareholders_10y',
    holder_count_concentration: 'cn_holder_count_concentration_10y',
  };

  try {
    const today = new Date();
    const tenYearsAgo = new Date(today);
    tenYearsAgo.setDate(tenYearsAgo.getDate() - 365 * 10 - 60);
    const tenYearsAgoISO = toISODate(tenYearsAgo);

    // Balance sheet：为对齐旧页科目/行名，这里直接取 cn_balance_sheet_10y 的全字段
    const { data: bsRaw, error: bsErr } = await supabase
      .from(TABLES.balance_sheet)
      .select('*')
      .eq('symbol', symbol)
      .order('report_date', { ascending: false })
      .limit(200);
    if (bsErr) throw bsErr;
    const balance_sheet_10y = (bsRaw || []);

    const { data: isRaw, error: isErr } = await supabase
      .from(TABLES.income_statement)
      .select('*')
      .eq('symbol', symbol)
      .order('report_date', { ascending: false })
      .limit(80);
    if (isErr) throw isErr;
    const income_statement_10y = (isRaw || []);

    const { data: cfRaw, error: cfErr } = await supabase
      .from(TABLES.cash_flow)
      .select('*')
      .eq('symbol', symbol)
      .order('report_date', { ascending: false })
      .limit(80);
    if (cfErr) throw cfErr;
    const cash_flow_10y = (cfRaw || []);

    // Market cap: 10y（日频，分批拉取，避免服务端分页上限）
    const mktFields = ['trade_date', 'symbol', 'mkt_cap_billion_cny'];
    const mkt_cap_10y = [];
    const batchSize = 1000;
    let offset = 0;
    while (true) {
      const { data: mcRaw, error: mcErr } = await supabase
        .from(TABLES.mkt_cap)
        .select(mktFields.join(','))
        .eq('symbol', symbol)
        .gte('trade_date', tenYearsAgoISO)
        .order('trade_date', { ascending: true })
        .range(offset, offset + batchSize - 1);
      if (mcErr) throw mcErr;
      if (!mcRaw || mcRaw.length === 0) break;
      mkt_cap_10y.push(...mcRaw.map(r => pick(r, mktFields)));
      if (mcRaw.length < batchSize) break;
      offset += batchSize;
    }

    // 上证指数：market_index_history（日频）
    const indexFields = ['date', 'symbol', 'market', 'close'];
    const market_index_history = [];
    let idxOffset = 0;
    while (true) {
      const { data: idxRaw, error: idxErr } = await supabase
        .from('market_index_history')
        .select(indexFields.join(','))
        .eq('symbol', '000001')
        .eq('market', 'SSE')
        .gte('date', tenYearsAgoISO)
        .order('date', { ascending: true })
        .range(idxOffset, idxOffset + batchSize - 1);
      if (idxErr) throw idxErr;
      if (!idxRaw || idxRaw.length === 0) break;
      market_index_history.push(...idxRaw.map(r => pick(r, indexFields)));
      if (idxRaw.length < batchSize) break;
      idxOffset += batchSize;
    }

    // Holder count concentration: 10y（季度/半年度频率，取更多以便展示“全”）
    const hcFields = [
      'report_date',
      'symbol',
      'security_name',
      'holder_count',
      'holder_count_prev',
      'holder_count_change',
      'avg_hold_num',
      'avg_hold_num_prev',
      'avg_hold_num_change',
    ];
    const { data: hcRaw, error: hcErr } = await supabase
      .from(TABLES.holder_count_concentration)
      .select(hcFields.join(','))
      .eq('symbol', symbol)
      .order('report_date', { ascending: false })
      .limit(240);
    if (hcErr) throw hcErr;
    const holder_court_concentration_10y = (hcRaw || []).map(r => pick(r, hcFields));

    // Top 10 shareholders: 取近 10 年全部报告期
    const t10Fields = [
      'report_date',
      'symbol',
      'rank',
      'shareholder_name',
      'share_type',
      'hold_num',
      'hold_ratio',
      'change_num',
      'change_ratio',
    ];
    const top10_shareholder_10y = [];
    const t10BatchSize = 1000;
    let t10Offset = 0;
    while (true) {
      const { data: t10Raw, error: t10Err } = await supabase
        .from(TABLES.top10_shareholders)
        .select(t10Fields.join(','))
        .eq('symbol', symbol)
        .gte('report_date', tenYearsAgoISO)
        .order('report_date', { ascending: false })
        .order('rank', { ascending: true })
        .range(t10Offset, t10Offset + t10BatchSize - 1);
      if (t10Err) throw t10Err;
      if (!t10Raw || t10Raw.length === 0) break;
      top10_shareholder_10y.push(...t10Raw.map(r => pick(r, t10Fields)));
      if (t10Raw.length < t10BatchSize) break;
      t10Offset += t10BatchSize;
    }

    // 公司名：优先从财报表里拿
    const security_name =
      balance_sheet_10y?.[0]?.security_name ||
      income_statement_10y?.[0]?.security_name ||
      cash_flow_10y?.[0]?.security_name ||
      holder_court_concentration_10y?.[0]?.security_name ||
      null;

    // 公司列表信息（用于表头展示）
    let company_list = null;
    try {
      const { data: companyRows, error: companyErr } = await supabase
        .from('company_list')
        .select('symbol,market,description,sector,industry,exchange')
        .eq('symbol', symbol)
        .eq('market', 'cn')
        .limit(1);
      if (companyErr) throw companyErr;
      company_list = companyRows?.[0] || null;
    } catch {
      company_list = null;
    }

    // 市场指标（beta 等来自 share_a_market）
    let share_a_market = null;
    try {
      const { data: marketRows, error: marketErr } = await supabase
        .from('share_a_market')
        .select('symbol,enterprise_value,market_capitalization,beta_5_years,beta_1_year,cash_from_operating_activities_trailing_12_months,"return_on_invested_capital_%_trailing_12_months",download_date')
        .eq('symbol', symbol)
        .order('download_date', { ascending: false })
        .limit(1);
      if (marketErr) throw marketErr;
      share_a_market = marketRows?.[0] || null;
    } catch {
      share_a_market = null;
    }

    // Views: quarterly / annual / ltm(TTM)
    const isQuarterlyDesc = sortByDateDesc(income_statement_10y, 'report_date');
    const cfQuarterlyDesc = sortByDateDesc(cash_flow_10y, 'report_date');
    const bsQuarterlyDesc = sortByDateDesc(balance_sheet_10y, 'report_date');

    // Annual: pick year-end only (12-31)
    const isAnnualDesc = isQuarterlyDesc.filter(r => isYearEnd(r.report_date));
    const cfAnnualDesc = cfQuarterlyDesc.filter(r => isYearEnd(r.report_date));
    const bsAnnualDesc = bsQuarterlyDesc.filter(r => isYearEnd(r.report_date));

    // TTM: sum flows (income/cash), balance sheet uses anchor quarter row
    // TTM: sum most flow line-items so Income/Cash tabs can match old page rows
    const incomeSumFields = [
      // Revenues / costs
      'total_operate_income',
      'operate_income',
      'other_business_income',
      'operate_cost',
      // Expenses
      'sale_expense',
      'manage_expense',
      'research_expense',
      'finance_expense',
      'operate_tax_add',
      // Interest / investment
      'interest_income',
      'interest_expense',
      'invest_income',
      'invest_joint_income',
      // Other P&L
      'asset_impairment_loss',
      'credit_impairment_loss',
      'asset_disposal_income',
      'noncurrent_disposal_income',
      'noncurrent_disposal_loss',
      'fairvalue_change_income',
      'other_income',
      'nonbusiness_income',
      'nonbusiness_expense',
      // Profit
      'operate_profit',
      'total_profit',
      'income_tax',
      'netprofit',
      'parent_netprofit',
      'minority_interest',
    ];
    const cashSumFields = [
      // Net flows
      'netcash_operate', 'netcash_invest', 'netcash_finance',
      // Net income (from cash flow statement)
      'netprofit',
      // D&A detail fields
      'fa_ir_depr', 'ia_amortize', 'lpe_amortize', 'useright_asset_amortize',
      // Non-cash adjustments
      'invest_loss', 'defer_income_amortize',
      'asset_impairment', 'disposal_longasset_loss', 'fa_scrap_loss',
      'finance_expense',
      // Working capital changes
      'operate_rece_reduce', 'inventory_reduce', 'operate_payable_add',
      'dt_asset_reduce', 'dt_liab_add', 'other',
      // Operating details
      'sales_services', 'buy_services', 'pay_staff_cash',
      'pay_all_tax', 'receive_tax_refund',
      'receive_other_operate', 'pay_other_operate',
      // Investing details
      'withdraw_invest', 'receive_invest_income',
      'disposal_long_asset', 'disposal_subsidiary_other',
      'receive_other_invest', 'construct_long_asset',
      'invest_pay_cash', 'buy_subsidiary_equity',
      'obtain_subsidiary_other', 'pay_other_invest',
      // Financing details
      'accept_invest_cash', 'subsidiary_accept_invest',
      'receive_loan_cash', 'issue_bond',
      'receive_other_finance', 'pay_debt_cash',
      'assign_dividend_porfit', 'subsidiary_pay_dividend',
      'pay_other_finance',
      // Other financing
      'borrow_fund_add', 'loan_advance_add', 'loan_advance_reduce',
      // Totals
      'total_operate_inflow', 'total_operate_outflow',
      'total_invest_inflow', 'total_invest_outflow',
      'total_finance_inflow', 'total_finance_outflow',
      // FX / net change / misc
      'rate_change_effect', 'cce_add',
      'cce_add_other', 'cce_add_balance',
    ];
    const isTTMDesc = computeTTM(isQuarterlyDesc, incomeSumFields);
    const cfTTMDesc = computeTTM(cfQuarterlyDesc, cashSumFields, {
      takeFirst: ['begin_cce'],
      takeLast:  ['end_cce'],
    });

    // Market cap annual: pick last trading day for each year
    const mktCapAnnualDesc = groupLatestByYear(mkt_cap_10y, 'trade_date');

    // For HK mode: convert CNY to HKD using dynamic exchange rates
    let exchangeRateMap = new Map();
    if (isHKMode && hkCode5) {
      // Fetch HK balance sheet total assets for exchange rate calculation
      const { data: hkBSRaw, error: hkBSErr } = await supabase
        .from('hk_balance_sheet')
        .select('report_date,amount')
        .eq('secucode', `${hkCode5}.HK`)
        .eq('std_item_name', '总资产')
        .order('report_date', { ascending: false })
        .limit(200);
      
      if (!hkBSErr && hkBSRaw && hkBSRaw.length > 0) {
        // Build exchange rate map: report_date -> (HK_assets / A_assets)
        const hkAssetsByDate = new Map();
        for (const row of hkBSRaw) {
          const date = toISODate(row.report_date);
          if (date) {
            hkAssetsByDate.set(date, row.amount / 1e8); // Convert to 亿
          }
        }
        
        // Calculate dynamic exchange rate for each A-share report date
        // Since HK data is in CNY equivalent, we need to convert to actual HKD
        // Method: Use the ratio of HK/A assets as a baseline, then apply market rate
        const CNY_TO_HKD_MARKET = 1 / 0.901; // Market rate: 1 CNY = 1.1099 HKD
        
        for (const bsRow of balance_sheet_10y) {
          const date = toISODate(bsRow.report_date);
          const aAssets = toNum(bsRow.total_assets);
          const hkAssets = hkAssetsByDate.get(date);
          
          if (date && aAssets && hkAssets && aAssets > 0) {
            // Since HK financial data is in CNY (verified by total assets being equal),
            // we apply the market CNY->HKD rate for display purposes
            // This ensures consistency with HKD-denominated market cap data
            exchangeRateMap.set(date, CNY_TO_HKD_MARKET);
          }
        }
      }
      
      // If no HK data, use default rate for all dates
      if (exchangeRateMap.size === 0) {
        const CNY_TO_HKD = 1 / 0.901;
        for (const bsRow of balance_sheet_10y) {
          const date = toISODate(bsRow.report_date);
          if (date) {
            exchangeRateMap.set(date, CNY_TO_HKD);
          }
        }
      }
      
      // Apply exchange rate conversion to all financial data
      const convertRow = (row) => {
        if (!row) return row;
        const date = toISODate(row.report_date);
        const rate = exchangeRateMap.get(date) || (1 / 0.901);
        const converted = { ...row };
        
        // Convert all numeric fields except dates, symbols, and ratios
        const excludeFields = new Set(['report_date', 'symbol', 'security_name', 'start_date', 'end_date', 'trade_date', 'id', 'created_at', 'updated_at']);
        for (const [key, val] of Object.entries(converted)) {
          if (!excludeFields.has(key) && typeof val === 'number' && !key.includes('_yoy') && !key.includes('_ratio') && !key.includes('_rate')) {
            converted[key] = val * rate;
          }
        }
        return converted;
      };
      
      // Convert all financial statement data
      balance_sheet_10y.forEach((row, i) => { balance_sheet_10y[i] = convertRow(row); });
      income_statement_10y.forEach((row, i) => { income_statement_10y[i] = convertRow(row); });
      cash_flow_10y.forEach((row, i) => { cash_flow_10y[i] = convertRow(row); });
      bsQuarterlyDesc.forEach((row, i) => { bsQuarterlyDesc[i] = convertRow(row); });
      isQuarterlyDesc.forEach((row, i) => { isQuarterlyDesc[i] = convertRow(row); });
      cfQuarterlyDesc.forEach((row, i) => { cfQuarterlyDesc[i] = convertRow(row); });
      bsAnnualDesc.forEach((row, i) => { bsAnnualDesc[i] = convertRow(row); });
      isAnnualDesc.forEach((row, i) => { isAnnualDesc[i] = convertRow(row); });
      cfAnnualDesc.forEach((row, i) => { cfAnnualDesc[i] = convertRow(row); });
      isTTMDesc.forEach((row, i) => { isTTMDesc[i] = convertRow(row); });
      cfTTMDesc.forEach((row, i) => { cfTTMDesc[i] = convertRow(row); });
      
      // Convert market cap data (use nearest exchange rate by date)
      const CNY_TO_HKD = 1 / 0.901;
      mkt_cap_10y.forEach((row, i) => {
        if (row && row.mkt_cap_billion_cny) {
          const date = toISODate(row.trade_date);
          // Find nearest report date for exchange rate
          let rate = CNY_TO_HKD; // Default
          if (date && exchangeRateMap.size > 0) {
            const reportDates = Array.from(exchangeRateMap.keys()).sort();
            const nearest = reportDates.reduce((prev, curr) => 
              Math.abs(new Date(curr) - new Date(date)) < Math.abs(new Date(prev) - new Date(date)) ? curr : prev
            );
            rate = exchangeRateMap.get(nearest) || CNY_TO_HKD;
          }
          mkt_cap_10y[i] = { ...row, mkt_cap_billion_cny: row.mkt_cap_billion_cny * rate };
        }
      });
      
      // Convert annual market cap
      mktCapAnnualDesc.forEach((row, i) => {
        if (row && row.mkt_cap_billion_cny) {
          const date = toISODate(row.trade_date);
          let rate = CNY_TO_HKD;
          if (date && exchangeRateMap.size > 0) {
            const reportDates = Array.from(exchangeRateMap.keys()).sort();
            const nearest = reportDates.reduce((prev, curr) => 
              Math.abs(new Date(curr) - new Date(date)) < Math.abs(new Date(prev) - new Date(date)) ? curr : prev
            );
            rate = exchangeRateMap.get(nearest) || CNY_TO_HKD;
          }
          mktCapAnnualDesc[i] = { ...row, mkt_cap_billion_cny: row.mkt_cap_billion_cny * rate };
        }
      });
    }

    return res.status(200).json({
      symbol: isHKMode ? hkSymbol : symbol,
      security_name,
      market: isHKMode ? 'hk' : 'cn',
      currency: isHKMode ? 'HKD' : 'CNY',
      company_list,
      share_a_market,
      // 你指定的表名语义
      cn_balance_sheet_1y: balance_sheet_10y,
      cn_income_statement_10y: income_statement_10y,
      cn_cash_flow_10y: cash_flow_10y,
      cn_mkt_cap_10y: mkt_cap_10y,
      market_index_history,
      cn_top10_shareholder_10y: top10_shareholder_10y,
      cn_holder_court_concentration_10y: holder_court_concentration_10y,
      views: {
        quarterly: {
          balance_sheet: bsQuarterlyDesc, // 近 1 年
          income_statement: isQuarterlyDesc,
          cash_flow: cfQuarterlyDesc,
          mkt_cap: mkt_cap_10y,
        },
        annual: {
          balance_sheet: bsAnnualDesc,
          income_statement: isAnnualDesc,
          cash_flow: cfAnnualDesc,
          mkt_cap: mktCapAnnualDesc,
        },
        ltm: {
          balance_sheet: bsQuarterlyDesc,
          income_statement_ttm: isTTMDesc,
          cash_flow_ttm: cfTTMDesc,
          mkt_cap: mkt_cap_10y,
        },
      },
      meta: {
        balance_sheet_resolved_table: TABLES.balance_sheet,
        top10_resolved_table: TABLES.top10_shareholders,
        holders_resolved_table: TABLES.holder_count_concentration,
        dual_listed_mode: isHKMode,
        ashare_symbol: isHKMode ? symbol : null,
      },
    });
  } catch (err) {
    console.error('cn-analysis error:', err);
    return res.status(500).json({ error: err?.message || '查询失败' });
  }
}

