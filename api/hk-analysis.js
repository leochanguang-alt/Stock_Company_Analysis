import { createClient } from '@supabase/supabase-js';

function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toISODate(d) {
  try {
    const dt = new Date(d);
    if (Number.isNaN(dt.getTime())) return null;
    return dt.toISOString().slice(0, 10);
  } catch { return null; }
}

function isYearEnd(dateStr) {
  return typeof dateStr === 'string' && dateStr.endsWith('-12-31');
}

function sortByDateDesc(rows, field = 'report_date') {
  return [...(rows || [])].sort((a, b) => String(b?.[field] || '').localeCompare(String(a?.[field] || '')));
}

// ---------- HK std_item_name -> A-share column name mappings ----------

const BS_MAP = {
  '现金及等价物': 'monetaryfunds',
  '交易性金融资产(流动)': 'trade_finasset',
  '指定以公允价值记账之金融资产(流动)': 'fvtpl_finasset',
  '指定以公允价值记账之金融资产': 'fvtoci_finasset',
  '其他金融资产(流动)': 'available_sale_finasset',
  '其他金融资产(非流动)': 'fvtoci_ncfinasset',
  '合同资产': 'contract_asset',
  '应收帐款': 'accounts_rece',
  '应收票据': 'note_rece',
  '预付款项': 'prepayment',
  '应收关联方款项': 'other_rece',
  '预付款按金及其他应收款': 'other_rece',
  '存货': 'inventory',
  '受限制存款及现金': 'settle_excess_reserve',
  '其他流动资产': 'other_current_asset',
  '流动资产其他项目': 'current_asset_other',
  '流动资产合计': 'total_current_assets',
  '非流动资产合计': 'total_noncurrent_assets',
  '非流动资产其他项目': 'noncurrent_asset_other',
  '物业厂房及设备': 'fixed_asset',
  '固定资产': 'fixed_asset',
  '在建工程': 'cip',
  '无形资产': 'intangible_asset',
  '商誉': 'goodwill',
  '长期投资': 'long_equity_invest',
  '联营公司权益': 'long_equity_invest',
  '合营公司权益': 'long_equity_invest',
  '长期应收款': 'long_rece',
  '递延税项资产': 'defer_tax_asset',
  '长期待摊费用': 'long_prepaid_expense',
  '开发支出': 'develop_expense',
  '投资物业': 'invest_property',
  '土地使用权': 'land_use_right',
  '其他非流动资产': 'other_noncurrent_asset',
  '总资产': 'total_assets',
  '应付帐款': 'accounts_payable',
  '应付票据': 'note_payable',
  '其他应付款及应计费用': 'other_payable',
  '职工薪酬及福利(流动)': 'staff_salary_payable',
  '短期贷款': 'short_loan',
  '应付税项': 'tax_payable',
  '合同负债': 'contract_liab',
  '预收款项': 'advance_receivables',
  '其他流动负债': 'other_current_liab',
  '流动负债其他项目': 'current_liab_other',
  '流动负债合计': 'total_current_liab',
  '长期贷款': 'long_loan',
  '长期应付款': 'long_payable',
  '融资租赁负债(非流动)': 'lease_liab',
  '融资租赁负债(流动)': 'lease_liab_1year',
  '递延收入(非流动)': 'defer_income',
  '递延收入(流动)': 'defer_income_current',
  '递延税项负债': 'defer_tax_liab',
  '递延税项负债(流动)': 'defer_tax_liab_current',
  '其他非流动负债': 'other_noncurrent_liab',
  '非流动负债其他项目': 'noncurrent_liab_other',
  '非流动负债合计': 'total_noncurrent_liab',
  '总负债': 'total_liabilities',
  '应付关联方款项(流动)': 'related_party_payable',
  '交易性金融负债(流动)': 'trade_finliab',
  '衍生金融工具-负债(流动)': 'derivative_liab',
  '拨备(流动)': 'provision_current',
  '持作出售的负债(流动)': 'held_for_sale_liab',
  '持作出售的资产(流动)': 'held_for_sale_asset',
  '股东权益': 'total_parent_equity',
  '少数股东权益': 'minority_equity',
  '总权益': 'total_equity',
  '股本': 'share_capital',
  '公积金': 'capital_reserve',
  '储备': 'surplus_reserve',
  '保留溢利(累计亏损)': 'unassign_rpofit',
  '库存股': 'treasury_shares',
  '其他综合性收益': 'other_compre_income',
  '其他权益工具': 'other_equity_instrument',
  '股东权益其他项目': 'parent_equity_other',
  '其他储备': 'other_reserve',
  '永久资本证券': 'perpetual_securities',
  '拟派股息': 'proposed_dividend',
  '发展中及待售物业': 'dev_property',
  '短期存款': 'short_deposit',
  '可供出售投资': 'available_sale_invest',
  '衍生金融工具-资产': 'derivative_asset',
  '衍生金融工具-资产(流动)': 'derivative_asset_current',
};

const IS_MAP = {
  '营业额': 'total_operate_income',
  '营运收入': 'total_operate_income',
  '营运收入其他项目': 'other_business_income',
  '营运支出': 'total_operate_cost',
  '销售成本': 'operate_cost',
  '毛利': 'gross_profit',
  '销售及分销费用': 'sale_expense',
  '行政开支': 'manage_expense',
  '研发费用': 'research_expense',
  '融资成本': 'finance_expense',
  '经营溢利': 'operate_profit',
  '除税前溢利': 'total_profit',
  '税项': 'income_tax',
  '持续经营业务税后利润': 'continued_netprofit',
  '除税后溢利': 'netprofit',
  '少数股东损益': 'minority_interest',
  '股东应占溢利': 'parent_netprofit',
  '每股基本盈利': 'basic_eps',
  '每股摊薄盈利': 'diluted_eps',
  '其他收入': 'other_income',
  '其他收益': 'other_gains',
  '减值及拨备': 'asset_impairment_loss',
  '重估盈余': 'revaluation_surplus',
  '出售资产之溢利': 'asset_disposal_income',
  '其他支出': 'other_expense',
  '营业税金及附加': 'operate_tax_add',
  '应占联营公司溢利': 'invest_joint_income',
  '应占合营公司溢利': 'invest_jv_income',
  '溢利其他项目': 'profit_other_items',
  '其他全面收益': 'other_compre_income_is',
  '其他全面收益其他项目': 'other_compre_income_other',
  '全面收益总额': 'total_compre_income',
  '非控股权益应占全面收益总额': 'minority_compre_income',
  '本公司拥有人应占全面收益总额': 'parent_compre_income',
  '股息': 'dividend',
};

const CF_MAP = {
  '除税前溢利(业务利润)': 'netprofit',
  '减:利息收入': 'interest_income',
  '加:利息支出': 'interest_expense',
  '减:投资收益': 'invest_loss',
  '减:应占附属公司溢利': 'subsidiary_profit',
  '加:减值及拨备': 'asset_impairment',
  '减:重估盈余': 'revaluation_surplus_cf',
  '减:出售资产之溢利': 'disposal_longasset_loss',
  '加:折旧及摊销': 'fa_ir_depr',
  '加:购股权开支': 'stock_option_expense',
  '减:政府补助': 'government_grant',
  '加:经营调整其他项目': 'operate_adjust_other',
  '营运资金变动前经营溢利': 'operate_profit_before_wc',
  '存货(增加)减少': 'inventory_reduce',
  '应收帐款减少': 'operate_rece_reduce',
  '应收关联方款项(增加)减少': 'related_rece_reduce',
  '应付帐款及应计费用增加(减少)': 'operate_payable_add',
  '应付关联方款项增加(减少)': 'related_payable_add',
  '营运资本变动其他项目': 'wc_other',
  '预付款项、按金及其他应收款项减少(增加)': 'prepaid_reduce',
  '预收账款、按金及其他应付款增加(减少)': 'advance_payable_add',
  '递延收入(增加)减少': 'defer_income_reduce',
  '应收票据(增加)减少': 'note_rece_reduce',
  '发展中物业(增加)减少': 'dev_property_reduce',
  '贷款和垫款(增加)减少': 'loan_advance_reduce',
  '持作买卖投资(增加)减少': 'trading_invest_reduce',
  '存款(增加)减少': 'deposit_reduce',
  '经营产生现金': 'operate_cash_generated',
  '已收利息(经营)': 'interest_received_operate',
  '已付利息(经营)': 'interest_paid_operate',
  '已付税项': 'pay_all_tax',
  '经营业务其他项目': 'operate_other',
  '经营业务现金净额': 'netcash_operate',
  '已收利息(投资)': 'interest_received_invest',
  '已收股息(投资)': 'dividend_received_invest',
  '存款减少(增加)': 'deposit_invest_reduce',
  '处置固定资产': 'disposal_long_asset',
  '购建固定资产': 'construct_long_asset',
  '处置无形资产及其他资产': 'disposal_intangible',
  '购建无形资产及其他资产': 'construct_intangible',
  '出售附属公司': 'disposal_subsidiary_other',
  '收购附属公司': 'buy_subsidiary_equity',
  '收回投资所得现金': 'withdraw_invest',
  '投资支付现金': 'invest_pay_cash',
  '政府补助(投资)': 'government_grant_invest',
  '投资业务其他项目': 'invest_other',
  '投资业务现金净额': 'netcash_invest',
  '融资前现金净额': 'net_cash_before_finance',
  '新增借款': 'receive_loan_cash',
  '偿还借款': 'pay_debt_cash',
  '已付利息(融资)': 'finance_interest_paid',
  '已付股息(融资)': 'assign_dividend_porfit',
  '吸收投资所得': 'accept_invest_cash',
  '发行股份': 'issue_shares',
  '发行相关费用': 'issue_expense',
  '回购股份': 'repurchase_shares',
  '赎回债券': 'redeem_bond',
  '发行债券': 'issue_bond',
  '偿还融资租赁': 'pay_lease',
  '受限制存款及现金增加(减少)': 'restricted_cash_change',
  '融资业务其他项目': 'finance_other',
  '融资业务现金净额': 'netcash_finance',
  '汇率影响': 'rate_change_effect',
  '现金净额': 'cce_add',
  '期初现金': 'begin_cce',
  '期间变动其他项目': 'period_other',
  '期末现金': 'end_cce',
};

// EPS fields should NOT be divided by 1e8 (they are per-share values)
const EPS_FIELDS = new Set(['basic_eps', 'diluted_eps']);

/**
 * Pivot HK long-table rows into wide-table rows (one object per report_date).
 * Amounts are converted from raw HKD to 亿HKD (÷1e8) to match A-share storage convention.
 * EPS fields are kept as-is (per-share values).
 */
function pivotLongToWide(longRows, nameMap) {
  const byDate = new Map();
  for (const r of longRows || []) {
    const d = toISODate(r.report_date);
    if (!d) continue;
    if (!byDate.has(d)) {
      byDate.set(d, {
        report_date: d,
        symbol: r.security_code || r.secucode,
        security_name: r.security_name,
      });
    }
    const row = byDate.get(d);
    const name = r.std_item_name;
    const col = nameMap[name];
    if (col) {
      const existing = toNum(row[col]);
      let newVal = toNum(r.amount);
      if (newVal != null) {
        if (!EPS_FIELDS.has(col)) {
          newVal = newVal / 1e8;
        }
        if (col === 'long_equity_invest' && existing != null) {
          row[col] = existing + newVal;
        } else {
          row[col] = newVal;
        }
      }
    }
  }
  return Array.from(byDate.values());
}

function computeTTM(rowsDesc, sumFields, opts = {}) {
  const takeFirst = opts.takeFirst || [];
  const takeLast = opts.takeLast || [];
  const out = [];
  for (let i = 0; i < (rowsDesc || []).length; i++) {
    const anchor = rowsDesc[i];
    const window = rowsDesc.slice(i, i + 4);
    if (window.length < 4) break;
    const record = { report_date: anchor.report_date };
    for (const f of sumFields) {
      let s = 0, ok = false;
      for (const w of window) {
        const n = toNum(w?.[f]);
        if (n != null) { s += n; ok = true; }
      }
      record[f] = ok ? s : null;
    }
    for (const f of takeFirst) {
      record[f] = toNum(window[window.length - 1]?.[f]);
    }
    for (const f of takeLast) {
      record[f] = toNum(anchor?.[f]);
    }
    out.push(record);
  }
  return out;
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
  return [...byYear.entries()]
    .sort((a, b) => b[0].localeCompare(a[0]))
    .map(([, r]) => r);
}

function sortByDateAsc(rows, field = 'trade_date') {
  return [...(rows || [])].sort((a, b) => String(a?.[field] || '').localeCompare(String(b?.[field] || '')));
}

function normalizeCompanyDesc(s) {
  if (!s) return '';
  return String(s)
    .toLowerCase()
    .replace(/\bclass\s+[ah]\b/g, ' ')
    .replace(/\bshares?\b/g, ' ')
    .replace(/\blimited\b/g, ' ')
    .replace(/\bco\.?\b/g, ' ')
    .replace(/\bltd\.?\b/g, ' ')
    .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

async function resolveAshareSymbolForDualListed(supabase, code5) {
  const { data: hkRows, error: hkErr } = await supabase
    .from('hkse_market')
    .select('description,download_date')
    .eq('symbol', code5)
    .order('download_date', { ascending: false })
    .limit(1);
  if (hkErr) return null;
  const hkDesc = hkRows?.[0]?.description || '';
  const norm = normalizeCompanyDesc(hkDesc);
  if (!norm) return null;

  const terms = Array.from(new Set(norm.split(' ').filter((t) => t && t.length >= 2)));
  const queryTerms = terms.slice(0, 3);
  if (!queryTerms.length) return null;

  const candidatesBySymbol = new Map();
  for (const term of queryTerms) {
    const { data, error } = await supabase
      .from('share_a_market')
      .select('symbol,description,download_date')
      .ilike('description', `%${term}%`)
      .order('download_date', { ascending: false })
      .limit(100);
    if (error) return null;
    for (const r of data || []) {
      const d = normalizeCompanyDesc(r.description || '');
      if (!d || !queryTerms.some((t) => d.includes(t))) continue;
      const prev = candidatesBySymbol.get(r.symbol);
      if (!prev || String(r.download_date || '') > String(prev.download_date || '')) {
        candidatesBySymbol.set(r.symbol, r);
      }
    }
  }

  if (!candidatesBySymbol.size) return null;
  const ranked = Array.from(candidatesBySymbol.values()).map((r) => {
    const d = normalizeCompanyDesc(r.description || '');
    const score = queryTerms.reduce((acc, t) => acc + (d.includes(t) ? 1 : 0), 0);
    return { ...r, score };
  }).sort((a, b) => (b.score - a.score) || String(b.download_date || '').localeCompare(String(a.download_date || '')));

  const best = ranked[0];
  if (!best || best.score <= 0) return null;
  return String(best.symbol || '').trim() || null;
}

export default async function handler(req, res) {
  const rawSymbol = String(req.query?.symbol || '').trim().toLowerCase().replace(/\.hk$/i, '');
  const symbol = rawSymbol.padStart(5, '0');

  if (!/^\d{5}$/.test(symbol)) {
    return res.status(400).json({ error: 'HK symbol 必须是 5 位数字代码（如 01211）' });
  }

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: '服务器配置错误: 缺少 Supabase 凭证' });
  }

  const supabase = createClient(supabaseUrl, supabaseKey);
  const secucode = `${symbol}.HK`;

  try {
    // Check if dual-listed (A+H) and prefer A-share financial data if available.
    const ashareSymbol = await resolveAshareSymbolForDualListed(supabase, symbol);
    console.log(`[hk-analysis] ${secucode} resolved A-share symbol:`, ashareSymbol);

    if (ashareSymbol) {
      // Try fetch A-share financials first
      const { data: ashareBS, error: bsErr } = await supabase
        .from('cn_balance_sheet_10y')
        .select('*')
        .eq('symbol', ashareSymbol)
        .order('report_date', { ascending: false })
        .limit(1);
      console.log(`[hk-analysis] A-share BS query result:`, { count: ashareBS?.length, error: bsErr?.message });
      if (!bsErr && ashareBS && ashareBS.length > 0) {
        // A-share data exists, use cn_analysis endpoint instead
        console.log(`[hk-analysis] Redirecting to cn-analysis for ${ashareSymbol}`);
        return res.redirect(307, `/api/cn-analysis?symbol=${ashareSymbol}&_hk_mode=1&_hk_symbol=${secucode}`);
      }
    }

    // Fallback: Fetch HK long-table data
    const fetchAll = async (table) => {
      const all = [];
      const batchSize = 1000;
      let offset = 0;
      while (true) {
        const { data, error } = await supabase
          .from(table)
          .select('*')
          .eq('secucode', secucode)
          .order('report_date', { ascending: false })
          .range(offset, offset + batchSize - 1);
        if (error) throw error;
        if (!data || data.length === 0) break;
        all.push(...data);
        if (data.length < batchSize) break;
        offset += batchSize;
      }
      return all;
    };

    // Fetch company info from company_list table
    let companyInfo = null;
    const symbolCandidates = Array.from(new Set([symbol, rawSymbol, secucode]));
    try {
      // 1) Preferred: explicit HK market
      const { data: companyRows1, error: companyErr1 } = await supabase
        .from('company_list')
        .select('symbol,market,description,sector,industry,exchange')
        .in('symbol', symbolCandidates)
        .eq('market', 'hk')
        .limit(1);
      if (companyErr1) throw companyErr1;
      companyInfo = companyRows1?.[0] || null;

      // 2) Fallback: HKEX exchange
      if (!companyInfo) {
        const { data: companyRows2, error: companyErr2 } = await supabase
          .from('company_list')
          .select('symbol,market,description,sector,industry,exchange')
          .in('symbol', symbolCandidates)
          .eq('exchange', 'HKEX')
          .limit(1);
        if (companyErr2) throw companyErr2;
        companyInfo = companyRows2?.[0] || null;
      }
    } catch (e) {
      console.warn('company_list query failed:', e?.message);
      companyInfo = null;
    }

    // Fetch latest HK market metrics (EV / Mkt Cap / Beta / Date)
    let hkMarketInfo = null;
    try {
      const { data: marketRows, error: marketErr } = await supabase
        .from('hkse_market')
        .select('symbol,enterprise_value,market_capitalization,beta_5_years,beta_1_year,download_date')
        .in('symbol', symbolCandidates)
        .order('download_date', { ascending: false })
        .limit(1);
      if (marketErr) throw marketErr;
      hkMarketInfo = marketRows?.[0] || null;
    } catch (e) {
      console.warn('hkse_market query failed:', e?.message);
      hkMarketInfo = null;
    }

    const [bsLong, isLong, cfLong] = await Promise.all([
      fetchAll('hk_balance_sheet'),
      fetchAll('hk_income_statement'),
      fetchAll('hk_cash_flow'),
    ]);

    // Fetch HK historical market cap series
    const hkMktCapAll = [];
    {
      const batchSize = 1000;
      let offset = 0;
      while (true) {
        const { data, error } = await supabase
          .from('hk_company_mkt_cap')
          .select('date,market_value_hkd_亿元,secucode,security_code,security_name_abbr')
          .eq('secucode', secucode)
          .order('date', { ascending: true })
          .range(offset, offset + batchSize - 1);
        if (error) throw error;
        if (!data || data.length === 0) break;
        // Frontend expects cn_mkt_cap_10y items to have trade_date + mkt_cap_billion_cny.
        // market_value_hkd_亿元 -> "亿" unit; convert to "bn" proxy so existing formatter (x10) restores original "亿".
        hkMktCapAll.push(...data.map((r) => ({
          trade_date: r.date,
          symbol: r.security_code || symbol,
          mkt_cap_billion_cny: toNum(r.market_value_hkd_亿元) == null ? null : toNum(r.market_value_hkd_亿元) / 10,
        })));
        if (data.length < batchSize) break;
        offset += batchSize;
      }
    }

    // Pivot long tables to wide format
    const bsWide = pivotLongToWide(bsLong, BS_MAP);
    const isWide = pivotLongToWide(isLong, IS_MAP);
    const cfWide = pivotLongToWide(cfLong, CF_MAP);

    const bsDesc = sortByDateDesc(bsWide);
    const isDesc = sortByDateDesc(isWide);
    const cfDesc = sortByDateDesc(cfWide);

    const bsAnnual = bsDesc.filter(r => isYearEnd(r.report_date));
    const isAnnual = isDesc.filter(r => isYearEnd(r.report_date));
    const cfAnnual = cfDesc.filter(r => isYearEnd(r.report_date));

    // TTM computation
    const incomeSumFields = Object.values(IS_MAP);
    const cashSumFields = Object.values(CF_MAP).filter(f => f !== 'begin_cce' && f !== 'end_cce');

    const isTTM = computeTTM(isDesc, incomeSumFields);
    const cfTTM = computeTTM(cfDesc, cashSumFields, {
      takeFirst: ['begin_cce'],
      takeLast: ['end_cce'],
    });

    const securityName =
      bsLong?.[0]?.security_name ||
      isLong?.[0]?.security_name ||
      cfLong?.[0]?.security_name || null;

    const normalizedCompanyInfo = companyInfo
      ? {
          symbol: companyInfo.symbol || symbol,
          market: companyInfo.market || 'hk',
          description: companyInfo.description || securityName,
          sector: companyInfo.sector || '-',
          industry: companyInfo.industry || '-',
          exchange: companyInfo.exchange || 'HKEX',
        }
      : {
          symbol,
          market: 'hk',
          description: securityName,
          sector: '-',
          industry: '-',
          exchange: 'HKEX',
        };

    // For detailed tabs, use hk_company_mkt_cap history only.
    const mktCapAsc = sortByDateAsc(hkMktCapAll, 'trade_date');
    const mktCapAnnual = groupLatestByYear(mktCapAsc, 'trade_date');

    return res.status(200).json({
      symbol,
      security_name: normalizedCompanyInfo.description || securityName,
      market: 'hk',
      currency: 'HKD',
      company_list: normalizedCompanyInfo,
      // Keep legacy key name for frontend compatibility.
      // For HK mode this payload is sourced from hkse_market.
      share_a_market: hkMarketInfo,
      cn_balance_sheet_1y: bsDesc,
      cn_income_statement_10y: isDesc,
      cn_cash_flow_10y: cfDesc,
      cn_mkt_cap_10y: mktCapAsc,
      market_index_history: [],
      cn_top10_shareholder_10y: [],
      cn_holder_court_concentration_10y: [],
      views: {
        quarterly: {
          balance_sheet: bsDesc,
          income_statement: isDesc,
          cash_flow: cfDesc,
          mkt_cap: mktCapAsc,
        },
        annual: {
          balance_sheet: bsAnnual,
          income_statement: isAnnual,
          cash_flow: cfAnnual,
          mkt_cap: mktCapAnnual,
        },
        ltm: {
          balance_sheet: bsDesc,
          income_statement_ttm: isTTM,
          cash_flow_ttm: cfTTM,
          mkt_cap: mktCapAsc,
        },
      },
      meta: {
        balance_sheet_resolved_table: 'hk_balance_sheet',
        data_format: 'pivoted_from_long_table',
      },
    });
  } catch (err) {
    console.error('hk-analysis error:', err);
    return res.status(500).json({ error: err?.message || '查询失败' });
  }
}
