# 项目流程复现清单

以下是从数据导入到网页展示的**可执行脚本清单**，按顺序一步步复现。

## 0. 环境准备
1) `.env` 配置（必须）
```
SUPABASE_URL=...
SUPABASE_SERVICE_ROLE_KEY=...
GEMINI_API_KEY=...   # 若要评分
```

2) 安装依赖
```
npm install
```

---

## 1. 初始化 Supabase 表（如需重建）
```
supabase db push
```

---

## 2. 导入基础数据

### 2.1 导入新闻（CSV → Supabase）
```
node scripts/import_cn_company_news.js
```

### 2.2 导入股东人数
```
node scripts/import_sharehold_data.js
```

### 2.3 导入 Top10 股东
```
node scripts/import_top10_sharehold.js
```

### 2.4 导入上证指数
```
node scripts/import_market_index_history.js
```

---

## 3. 市值数据（akshare → CSV）
```
python3 scripts/export_mkt_cap_akshare.py --symbol 002508 --years 10
```

---

## 4. 新闻清洗 / 评分 / 去重（可选）
```
node scripts/process_cn_company_news_with_gemini.js --symbol 002508
```

---

## 5. 新闻 1 个月市值变化（用于相对强度）
```
node scripts/update_news_mkt_cap_change_1m.js --symbol 002508 --overwrite
```

---

## 6. 新闻相对强度（相对上证）
```
node scripts/update_news_relative_strength_1m.js --symbol 002508 --limit 1000
```

---

## 7. （可选）抓取最近 3 个月新闻并直接入库
```
python3 scripts/import_akshare_news_3m.py --symbol 000333 --months 3
```

---

## 8. 启动网页展示
本地静态服务器：
```
python3 -m http.server 8000
```

打开页面：
```
http://localhost:8000/koyfin_dashboard_002508.html
```

---

## 备注：页面数据来源
- 财务主表：`fetchAllFinancials()` 从 Supabase 读取
- 股东人数：`cn_sharehold_data`
- Top10 股东：`cn_top10_sharehold`
- 新闻：`cn_company_news`
- 上证指数：`market_index_history`
