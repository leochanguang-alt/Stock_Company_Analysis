# Stock Company Analysis Dashboard

中国A股公司财务分析仪表板，支持财务数据下载、处理和可视化展示。

## 功能特性

- 📊 财务报表数据下载（资产负债表、利润表、现金流量表）
- 📈 市值历史数据跟踪
- 👥 股东数据分析（前十大股东、股东人数集中度）
- 🗄️ Supabase 数据库集成
- 🖥️ Web Dashboard 可视化

## 技术栈

- **后端**: Python 3.9+ (AKShare, Pandas, Supabase)
- **前端**: HTML/CSS/JavaScript
- **数据库**: Supabase (PostgreSQL)
- **数据源**: AKShare (东方财富、新浪财经等)

## 项目结构

```
├── scripts/              # Python 和 Node.js 脚本
│   ├── fetch_stock_data.py    # 数据下载脚本（并行优化）
│   ├── upload_stock_data.py   # 数据上传脚本（并行优化）
│   └── akshare_fetch_server.js # HTTP 服务器
├── supabase/             # 数据库迁移文件
│   └── migrations/       # SQL 迁移脚本
├── koyfin_dashboard_*.html  # Dashboard 页面
└── outputs/              # 下载的数据文件（gitignore）
```

## 快速开始

### 1. 安装依赖

```bash
# Python 依赖
python3 -m venv .venv
source .venv/bin/activate
pip install akshare pandas python-dotenv supabase

# Node.js 依赖
npm install
```

### 2. 配置环境变量

创建 `.env` 文件：

```env
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

### 3. 启动服务器

```bash
node scripts/akshare_fetch_server.js
```

### 4. 访问 Dashboard

打开浏览器访问 http://localhost:8000

## 数据下载

```bash
# 下载单个股票数据
python scripts/fetch_stock_data.py --symbol=002508

# 上传到 Supabase
python scripts/upload_stock_data.py --symbol=002508
```

## 性能优化

- 并行下载：6个数据源同时下载，速度提升 3.6x
- 并行上传：4个表同时上传，速度提升 1.7x
- 总体性能：从 ~260s 优化到 ~84s

## 自动化同步页面 (BSA)

本项目提供了在 `bsa.buiservice.com` 运行的自动化同步页面，架构如下：

- **前端**: 部署在 Vercel (`public/stock-search.html`)
- **后端**: 部署在 Modal.com (Python 运行环境)

### 后端部署 (Modal.com)

1. 安装 Modal CLI: `pip install modal`
2. 登录: `modal token new`
3. 配置 Secrets (在 Modal 控制台或 CLI):
   ```bash
   modal secret create supabase-secrets \
     SUPABASE_URL=你的URL \
     SUPABASE_SERVICE_ROLE_KEY=你的KEY
   ```
4. 部署: `modal deploy modal_app/app.py`
5. **记下生成的 Web Endpoint URL**。

### 前端部署 (Vercel)

1. 在 Vercel 项目中配置 `SUPABASE_URL` 和 `SUPABASE_ANON_KEY`。
2. 在 `public/stock-search.html` 中更新 `MODAL_ENDPOINT` 为上一步得到的 URL。

## 数据表说明

MIT
