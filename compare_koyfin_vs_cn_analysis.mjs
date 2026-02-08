import { chromium } from "playwright-core";
import fs from "node:fs";
import path from "node:path";

function argValue(name, defaultValue = null) {
  const idx = process.argv.indexOf(name);
  if (idx === -1) return defaultValue;
  const v = process.argv[idx + 1];
  if (!v || v.startsWith("--")) return defaultValue;
  return v;
}

function normalizeSpace(s) {
  return String(s ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function parseMaybeNumber(raw) {
  const s0 = normalizeSpace(raw);
  if (!s0 || s0 === "-" || s0 === "—") return null;

  // negative like (12.3) or (12.3%)
  const neg = /^\(.*\)$/.test(s0);
  const s1 = s0.replace(/^\(|\)$/g, "");

  // ratio like 3.2x
  const ratioMatch = s1.match(/^(-?\d+(?:\.\d+)?)x$/i);
  if (ratioMatch) {
    const n = Number(ratioMatch[1]);
    return neg ? -Math.abs(n) : n;
  }

  // percent like 12.3%
  const pctMatch = s1.match(/^(-?\d+(?:\.\d+)?)%$/);
  if (pctMatch) {
    const n = Number(pctMatch[1]) / 100;
    return neg ? -Math.abs(n) : n;
  }

  // money in 亿元 e.g. 1766.38 亿元
  const yiMatch = s1.match(/^(-?\d+(?:\.\d+)?)(?:\s*)亿元$/);
  if (yiMatch) {
    const n = Number(yiMatch[1]);
    return neg ? -Math.abs(n) : n;
  }

  // old dashboard monetary: 12.3M (we normalize to 亿元; 1M CNY = 0.01 亿元)
  const mMatch = s1.match(/^(-?\d+(?:\.\d+)?)M$/i);
  if (mMatch) {
    const n = Number(mMatch[1]) * 0.01;
    return neg ? -Math.abs(n) : n;
  }

  // plain number with commas
  const s2 = s1.replace(/,/g, "");
  if (/^-?\d+(\.\d+)?$/.test(s2)) {
    const n = Number(s2);
    return neg ? -Math.abs(n) : n;
  }

  return null;
}

async function clickTabByText(page, containerSelector, text) {
  const tabs = await page.locator(`${containerSelector} .tab`).all();
  for (const t of tabs) {
    const tt = normalizeSpace(await t.textContent());
    if (tt.toLowerCase() === text.toLowerCase()) {
      await t.click();
      return true;
    }
  }
  // fallback: contains
  for (const t of tabs) {
    const tt = normalizeSpace(await t.textContent());
    if (tt.toLowerCase().includes(text.toLowerCase())) {
      await t.click();
      return true;
    }
  }
  return false;
}

async function waitForTableStable(page, containerSelector) {
  // 关键：不要在 “Loading data...” 文本稳定时提前返回；
  // 我们优先等待容器内真正出现 <table>，再做短暂稳定检查。
  const loc = page.locator(containerSelector);
  await loc.waitFor({ state: "visible", timeout: 60_000 });

  // Wait until at least one table exists in the container.
  try {
    await page.waitForFunction(
      (sel) => {
        const el = document.querySelector(sel);
        if (!el) return false;
        return el.querySelectorAll("table").length > 0;
      },
      containerSelector,
      { timeout: 60_000 }
    );
  } catch {
    // If table never appears (some tabs), fall back to best-effort.
  }

  // Then wait for text to stabilize briefly.
  await page.waitForTimeout(250);
  let prev = "";
  for (let i = 0; i < 16; i++) {
    const cur = normalizeSpace(await loc.textContent());
    if (cur && cur === prev) return;
    prev = cur;
    await page.waitForTimeout(200);
  }
}

async function extractTables(page, containerSelector) {
  return await page.evaluate((sel) => {
    const container = document.querySelector(sel);
    if (!container) return [];
    const tables = Array.from(container.querySelectorAll("table"));
    return tables.map((table) => {
      const headers = Array.from(table.querySelectorAll("thead th")).map((th) =>
        (th.textContent || "").replace(/\s+/g, " ").trim()
      );
      const rows = Array.from(table.querySelectorAll("tbody tr")).map((tr) => {
        const cells = Array.from(tr.querySelectorAll("th,td")).map((td) =>
          (td.textContent || "").replace(/\s+/g, " ").trim()
        );
        return cells;
      });
      return { headers, rows };
    });
  }, containerSelector);
}

function tablesToMatrix(tables) {
  // Flatten to a map: rowLabel -> { colLabel -> cellText }
  // We only consider the first table for numeric compare by default.
  if (!tables || tables.length === 0) return { headers: [], matrix: {} };
  const t = tables[0];
  const headers = t.headers || [];
  const matrix = {};
  // Canonicalize row labels so old/new can align.
  // Map BOTH CN and EN variants to the same canonical key.
  const rowAliases = {
    // Canonical finance summary keys
    "市值": "Market Cap",
    "Market Cap": "Market Cap",
    "Market Capitalization": "Market Cap",

    "企业价值 EV": "EV",
    "企业价值": "EV",
    "EV": "EV",
    "Enterprise Value": "EV",
    "Enterprise Value - EV": "EV",

    "营收": "Revenue",
    "Revenue": "Revenue",
    "Total Revenues": "Revenue",

    "归母净利润": "Net Income Parent",
    "Net Income Parent": "Net Income Parent",
    // Old page often labels this as Net Income; map to parent for comparison.
    "Net Income": "Net Income Parent",

    "经营现金流": "Operating Cash Flow",
    "Operating Cash Flow": "Operating Cash Flow",
    "Cash from Operations": "Operating Cash Flow",

    "毛利（估算）": "Gross Profit",
    "Gross Profit": "Gross Profit",
    "Gross Profit (Loss)": "Gross Profit",

    "毛利率（估算）": "Gross Margin",
    "Gross Margin": "Gross Margin",
    "Gross Profit Margin": "Gross Margin",

    "营业利润率": "Operating Margin",
    "Operating Margin": "Operating Margin",

    "净利率（归母）": "Net Margin",
    "Net Margin": "Net Margin",
    "Net Income Margin": "Net Margin",

    "ROE（归母）": "ROE",
    "ROE": "ROE",
    "ROA": "ROA",

    // Multiples
    "P/E": "P/E",
    "Price / Earnings - P/E": "P/E",
    "P/B": "P/B",
    "P/S": "P/S",
    "EV/Sales": "EV/Sales",
    "aOCF/EV": "aOCF/EV",

    // Common statement items
    "营业成本": "COGS",
    "Cost Of Revenues": "COGS",
    "COGS": "COGS",

    "营业利润": "Operating Income",
    "Operating Income": "Operating Income",

    "利润总额": "Pretax Income",
    "Pretax Income": "Pretax Income",
    "EBT, Incl. Unusual Items": "Pretax Income",

    "所得税费用": "Income Tax Expense",
    "Income Tax Expense": "Income Tax Expense",

    "经营活动现金流净额": "OCF",
    "OCF": "OCF",

    "购建长期资产支付现金（CapEx）": "CapEx",
    "Capital Expenditure": "CapEx",
    "CapEx": "CapEx",
  };

  const shouldSkipRow = (label) => {
    if (!label) return true;
    const s = normalizeSpace(label);
    return (
      s.startsWith("▼") ||
      s.startsWith("▶") ||
      s.startsWith("点击展开") ||
      s.startsWith("Click") ||
      s === ""
    );
  };

  const canonicalizeCol = (colLabel) => {
    const s = normalizeSpace(colLabel);
    if (!s) return s;
    if (s === "最新" || s.toLowerCase() === "latest") return "__latest__";
    const mY = s.match(/^CY\s+(\d{4})$/i);
    if (mY) return `${mY[1]}-12-31`;
    const mQ = s.match(/^([1-4])Q\s+CY(\d{4})$/i);
    if (mQ) {
      const q = Number(mQ[1]);
      const y = mQ[2];
      const end = q === 1 ? "03-31" : q === 2 ? "06-30" : q === 3 ? "09-30" : "12-31";
      return `${y}-${end}`;
    }
    // New page often uses ISO date already
    if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
    return s;
  };

  for (const row of t.rows || []) {
    if (!row || row.length === 0) continue;
    const rawLabel = normalizeSpace(row[0]);
    if (shouldSkipRow(rawLabel)) continue;
    const rowLabel = rowAliases[rawLabel] ?? rawLabel;
    matrix[rowLabel] = matrix[rowLabel] || {};
    for (let i = 1; i < row.length; i++) {
      const colRaw = normalizeSpace(headers[i] ?? String(i));
      const col = canonicalizeCol(colRaw);
      matrix[rowLabel][col] = row[i];
    }
  }
  return { headers, matrix };
}

function diffMatrices(a, b, opts) {
  const epsilon = opts?.epsilon ?? 0.02; // in 亿元 for money; in ratio for others
  const out = [];

  const rowKeys = new Set([...Object.keys(a.matrix), ...Object.keys(b.matrix)]);
  for (const row of Array.from(rowKeys).sort()) {
    const aRow = a.matrix[row];
    const bRow = b.matrix[row];
    if (!aRow || !bRow) {
      out.push({ kind: "row_missing", row, a: !!aRow, b: !!bRow });
      continue;
    }
    // Only compare columns that exist on both sides (intersection),
    // otherwise old-only/new-only columns would create noisy diffs.
    const colKeys = new Set(Object.keys(aRow).filter((k) => Object.prototype.hasOwnProperty.call(bRow, k)));
    for (const col of Array.from(colKeys).sort()) {
      const av = aRow[col];
      const bv = bRow[col];
      const an = parseMaybeNumber(av);
      const bn = parseMaybeNumber(bv);
      if (an === null || bn === null) {
        // If both non-numeric, compare text
        if (normalizeSpace(av) !== normalizeSpace(bv)) {
          out.push({ kind: "cell_text_diff", row, col, a: av, b: bv });
        }
        continue;
      }
      const diff = Math.abs(an - bn);
      if (diff > epsilon) {
        out.push({
          kind: "cell_num_diff",
          row,
          col,
          a: av,
          b: bv,
          a_num: an,
          b_num: bn,
          diff,
        });
      }
    }
  }

  return out;
}

async function run() {
  const symbol = argValue("--symbol", "601319");
  const outDir = argValue("--outDir", "playwright-compare");
  const headless = argValue("--headful", null) ? false : true;

  const oldFile = path.resolve(process.cwd(), "koyfin_dashboard_002508.html");
  const oldUrl = `file://${oldFile}`;
  const newUrl = "https://bsa.buiservice.com/stock-search.html";

  fs.mkdirSync(outDir, { recursive: true });

  const browser = await chromium.launch({
    headless,
    // Use local Chrome to avoid downloading browsers.
    channel: "chrome",
  });

  const ctx = await browser.newContext({
    viewport: { width: 1440, height: 900 },
  });

  const pageOld = await ctx.newPage();
  const pageNew = await ctx.newPage();

  const views = [
    { key: "annual", oldBtnText: "Annual (Y)", newBtnId: "#btnViewAnnual" },
    { key: "ltm", oldBtnText: "Last 12 Months (LTM)", newBtnId: "#btnViewLTM" },
  ];

  const tabs = [
    { key: "Highlights", labelOld: "Highlights", labelNew: "Highlights" },
    { key: "IncomeStatement", labelOld: "Income Statement", labelNew: "Income" },
    { key: "BalanceSheet", labelOld: "Balance Sheet", labelNew: "Balance" },
    { key: "CashFlow", labelOld: "Cash Flow", labelNew: "Cash Flow" },
    { key: "Multiples", labelOld: "Multiples", labelNew: "Multiples" },
    { key: "EnterpriseValue", labelOld: "Enterprise Value", labelNew: "EV" },
    { key: "Profitability", labelOld: "Profitability", labelNew: "Profitability" },
    { key: "ROIC", labelOld: "ROIC", labelNew: "ROIC" },
    { key: "Solvency", labelOld: "Solvency", labelNew: "Solvency" },
    // New page merges holders + top10 into Shareholders; we compare against Holders table by default.
    { key: "Shareholders", labelOld: "Holders", labelNew: "Shareholders" },
  ];

  // Open pages
  await pageOld.goto(oldUrl, { waitUntil: "domcontentloaded" });
  await pageNew.goto(newUrl, { waitUntil: "domcontentloaded" });

  // Old: input company code
  await pageOld.locator("#company-search-input").waitFor({ timeout: 60_000 });
  await pageOld.fill("#company-search-input", symbol);
  await pageOld.dispatchEvent("#company-search-input", "change");
  await pageOld.keyboard.press("Enter").catch(() => {});

  // New: input and load analysis
  await pageNew.locator("#symbolInput").waitFor({ timeout: 60_000 });
  await pageNew.fill("#symbolInput", symbol);
  await pageNew.click("#viewBtn");

  const report = { symbol, generated_at: new Date().toISOString(), views: {} };

  for (const v of views) {
    // set view
    await pageOld.getByRole("button", { name: v.oldBtnText }).click();
    await pageNew.click(v.newBtnId);

    // ensure the main table is rendered
    await waitForTableStable(pageOld, "#table-box");
    await waitForTableStable(pageNew, "#analysisBox");

    const viewReport = { tabs: {} };

    for (const t of tabs) {
      // click tab
      const okOld = await clickTabByText(pageOld, "#tabs", t.labelOld);
      const okNew = await clickTabByText(pageNew, "#analysisTabs", t.labelNew);

      if (!okOld || !okNew) {
        viewReport.tabs[t.key] = {
          error: "tab_not_found",
          okOld,
          okNew,
        };
        continue;
      }

      await waitForTableStable(pageOld, "#table-box");
      await waitForTableStable(pageNew, "#analysisBox");

      const oldTables = await extractTables(pageOld, "#table-box");
      const newTables = await extractTables(pageNew, "#analysisBox");

      const oldMat = tablesToMatrix(oldTables);
      const newMat = tablesToMatrix(newTables);

      const diffs = diffMatrices(oldMat, newMat, { epsilon: 0.05 });

      // Capture small screenshots for debugging
      const shotOld = path.join(outDir, `old_${symbol}_${v.key}_${t.key}.png`);
      const shotNew = path.join(outDir, `new_${symbol}_${v.key}_${t.key}.png`);
      await pageOld.screenshot({ path: shotOld, fullPage: false });
      await pageNew.screenshot({ path: shotNew, fullPage: false });

      viewReport.tabs[t.key] = {
        diffs_count: diffs.length,
        diffs: diffs.slice(0, 200),
        screenshots: {
          old: path.basename(shotOld),
          new: path.basename(shotNew),
        },
      };
    }

    report.views[v.key] = viewReport;
  }

  const outJson = path.join(outDir, `report_${symbol}.json`);
  fs.writeFileSync(outJson, JSON.stringify(report, null, 2), "utf8");

  // Produce a compact markdown summary
  const lines = [];
  lines.push(`# Playwright 对比报告`);
  lines.push(`- symbol: \`${symbol}\``);
  lines.push(`- generated_at: \`${report.generated_at}\``);
  lines.push(`- old: \`${oldUrl}\``);
  lines.push(`- new: \`${newUrl}\``);
  lines.push("");
  for (const viewKey of Object.keys(report.views)) {
    lines.push(`## view: ${viewKey}`);
    const tabsObj = report.views[viewKey].tabs || {};
    for (const tabKey of Object.keys(tabsObj)) {
      const r = tabsObj[tabKey];
      if (r.error) {
        lines.push(`- **${tabKey}**: error=${r.error} old=${r.okOld} new=${r.okNew}`);
      } else {
        lines.push(`- **${tabKey}**: diffs=${r.diffs_count} (截图 old=${r.screenshots.old} new=${r.screenshots.new})`);
      }
    }
    lines.push("");
  }

  const outMd = path.join(outDir, `report_${symbol}.md`);
  fs.writeFileSync(outMd, lines.join("\n"), "utf8");

  await browser.close();

  console.log(`Wrote ${outJson}`);
  console.log(`Wrote ${outMd}`);
}

run().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

