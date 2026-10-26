"use strict";

// All API values enter the page through text nodes. No dynamic HTML is evaluated.
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => Array.from(document.querySelectorAll(selector));
const state = { page: "overview", overview: null, config: null, runs: [], overviewRun: null, run: null, runId: "", metric: "nav", dataset: "stock_daily", symbol: "", logKind: "orders", logOffset: 0, logTotal: 0, dataToken: 0, barsToken: 0, runToken: 0, logToken: 0, demoBusy: false };
const pageNames = { overview: "系统概览", data: "真实数据", strategy: "策略配置", runs: "运行记录" };
const datasetNames = { stock_daily: "A股日线", etf_daily: "ETF日线", financials: "财务快照", financial: "财务快照", financial_snapshots: "财务快照", financial_originals: "原始财报证据", corporate_actions: "公司行动", actions: "公司行动", action_samples: "公司行动", action_notice_evidence: "公司行动原公告", stock_actions: "公司行动", stock_status_reference: "历史状态来源观察", exchange_calendar: "交易日历", sse_calendar: "上交所交易日历", szse_calendar: "深交所交易日历", calendar: "交易日历", security_metadata: "证券元信息", securities: "证券元信息" };
const strategyNames = { stock: "个股多因子", etf_rotation: "ETF轮动", defensive: "防御配置" };
const shortGaps = {
  historical_financial_vintages: "财务原始披露版本待补全",
  point_in_time_financials: "财务原始披露版本待补全",
  missing_retired_prices: "退市证券行情覆盖待补全",
  historical_security_status: "历史证券状态待核验",
  historical_tradability: "历史可交易状态待核验",
  historical_industry: "历史行业归属待核验",
  corporate_action_accounting: "公司行动与账户处理待核验",
  original_financial_vintages_unavailable: "财务原始披露版本待补全"
};
const svgNS = "http://www.w3.org/2000/svg";

function node(tag, className, text) {
  const item = document.createElement(tag);
  if (className) item.className = className;
  if (text !== undefined && text !== null) item.textContent = String(text);
  return item;
}
function svgNode(tag, attrs = {}) {
  const item = document.createElementNS(svgNS, tag);
  Object.entries(attrs).forEach(([key, value]) => item.setAttribute(key, String(value)));
  return item;
}
function clean(target) { target.replaceChildren(); return target; }
function validNumber(value) { return value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value)); }
function number(value, decimals = 0) { return validNumber(value) ? Number(value).toLocaleString("zh-CN", { maximumFractionDigits: decimals, minimumFractionDigits: decimals }) : "—"; }
function pct(value, decimals = 1) { return validNumber(value) ? `${number(Number(value) * 100, decimals)}%` : "—"; }
function date(value) { return value ? String(value).slice(0, 10) : "—"; }
function collection(value) { return Array.isArray(value) ? value : value === null || value === undefined ? [] : [value]; }
function count(value) { return Array.isArray(value) ? value.length : validNumber(value) ? Number(value) : null; }
function countLabel(value) { return value === null || value === undefined ? "—" : number(value); }
function compactRows(value) { return validNumber(value) && Number(value) >= 10000 ? number(Number(value) / 10000, 1) : number(value); }
function readable(value) {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
function humanGap(value) {
  const source = typeof value === "object" && value ? value.message || value.reason || value.name || JSON.stringify(value) : String(value);
  if (shortGaps[source]) return shortGaps[source];
  if (/financial|vintage|财务|披露/i.test(source)) return "财务原始披露与可用时间待核验";
  if (/retir|delist|surviv|退市|幸存/i.test(source)) return "退市证券与历史股票池覆盖待补全";
  if (/tradab|suspend|halt|status|ST|停牌|交易状态/i.test(source)) return "历史停复牌与证券状态待核验";
  if (/industry|行业/i.test(source)) return "历史行业与穿透暴露待核验";
  if (/calendar|session|日历/i.test(source)) return "交易日历覆盖待核验";
  if (/split|dividend|corporate|action|rights|复权|分红|行动/i.test(source)) return "公司行动与价格调整待核验";
  if (/fee|rule|limit|执行|费用|涨跌停/i.test(source)) return "历史交易规则与执行条件待核验";
  return /[\u3400-\u9fff]/.test(source) ? source : "部分历史字段仍需核验";
}
async function api(path, options = {}) {
  const response = await fetch(path, { credentials: "same-origin", ...options, headers: { Accept: "application/json", ...options.headers } });
  let payload;
  try { payload = await response.json(); } catch (_) { throw new Error("服务返回了无法读取的内容，请重试。"); }
  if (!response.ok) throw new Error(payload.message || payload.error || `读取失败（${response.status}），请重试。`);
  return payload;
}
function message(text, error = false, retry) {
  const target = clean($("#global-message"));
  target.className = `message${error ? " error" : ""}`;
  target.append(node("span", "", text));
  if (retry) { const button = node("button", "", "重试"); button.type = "button"; button.addEventListener("click", retry); target.append(button); }
}
function hideMessage() { $("#global-message").classList.add("hidden"); }
function empty(target, text, retry, loading = false) {
  clean(target);
  const wrap = node("div", "empty-state");
  if (loading) wrap.append(node("span", "loading-dot"));
  wrap.append(node("span", "", text));
  if (retry) { const button = node("button", "button secondary compact", "重新读取"); button.type = "button"; button.addEventListener("click", retry); wrap.append(button); }
  target.append(wrap);
}
function table(target, columns, items, emptyText = "暂无记录") {
  clean(target);
  if (!items.length) { empty(target, emptyText); return; }
  const element = node("table");
  const head = node("thead"); const header = node("tr");
  columns.forEach((column) => header.append(node("th", "", column.label)));
  head.append(header); element.append(head);
  const body = node("tbody");
  items.forEach((item) => {
    const row = node("tr");
    columns.forEach((column) => {
      const cell = node("td", column.className || "");
      const value = column.render ? column.render(item) : item[column.key];
      if (value instanceof Node) cell.append(value); else cell.textContent = readable(value);
      row.append(cell);
    });
    body.append(row);
  });
  element.append(body); target.append(element);
}
function tag(text, className = "neutral") { return node("span", `tag ${className}`, text); }
function kpi(label, value, unit, note, accent) {
  const card = node("div", "kpi-card");
  card.append(node("div", "kpi-label", label));
  const main = node("div", "kpi-value", value); if (unit) main.append(node("small", "", unit)); card.append(main);
  card.append(node("div", "kpi-note", note));
  if (accent) card.append(node("span", "kpi-accent", accent));
  return card;
}
function audit(target, source, notes = []) {
  clean(target);
  notes.forEach((note) => target.append(node("p", "", note)));
  if (source !== null && source !== undefined) target.append(node("pre", "", JSON.stringify(source, null, 2)));
}

function chart(target, records, metric = "nav", prefix = "account") {
  clean(target);
  const field = metric === "drawdown" ? "drawdown" : "nav";
  const data = records.filter((item) => validNumber(item[field]));
  if (!data.length) { empty(target, "还没有可显示的合成演示记录。"); return; }
  const initial = Number(data[0].nav);
  const normalized = metric !== "drawdown" && Number.isFinite(initial) && initial > 0;
  const valueOf = (item) => metric === "drawdown" ? -Math.abs(Number(item.drawdown)) : normalized ? Number(item.nav) / initial : Number(item.nav);
  const values = data.map(valueOf);
  const width = 750, height = 250, left = 49, right = 16, top = 17, bottom = 34;
  let min = Math.min(...values), max = Math.max(...values);
  const pad = (max - min || Math.abs(max) * .05 || .05) * .18;
  min -= pad; max += pad;
  const x = (index) => left + index / Math.max(1, data.length - 1) * (width - left - right);
  const y = (value) => height - bottom - (value - min) / (max - min) * (height - top - bottom);
  const svg = svgNode("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": metric === "drawdown" ? "合成工程演示的账户回撤轨迹" : "合成工程演示的归一化账户净值轨迹", preserveAspectRatio: "none" });
  const defs = svgNode("defs"); const gradientId = `${prefix}-${metric}-gradient`;
  const gradient = svgNode("linearGradient", { id: gradientId, x1: 0, y1: 0, x2: 0, y2: 1 });
  gradient.append(svgNode("stop", { offset: "0%", "stop-color": metric === "drawdown" ? "#bc9a75" : "#5c83e2", "stop-opacity": .13 }), svgNode("stop", { offset: "100%", "stop-color": "#fff", "stop-opacity": 0 })); defs.append(gradient); svg.append(defs);
  for (let i = 0; i <= 4; i++) {
    const value = min + i / 4 * (max - min); const pointY = y(value);
    svg.append(svgNode("line", { x1: left, x2: width - right, y1: pointY, y2: pointY, stroke: "#edf1f6", "stroke-dasharray": "3 4" }));
    const label = svgNode("text", { x: left - 10, y: pointY + 3, "text-anchor": "end", fill: "#a0adbe", "font-size": 10 }); label.textContent = metric === "drawdown" ? pct(value, 1) : number(value, normalized ? 3 : 0); svg.append(label);
  }
  const points = values.map((value, index) => `${x(index).toFixed(2)},${y(value).toFixed(2)}`).join(" ");
  svg.append(svgNode("polygon", { points: `${left},${height - bottom} ${points} ${x(data.length - 1)},${height - bottom}`, fill: `url(#${gradientId})` }));
  svg.append(svgNode("polyline", { points, fill: "none", stroke: metric === "drawdown" ? "#bb9670" : "#557cdf", "stroke-width": 2.1, "stroke-linejoin": "round", "stroke-linecap": "round", "vector-effect": "non-scaling-stroke" }));
  [0, Math.floor((data.length - 1) / 2), data.length - 1].forEach((index, position) => {
    const label = svgNode("text", { x: x(index), y: height - 11, "text-anchor": position === 0 ? "start" : position === 2 ? "end" : "middle", fill: "#a0adbe", "font-size": 10 }); label.textContent = date(data[index].trade_date); svg.append(label);
  });
  const guide = svgNode("line", { x1: 0, x2: 0, y1: top, y2: height - bottom, stroke: "#9cb1db", "stroke-dasharray": "3 3", opacity: 0 });
  const point = svgNode("circle", { cx: 0, cy: 0, r: 3.5, fill: "#fff", stroke: "#557cdf", "stroke-width": 2, opacity: 0 }); svg.append(guide, point);
  target.append(svg);
  const tooltip = node("div", "chart-tooltip hidden"); target.append(tooltip);
  svg.addEventListener("pointermove", (event) => {
    const bounds = svg.getBoundingClientRect();
    const coordinate = (event.clientX - bounds.left) / bounds.width * width;
    const index = Math.max(0, Math.min(data.length - 1, Math.round((coordinate - left) / (width - left - right) * (data.length - 1))));
    guide.setAttribute("x1", x(index)); guide.setAttribute("x2", x(index)); guide.setAttribute("opacity", 1); point.setAttribute("cx", x(index)); point.setAttribute("cy", y(values[index])); point.setAttribute("opacity", 1);
    clean(tooltip); tooltip.append(node("div", "", date(data[index].trade_date)), node("div", "", metric === "drawdown" ? `回撤 ${pct(values[index], 2)}` : `归一化净值 ${number(values[index], 4)}`), node("div", "", `账户金额 ¥${number(data[index].nav, 2)}`)); tooltip.classList.remove("hidden");
    const local = event.clientX - target.getBoundingClientRect().left; tooltip.style.left = `${Math.max(5, Math.min(local + 12, target.clientWidth - tooltip.offsetWidth - 6))}px`; tooltip.style.top = "13px";
  });
  svg.addEventListener("pointerleave", () => { guide.setAttribute("opacity", 0); point.setAttribute("opacity", 0); tooltip.classList.add("hidden"); });
}

function budgets(config = {}) { return config.budgets || {}; }
function renderAllocation(target, config, full = false) {
  clean(target);
  const settings = [
    { key: "stock", color: "stock", title: "个股多因子", intro: "质量、价值、动量与低波动共同选股，按月调整。", rules: [["更新频率", "每月"], ["目标数量", "25只"], ["因子权重", "35 / 25 / 25 / 15"], ["规则", "质量 / 价值 / 动量 / 低波"]] },
    { key: "etf_rotation", color: "etf", title: "ETF轮动", intro: "以趋势与动量筛选ETF，按波动率分配风险。", rules: [["更新频率", "每周"], ["候选入选", "前3只"], ["趋势条件", "60日均线"], ["动量窗口", "20日 / 60日"]] },
    { key: "defensive", color: "defensive", title: "防御配置", intro: "现金与国债ETF作为缓冲，配合统一组合风控。", rules: [["资产范围", "现金 / 国债ETF"], ["国债ETF预算", "防御预算的60%"], ["空闲预算", "保留现金"], ["杠杆", "无杠杆"]] }
  ];
  settings.forEach((item, index) => {
    const budget = budgets(config)[item.key];
    if (full) {
      const card = node("article", `strategy-card ${item.key}`); card.append(node("div", "strategy-number", `STRATEGY 0${index + 1}`), node("h2", "", item.title));
      const amount = node("div", "budget", pct(budget, 0)); amount.append(node("span", "", "研究预算")); card.append(amount, node("p", "intro", item.intro));
      const rules = node("div", "strategy-rules"); item.rules.forEach(([label, value]) => { const row = node("div", "strategy-rule"); row.append(node("span", "", label), node("strong", "", value)); rules.append(row); }); card.append(rules); target.append(card);
    } else {
      const module = node("div", "allocation-module"); const heading = node("div", "allocation-title"); const title = node("span"); title.append(node("i", `allocation-dot ${item.color}-color`), node("span", "", item.title)); heading.append(title, node("strong", "", pct(budget, 0))); module.append(heading);
      const bar = node("div", "allocation-bar"); const fill = node("i", `${item.color}-color`); fill.style.width = validNumber(budget) ? `${Math.max(0, Math.min(100, Number(budget) * 100))}%` : "0%"; bar.append(fill); module.append(bar, node("div", "allocation-description", item.intro)); target.append(module);
    }
  });
}

async function loadOverview() {
  empty($("#overview-chart"), "正在读取研究概览…", null, true);
  try {
    const [overview, config] = await Promise.all([api("/api/overview"), api("/api/config")]);
    state.overview = overview; state.config = config;
    renderOverview(overview, config);
    if (overview.latest_run && overview.latest_run.run_id && overview.latest_run.valid !== false) {
      const run = await api(`/api/runs/${encodeURIComponent(overview.latest_run.run_id)}`);
      state.overviewRun = run; renderOverviewChart();
    } else { state.overviewRun = null; empty($("#overview-chart"), "暂无合成演示记录。点击“运行工程演示”检查系统链路。"); $("#overview-chart-caption").textContent = "真实历史策略效果尚未验证。"; }
  } catch (error) { empty($("#overview-chart"), error.message, loadOverview); message(`概览读取失败：${error.message}`, true, loadOverview); }
}
function renderOverview(overview, config) {
  const datasets = collection(overview.datasets);
  const stock = datasets.find((item) => item.name === "stock_daily" || /stock.*daily|股票日线|A股日线/i.test(item.name || ""));
  const etf = datasets.find((item) => item.name === "etf_daily" || /etf/i.test(item.name || ""));
  const retired = overview.retired_prices || overview.coverage || {};
  const missing = count(retired.missing_codes ?? retired.retired_missing_count);
  const latest = overview.latest_run;
  const range = overview.stock_date_range || overview.coverage?.stock_date_range || {};
  clean($("#overview-kpis")).append(
    kpi("A股日线覆盖", countLabel(stock?.file_count), "只", stock?.rows !== undefined ? `${compactRows(stock.rows)}万条日线 · ${date(range.end || stock.cutoff)}截止` : "等待来源确认", "▥"),
    kpi("ETF行情样本", countLabel(etf?.file_count), "只", etf?.cutoff ? `快照截止 ${date(etf.cutoff)}` : "按来源分别核验", "↗"),
    kpi("退市行情待补", countLabel(missing), "个代码", retired.research_window ? `${date(Array.isArray(retired.research_window) ? retired.research_window[0] : retired.research_window.start)}起研究区间` : "已取得退市来源中的行情缺口", "◌"),
    kpi("最新工程演示", countLabel(latest?.evaluation_sessions), "交易日", latest ? `${number(latest.orders)}笔意向 · ${number(latest.fills)}笔模拟成交` : "尚未运行合成验证", "⌁")
  );
  const admitted = overview.historical_data_admitted === true;
  $("#admission-tag").textContent = admitted ? "已准入" : overview.admission?.status === "unavailable" ? "信息不可用" : "等待补全";
  $("#admission-tag").className = `tag ${admitted ? "teal" : "amber"}`;
  $("#admission-title").textContent = admitted ? "研究数据已通过准入" : "先完善数据，再验证策略";
  $("#admission-description").textContent = admitted ? "请按已核验的来源范围与交易条件开展研究。" : "当前真实数据仍有覆盖和历史状态缺口，完整历史回测准入保持关闭。";
  // 准入由 §5.1/§5.2 的计算判据合取决定，计算判据优先于名词式缺口清单展示。
  const failedCriteria = datasets.flatMap((item) => collection(item.criteria)
    .filter((criterion) => criterion && criterion.passed === false)
    .map((criterion) => `${datasetNames[item.name] || item.name}｜${criterion.criterion}：${criterion.detail}`));
  const gaps = [...new Set([...failedCriteria, ...collection(overview.history_gaps), ...datasets.flatMap((item) => collection(item.gaps || item.readiness_gaps))].map(humanGap))];
  if (missing > 0 && !gaps.some((item) => item.includes("退市"))) gaps.unshift("退市证券与历史股票池覆盖待补全");
  clean($("#admission-gaps"));
  (gaps.length ? gaps.slice(0, 3) : [overview.admission?.reason ? humanGap(overview.admission.reason) : "核验结果以归档来源为准"]).forEach((gap) => $("#admission-gaps").append(node("div", "gap-item", gap)));
  renderAllocation($("#overview-allocation"), config);
  table($("#overview-datasets"), [
    { label: "数据集", className: "table-primary", render: (item) => datasetNames[item.name] || item.name },
    { label: "归档文件", render: (item) => number(item.file_count) },
    { label: "记录数", render: (item) => number(item.rows) },
    { label: "快照截止", render: (item) => date(item.cutoff || item.as_of) },
    { label: "研究状态", render: (item) => tag(item.error ? "来源异常" : item.ready === true ? "已准入" : "待核验", item.ready === true ? "teal" : "amber") }
  ], datasets, "没有已登记的数据集。请检查本地数据登记。");
  const project = typeof overview.project === "object" ? overview.project : {};
  $("#system-version").textContent = project.system_version || overview.system_version ? `系统 v${project.system_version || overview.system_version}` : "本地研究环境";
  const footer = $("#footer-status"); clean(footer); footer.append(node("i"), node("span", "", `真实历史回测准入：${admitted ? "已通过" : "待补全与核验"}`));
  audit($("#overview-audit"), { admission: overview.admission, warnings: overview.warnings, reports: overview.reports, datasets: overview.datasets }, collection(overview.warnings).map((warning) => `来源提示：${warning}`));
}
function renderOverviewChart() {
  const run = state.overviewRun;
  if (!run) return;
  chart($("#overview-chart"), collection(run.equity), state.metric, "overview");
  const summary = run.summary || {};
  const caption = clean($("#overview-chart-caption"));
  caption.append(node("span", "", `${date(summary.start_date || run.equity?.[0]?.trade_date)} — ${date(summary.end_date || run.equity?.at(-1)?.trade_date)}`), node("span", "", "仅验证账户与决策链路"));
}

async function loadConfig() {
  try {
    const config = await api("/api/config"); state.config = config;
    renderAllocation($("#strategy-allocation"), config, true);
    const risk = config.risk || {};
    const riskFields = [["单一个股上限", "stock_cap"], ["单只ETF上限", "etf_cap"], ["权益总仓位上限", "equity_cap"], ["行业穿透上限", "industry_cap"], ["最低现金比例", "min_cash"], ["年化波动研究目标", "target_annual_vol"]];
    clean($("#risk-config")); riskFields.forEach(([label, key]) => { const rule = node("div"); rule.append(node("div", "rule-label", label), node("div", "rule-value", pct(risk[key], 0))); $("#risk-config").append(rule); });
    const fees = config.fees || {};
    clean($("#fee-config")); [["佣金率", pct(fees.commission_rate, 2)], ["最低佣金", validNumber(fees.minimum_commission) ? `¥${number(fees.minimum_commission, 2)}` : "—"], ["股票卖出税率", pct(fees.stock_sell_tax_rate, 2)], ["滑点假设", validNumber(fees.slippage_bps) ? `${number(fees.slippage_bps)}基点` : "—"]].forEach(([label, value]) => { const row = node("div", "definition-row"); row.append(node("span", "", label), node("strong", "", value)); $("#fee-config").append(row); });
    audit($("#strategy-assumptions"), config, ["当前配置用于合成数据工程演示。收盘生成目标，次日按可用执行条件模拟成交。", "回撤研究目标为15%–20%，属于设计目标，实际回撤可能超过该范围。"]);
  } catch (error) { message(`配置读取失败：${error.message}`, true, loadConfig); empty($("#strategy-allocation"), "无法读取当前配置", loadConfig); }
}

function clearBars(message) {
  ++state.barsToken;
  $("#data-symbol-title").textContent = "选择证券";
  $("#data-symbol-description").textContent = "查看日线价格与成交量";
  $("#data-readiness").textContent = "行情展示仅用于数据检查，真实历史回测准入待核验。";
  $("#data-bars-caption").textContent = "";
  empty($("#data-bars-chart"), message);
  empty($("#data-bars-table"), message);
  clean($("#data-audit"));
}
async function loadCatalog() {
  const token = ++state.dataToken;
  const query = $("#symbol-search").value.trim();
  const dataset = $("#dataset-select").value; state.dataset = dataset;
  clearBars("请从当前目录选择证券。");
  empty($("#symbol-list"), "正在读取…", null, true);
  try {
    const result = await api(`/api/data/catalog?dataset=${encodeURIComponent(dataset)}&q=${encodeURIComponent(query)}&limit=50`);
    if (token !== state.dataToken) return;
    const items = collection(result.items);
    $("#catalog-count").textContent = `匹配 ${number(result.total)}个证券${items.length < Number(result.total) ? " · 展示前50个" : ""}`;
    clean($("#symbol-list"));
    if (!items.length) { state.symbol = ""; empty($("#symbol-list"), "未找到匹配的证券。"); clearBars("未找到匹配的证券，请调整搜索条件。"); return; }
    items.forEach((item) => {
      const button = node("button", "symbol-row", ""); button.type = "button"; button.dataset.symbol = String(item.symbol); button.append(node("span", "", item.symbol), node("small", "", `${number(item.rows)}日线`)); button.title = `${item.symbol} · ${date(item.start)}至${date(item.end)}`; button.addEventListener("click", () => loadBars(item.symbol)); $("#symbol-list").append(button);
    });
    const selected = items.find((item) => String(item.symbol) === state.symbol) || items[0];
    await loadBars(selected.symbol);
  } catch (error) { if (token !== state.dataToken) return; $("#catalog-count").textContent = "证券目录读取失败"; empty($("#symbol-list"), error.message, loadCatalog); clearBars("证券目录读取失败，请重试。"); }
}
async function loadBars(symbol) {
  const token = ++state.barsToken;
  state.symbol = String(symbol);
  $$(".symbol-row").forEach((button) => { const selected = button.dataset.symbol === state.symbol; button.classList.toggle("selected", selected); button.setAttribute("aria-pressed", selected); });
  $("#data-symbol-title").textContent = state.symbol;
  $("#data-symbol-description").textContent = `${datasetNames[state.dataset] || state.dataset} · 读取日线中`;
  $("#data-bars-caption").textContent = ""; clean($("#data-audit"));
  empty($("#data-bars-chart"), "正在读取日线…", null, true); empty($("#data-bars-table"), "正在读取明细…", null, true);
  try {
    const result = await api(`/api/data/bars?dataset=${encodeURIComponent(state.dataset)}&symbol=${encodeURIComponent(state.symbol)}&limit=200`);
    if (token !== state.barsToken) return;
    const rows = collection(result.rows).sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)));
    $("#data-symbol-description").textContent = `${datasetNames[state.dataset] || state.dataset} · ${date(rows[0]?.trade_date)} — ${date(rows.at(-1)?.trade_date)}`;
    $("#data-readiness").textContent = result.backtest_ready === true ? "请按已核验的字段与历史范围开展研究。" : "未复权行情仅用于数据检查。历史复权、可交易状态和字段可用时间仍需核验；此页不生成策略信号。";
    candleChart($("#data-bars-chart"), rows.slice(-60));
    $("#data-bars-caption").textContent = rows.length ? `图示最近${Math.min(rows.length, 60)}个交易日 · 日线明细展示已读取的${rows.length}条记录 · 红涨绿跌` : "当前证券没有可显示的日线。";
    table($("#data-bars-table"), [
      { label: "交易日期", className: "table-primary", render: (item) => date(item.trade_date) },
      { label: "开盘", render: (item) => number(item.open, 3) }, { label: "最高", render: (item) => number(item.high, 3) },
      { label: "最低", render: (item) => number(item.low, 3) }, { label: "收盘", render: (item) => number(item.close, 3) },
      { label: "原始成交量", render: (item) => number(item.volume_raw ?? item.volume) },
      { label: "复权价格", render: (item) => item.adjusted_close === null || item.adjusted_close === undefined ? "待核验" : number(item.adjusted_close, 3) },
      { label: "可交易状态", render: (item) => item.tradable === true ? "来源已标记" : item.tradable === false ? "不可交易" : "未知" }
    ], rows.slice().reverse(), "没有日线记录。");
    audit($("#data-audit"), { dataset: result.dataset, symbol: result.symbol, price_basis: result.price_basis, backtest_ready: result.backtest_ready, readiness_gaps: result.readiness_gaps, source: result.source, manifest: result.manifest }, ["原始成交量保留来源值；成交股数估计不作为已核验的历史口径。缺失的复权、可交易状态与字段可用时间保持未知。"]);
  } catch (error) { if (token !== state.barsToken) return; empty($("#data-bars-chart"), error.message, () => loadBars(symbol)); empty($("#data-bars-table"), "日线读取失败。"); $("#data-symbol-description").textContent = "读取失败，请重试"; }
}
function candleChart(target, rows) {
  clean(target);
  const data = rows.filter((item) => [item.open, item.high, item.low, item.close].every(validNumber));
  if (!data.length) { empty(target, "没有可绘制的完整OHLC记录。"); return; }
  const width = 820, height = 280, left = 55, right = 14, top = 24, bottom = 32;
  let min = Math.min(...data.map((item) => Number(item.low))), max = Math.max(...data.map((item) => Number(item.high)));
  const pad = (max - min || max * .01 || 1) * .1; min -= pad; max += pad;
  const y = (value) => top + (max - Number(value)) / (max - min) * (height - top - bottom);
  const step = (width - left - right) / data.length;
  const x = (index) => left + (index + .5) * step;
  const svg = svgNode("svg", { viewBox: `0 0 ${width} ${height}`, preserveAspectRatio: "none", role: "img", "aria-label": "真实未复权行情开高低收价格图，红涨绿跌" });
  for (let i = 0; i < 5; i++) { const value = min + i / 4 * (max - min); svg.append(svgNode("line", { x1: left, x2: width - right, y1: y(value), y2: y(value), stroke: "#edf1f6", "stroke-dasharray": "3 4" })); const label = svgNode("text", { x: left - 10, y: y(value) + 3, fill: "#9ba9bd", "font-size": 10, "text-anchor": "end" }); label.textContent = number(value, 2); svg.append(label); }
  data.forEach((item, index) => { const color = Number(item.close) >= Number(item.open) ? "#bf777f" : "#5eaba0"; svg.append(svgNode("line", { x1: x(index), x2: x(index), y1: y(item.high), y2: y(item.low), stroke: color, "stroke-width": 1 })); svg.append(svgNode("rect", { x: x(index) - Math.max(2, step * .5) / 2, y: Math.min(y(item.close), y(item.open)), width: Math.max(2, step * .5), height: Math.max(1, Math.abs(y(item.close) - y(item.open))), fill: color, rx: .6 })); });
  [0, Math.floor((data.length - 1) / 2), data.length - 1].forEach((index, position) => { const label = svgNode("text", { x: x(index), y: height - 9, fill: "#9ba9bd", "font-size": 10, "text-anchor": position === 0 ? "start" : position === 2 ? "end" : "middle" }); label.textContent = date(data[index].trade_date); svg.append(label); });
  const guide = svgNode("line", { x1: 0, x2: 0, y1: top, y2: height - bottom, stroke: "#aebbd0", "stroke-dasharray": "3 3", opacity: 0 }); svg.append(guide); target.append(svg);
  const tooltip = node("div", "chart-tooltip hidden"); target.append(tooltip);
  svg.addEventListener("pointermove", (event) => { const bounds = svg.getBoundingClientRect(); const local = (event.clientX - bounds.left) / bounds.width * width; const index = Math.max(0, Math.min(data.length - 1, Math.floor((local - left) / step))); const item = data[index]; guide.setAttribute("x1", x(index)); guide.setAttribute("x2", x(index)); guide.setAttribute("opacity", 1); clean(tooltip); tooltip.append(node("div", "", date(item.trade_date)), node("div", "", `开 ${number(item.open, 3)}  高 ${number(item.high, 3)}`), node("div", "", `低 ${number(item.low, 3)}  收 ${number(item.close, 3)}`)); tooltip.classList.remove("hidden"); tooltip.style.left = `${Math.max(5, Math.min(event.clientX - target.getBoundingClientRect().left + 12, target.clientWidth - tooltip.offsetWidth - 6))}px`; tooltip.style.top = "12px"; });
  svg.addEventListener("pointerleave", () => { tooltip.classList.add("hidden"); guide.setAttribute("opacity", 0); });
}

async function loadRuns(preserve = true) {
  try {
    const result = await api("/api/runs"); state.runs = collection(result.items);
    const select = clean($("#run-select"));
    if (!state.runs.length) { select.append(node("option", "", "暂无运行记录")); state.run = null; state.runId = ""; clearRun("暂无工程演示。点击“运行工程演示”创建首条记录。"); return; }
    state.runs.forEach((run) => { const option = node("option", "", `${date(run.generated_at)} · ${run.run_id || "不可读记录"}${run.valid === false ? " · 记录异常" : " · 合成工程验证"}`); option.value = run.run_id || ""; option.disabled = !run.run_id; select.append(option); });
    const selected = preserve && state.runs.some((run) => run.run_id === state.runId) ? state.runId : state.runs[0].run_id;
    select.value = selected;
    await loadRun(selected);
  } catch (error) { message(`运行记录读取失败：${error.message}`, true, () => loadRuns()); $("#run-select").replaceChildren(node("option", "", "读取失败")); clearRun("运行记录读取失败，请重试。"); }
}
function clearRun(reason) {
  clean($("#run-kpis"));
  ["#run-chart", "#run-holdings", "#run-decision", "#run-log"].forEach((selector) => empty($(selector), reason));
  $("#run-date-range").textContent = ""; $("#log-page-description").textContent = "暂无记录";
  $("#log-prev").disabled = true; $("#log-next").disabled = true;
  clean($("#run-audit"));
}
async function loadRun(runId) {
  const token = ++state.runToken; state.runId = runId; state.logOffset = 0;
  empty($("#run-chart"), "正在读取运行记录…", null, true);
  try {
    const run = await api(`/api/runs/${encodeURIComponent(runId)}`);
    if (token !== state.runToken) return;
    state.run = run;
    const summary = run.summary || {}; const manifest = run.manifest || {};
    $("#run-badge").textContent = "合成数据 · 工程验证";
    clean($("#run-kpis")).append(
      kpi("模拟账户金额", validNumber(summary.final_nav) ? number(summary.final_nav, 2) : "—", "元", "用于检查账户账本，非真实业绩"),
      kpi("执行交易日", number(manifest.evaluation_sessions ?? run.equity?.length), "日", `${date(summary.start_date)} — ${date(summary.end_date)}`),
      kpi("模拟成交", number(manifest.fills), "笔", `${number(manifest.orders)}笔订单意向`),
      kpi("最终可用现金", number(summary.final_cash, 2), "元", `${number(summary.holding_count)}个模拟持仓`)
    );
    chart($("#run-chart"), collection(run.equity), "nav", "run");
    $("#run-date-range").textContent = `${date(summary.start_date)} — ${date(summary.end_date)}`;
    const holdings = run.final_holdings || run.equity?.at(-1)?.holdings || {};
    const holdingRows = Array.isArray(holdings) ? holdings : Object.entries(holdings).map(([symbol, item]) => typeof item === "object" && item !== null ? { symbol, ...item } : { symbol, quantity: item });
    table($("#run-holdings"), [{ label: "证券代码", key: "symbol", className: "table-primary" }, { label: "模拟数量", render: (item) => number(item.quantity ?? item.shares) }], holdingRows, "最终账户没有持仓。");
    renderDecision(run.latest_decision);
    audit($("#run-audit"), { manifest, config: run.config }, ["所有证券、行情、因子与执行条件均为合成设定。该运行仅用于工程验证。"]);
    await loadLog();
  } catch (error) { if (token !== state.runToken) return; empty($("#run-chart"), error.message, () => loadRun(runId)); clean($("#run-kpis")); empty($("#run-holdings"), "当前记录不可读取。"); empty($("#run-decision"), "当前记录不可读取。"); empty($("#run-log"), "请选择有效的运行记录。"); message(error.message, true, () => loadRun(runId)); }
}
function renderDecision(decision) {
  const target = clean($("#run-decision"));
  if (!decision) { empty(target, "暂无风险决策记录。"); return; }
  const metrics = node("div", "decision-metrics");
  [["风险缩放", validNumber(decision.risk_scale) ? number(decision.risk_scale, 2) : "—"], ["当前回撤", pct(decision.drawdown, 2)], ["风险锁定", decision.locked === true ? "已锁定" : decision.locked === false ? "未锁定" : "未知"]].forEach(([label, value]) => { const metric = node("div"); metric.append(node("small", "", label), node("strong", "", value)); metrics.append(metric); }); target.append(metrics);
  target.append(node("div", "", `决策日期：${date(decision.decision_at)}`));
  const reasons = collection(decision.reasons);
  if (reasons.length) { const list = node("ul"); reasons.slice(0, 4).forEach((reason) => list.append(node("li", "", reasonLabel(reason)))); target.append(list); } else target.append(node("div", "", "本次决策未记录额外限制原因。"));
  const violations = collection(decision.actual_limit_violations);
  if (violations.length) target.append(node("div", "negative", `实际持仓约束触发 ${number(violations.length)}项，详情见策略决策记录。`));
}
const reasonLabels = { drawdown_lock_requires_separate_research_decision: "回撤锁定，需独立研究判断后恢复", equity_cap: "目标权益仓位超过上限，已缩减", industry_cap: "目标行业暴露超过上限，已缩减", minimum_available_cash: "现金比例约束", volatility_scaling: "按波动率缩减风险", fixture_calendar_ended: "合成日历已结束", insufficient_cash: "可用现金不足", security_not_tradable: "模拟证券当前不可交易", drawdown_scaling: "按回撤阶段缩减风险", unknown_actual_industry_exposure_blocks_equity_additions: "行业暴露未知，暂停增加权益仓位", order_already_attempted: "订单已执行过", quantity_must_be_integer: "数量须为整数", execution_rules_unverified: "执行规则尚未核验", missing_execution_bar: "缺少执行当日行情", wrong_execution_session: "行情与执行交易日不符", invalid_buy_lot: "买入数量不符合交易单位", insufficient_settled_quantity: "已结算可卖数量不足", invalid_partial_sell_lot: "部分卖出数量不符合交易单位", nonpositive_slipped_price: "模拟执行价格无效", insufficient_cash_for_one_lot_and_fees: "现金不足以支付一个交易单位与费用", insufficient_cash_for_sale_fees: "现金不足以支付卖出费用", executed: "按规则执行完成", cash_limited: "现金约束导致部分成交" };
function reasonLabel(reason) { return reasonLabels[reason] || (/^[a-z_]+$/.test(String(reason)) ? "规则限制（详见原始决策记录）" : readable(reason)); }
function detailsCell(item, label = "查看记录") { const wrap = node("details"); wrap.append(node("summary", "", label), node("pre", "", JSON.stringify(item, null, 2))); return wrap; }
function side(value) { return value === "buy" ? "买入" : value === "sell" ? "卖出" : readable(value); }
function eventLabel(value) { return ({ rejected: "已拒绝", filled: "已成交", partial: "部分成交", partially_filled: "部分成交", accrued: "应收分红确认", paid: "分红现金到账" })[value] || readable(value); }
function logColumns(kind) {
  if (kind === "orders") return [{ label: "执行日期", render: (item) => date(item.execute_on) }, { label: "证券代码", key: "symbol", className: "table-primary" }, { label: "方向", render: (item) => side(item.side) }, { label: "数量", render: (item) => number(item.quantity) }, { label: "参考价格", render: (item) => number(item.reference_price, 3) }, { label: "收盘决策", render: (item) => date(item.decision_at) }];
  if (kind === "fills") return [{ label: "成交日期", render: (item) => date(item.trade_date) }, { label: "证券代码", key: "symbol", className: "table-primary" }, { label: "方向", render: (item) => side(item.side) }, { label: "数量", render: (item) => number(item.quantity) }, { label: "模拟价格", render: (item) => number(item.price, 3) }, { label: "金额", render: (item) => number(item.notional, 2) }, { label: "费用", render: (item) => number(Number(item.commission || 0) + Number(item.tax || 0), 2) }];
  if (kind === "decisions") return [{ label: "决策日期", render: (item) => date(item.decision_at) }, { label: "更新策略", render: (item) => collection(item.refreshed_strategies).map((name) => strategyNames[name] || name).join(" / ") || "延续目标" }, { label: "风险缩放", render: (item) => number(item.risk_scale, 3) }, { label: "回撤", render: (item) => pct(item.drawdown, 2) }, { label: "实际约束触发", render: (item) => number(collection(item.actual_limit_violations).length) }, { label: "决策详情", className: "log-details", render: (item) => detailsCell(item) }];
  return [{ label: "日期", render: (item) => date(item.trade_date || item.event_date || item.date || item.execute_on || item.pay_date || item.ex_date) }, { label: "证券代码", render: (item) => item.symbol || "—", className: "table-primary" }, { label: kind === "dividend_events" ? "分红事件" : "执行结果", render: (item) => eventLabel(item.status || item.event || item.type || "事件记录") }, { label: "原因", render: (item) => reasonLabel(item.reason || "—") }, { label: "事件详情", className: "log-details", render: (item) => detailsCell(item) }];
}
async function loadLog() {
  if (!state.runId) return;
  const token = ++state.logToken;
  const runId = state.runId, kind = state.logKind, offset = state.logOffset;
  empty($("#run-log"), "正在读取记录…", null, true);
  $("#log-prev").disabled = true; $("#log-next").disabled = true;
  try {
    const result = await api(`/api/runs/${encodeURIComponent(runId)}/${kind}?offset=${offset}&limit=30`);
    if (token !== state.logToken || runId !== state.runId) return;
    const items = collection(result.items); state.logTotal = Number(result.total) || 0;
    table($("#run-log"), logColumns(kind), items, "本次运行没有此类记录。");
    $("#log-page-description").textContent = state.logTotal ? `第 ${number(offset + 1)}–${number(offset + items.length)}条，共${number(result.total)}条` : "共0条记录";
    $("#log-prev").disabled = offset <= 0; $("#log-next").disabled = offset + items.length >= state.logTotal;
  } catch (error) { if (token !== state.logToken) return; empty($("#run-log"), error.message, loadLog); $("#log-page-description").textContent = "读取失败"; }
}

async function runDemo() {
  if (state.demoBusy) return;
  state.demoBusy = true;
  $$(".demo-button").forEach((button) => { button.disabled = true; button.textContent = "正在运行合成演示…"; });
  message("正在运行合成数据工程演示，结果用于检查决策、风控与账户链路。");
  try {
    const result = await api("/api/demo", { method: "POST" });
    message(`工程演示已完成：${number(result.evaluation_sessions)}个合成交易日，${number(result.orders)}笔订单意向，${number(result.fills)}笔模拟成交。`);
    if (result.run_id) state.runId = result.run_id;
    if (state.page === "runs") await loadRuns(true); else if (state.page === "overview") await loadOverview();
  } catch (error) { message(`工程演示未完成：${error.message}`, true, runDemo); }
  finally { state.demoBusy = false; $$(".demo-button").forEach((button) => { button.disabled = false; clean(button); button.append(node("span", "", "▷"), document.createTextNode(" 运行工程演示")); }); }
}
function activatePage() {
  const name = location.hash.slice(1);
  state.page = Object.hasOwn(pageNames, name) ? name : "overview";
  $$(".page").forEach((page) => page.classList.toggle("hidden", page.id !== `page-${state.page}`));
  $$(".nav-item").forEach((item) => { const active = item.dataset.page === state.page; item.classList.toggle("active", active); if (active) item.setAttribute("aria-current", "page"); else item.removeAttribute("aria-current"); });
  $("#breadcrumb-page").textContent = pageNames[state.page]; document.title = `${pageNames[state.page]} · 混合量化`;
  hideMessage(); refreshPage();
}
function refreshPage() {
  if (state.page === "overview") loadOverview();
  else if (state.page === "data") loadCatalog();
  else if (state.page === "strategy") loadConfig();
  else loadRuns();
}
window.addEventListener("hashchange", activatePage);
$("#refresh-button").addEventListener("click", refreshPage);
$$(".demo-button").forEach((button) => button.addEventListener("click", runDemo));
$$("[data-chart]").forEach((button) => button.addEventListener("click", () => { state.metric = button.dataset.chart; $$("[data-chart]").forEach((item) => { const selected = item.dataset.chart === state.metric; item.classList.toggle("selected", selected); item.setAttribute("aria-pressed", selected); }); renderOverviewChart(); }));
$("#dataset-select").addEventListener("change", () => { state.symbol = ""; ++state.barsToken; loadCatalog(); });
let searchTimeout;
$("#symbol-search").addEventListener("input", () => { clearTimeout(searchTimeout); searchTimeout = setTimeout(loadCatalog, 250); });
$("#run-select").addEventListener("change", (event) => { hideMessage(); loadRun(event.target.value); });
$$("[data-log]").forEach((button) => button.addEventListener("click", () => { state.logKind = button.dataset.log; state.logOffset = 0; $$("[data-log]").forEach((item) => { const selected = item.dataset.log === state.logKind; item.classList.toggle("selected", selected); item.setAttribute("aria-selected", selected); }); loadLog(); }));
$("#log-prev").addEventListener("click", () => { state.logOffset = Math.max(0, state.logOffset - 30); loadLog(); });
$("#log-next").addEventListener("click", () => { state.logOffset += 30; loadLog(); });
activatePage();
