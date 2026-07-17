/* report.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// ============ 导入 / 导出 / 报告 ============
const REPORT_VERSION = '20260708';
const CHART_JS_CDN = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js';
const TAILWIND_CDN = 'https://cdn.tailwindcss.com';

// 策略比较报告导出用的图表标题 / 说明文案（与 strategy.js 的 setScYMode 保持一致）
const SC_XIRR_TITLE = '投资收益率曲线（资金加权 XIRR 年化，%）';
const SC_NET_TITLE = '投资净值曲线（资金加权，起点=1.0）';
const SC_XIRR_NOTE = '* 采用资金加权的 XIRR 年化收益率（与策略对比表"XIRR年化"同口径，曲线终点=该值）：每日净外部现金流 = −当日投入 + 当日落袋现金（赎回/现金分红），期末价值只计剩余持仓市值，已落袋现金此前已作正流避免重复计数。该口径不受持续定投稀释，能直接看到资金加权年化随时间的真实走势；止盈赎回作为正流计入，曲线不会因赎回而假跌。可切换"按持有期月数对齐"或"按日期"调整 x 轴。';
const SC_NET_NOTE = '* 采用资金加权的"这一笔投资净值"（起点=1.0）：当日净值 = 当日总资产 ÷ 截至当日的累计已投入本金。该口径保留了投入节奏的影响——上涨市中单笔（一次投入）会跑在定投（分批投入）上方，能直接看出策略差异；可切换"按持有期月数对齐"（各条目从0月对齐便于跨条目对比）或"按日期"（按真实日历日期错开显示），并可在"净值 / XIRR年化"间切换 y 轴口径。';

// 序列化 / 反序列化 fundsData（Date <-> yyyy-mm-dd）
function serializeFunds(fd) {
    const out = {};
    for (const k in fd) {
        const f = fd[k];
        out[k] = {
            dates: f.dates.map(function (d) { return formatDate(d); }),
            nav: f.nav, div: f.div,
            minDate: f.minDate, maxDate: f.maxDate
        };
    }
    return out;
}
function deserializeFunds(obj) {
    const out = {};
    for (const k in obj) {
        const f = obj[k];
        out[k] = {
            dates: f.dates.map(function (s) { return new Date(s + 'T00:00:00'); }),
            nav: f.nav, div: f.div,
            minDate: f.minDate, maxDate: f.maxDate
        };
    }
    return out;
}

// 导出项目（打包全部基准）
async function exportProject() {
    const benchmarks = await db.benchmarks.toArray();
    const snap = {
        version: REPORT_VERSION,
        type: 'dca-backtest-project',
        exportedAt: new Date().toISOString(),
        fundsData: serializeFunds(fundsData),
        investmentPlans: investmentPlans,
        compositeWeights: compositeWeights,
        fillMissingNav: fillMissingNav,
        currentBenchmarkId: currentBenchmarkId,
        benchmarks: benchmarks
    };
    const blob = new Blob([JSON.stringify(snap, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = '回测项目_' + formatDate(new Date()) + '.json';
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
    alert('项目已导出（含全部基准数据）');
}

// 导入项目（基准 id 重映射 + 自动回测）
async function importProject(file) {
    let snap;
    try { snap = JSON.parse(await file.text()); }
    catch (e) { alert('文件解析失败，请确认是导出的项目 JSON'); return; }
    if (!snap || typeof snap !== 'object') { alert('文件格式不正确'); return; }

    fundsData = snap.fundsData ? deserializeFunds(snap.fundsData) : {};
    investmentPlans = Array.isArray(snap.investmentPlans) ? snap.investmentPlans : [];
    compositeWeights = snap.compositeWeights || {};
    fillMissingNav = !!snap.fillMissingNav;

    await db.benchmarks.clear();
    const idMap = {};
    if (Array.isArray(snap.benchmarks)) {
        for (const b of snap.benchmarks) {
            const rest = {};
            for (const k in b) { if (k !== 'id') rest[k] = b[k]; }
            idMap[b.id] = await db.benchmarks.add(rest);
        }
    }
    currentBenchmarkId = snap.currentBenchmarkId != null ? idMap[snap.currentBenchmarkId] : null;

    if (Object.keys(fundsData).length) {
        const pls = document.getElementById('planListSection'); if (pls) pls.style.display = 'block';
        const rs = document.getElementById('resultSection'); if (rs) rs.style.display = 'block';
    }
    renderPlanList();
    await loadBenchmarkList();
    if (investmentPlans.length > 0) runBacktest();
    alert('项目导入完成，已自动运行回测');
}

// 报告内嵌脚本（在报告页内独立运行，复用主页算法）
function buildReportInner() {
    const RD = window.__RD__;
    let netValueChart = null, assetChart = null;
    let curBmId = RD.currentBenchmarkId;
    function formatDate(d) {
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, '0');
        const day = String(d.getDate()).padStart(2, '0');
        return y + '-' + m + '-' + day;
    }
    function getBenchmarkById(id) {
        return RD.benchmarksData.find(function (b) { return b.id === id; }) || null;
    }
    const RISK_FREE_RATE = 0.025;
    const MIN_TRADE_DAYS = 30;

    // ============ 指标卡悬停解释（报告内复刻主工具 tooltip） ============
    const METRIC_TIPS = {
        "总投入本金": "回测区间内实际投入的所有资金（定投扣款+一次性买入），不含分红再投。即你的成本基数。",
        "持仓市值": "期末仍持有的份额 × 最新净值，不含已到手现金分红。",
        "累计现金分红": "累计收到的、未再投的现金分红；红利再投模式下为 0。",
        "总资产": "持仓市值 + 累计现金分红，即组合上的全部家当。",
        "累计收益率": "资金加权口径：(总资产 ÷ 本金 − 1)。<br>正数代表整体实际赚钱。",
        "XIRR年化": "把每笔投入/分红/市值都当作现金流，算考虑时间权重的年化内部收益率。<br>早投的钱权重更高，比累计收益率更公平。",
        "年化收益率(时间加权)": "把组合净值序列年化（剔除你的投入节奏），反映“组合本身”的赚钱能力。",
        "胜率(正收益日占比)": "上涨交易日数 ÷ 总交易日数，越高说明日子大多在涨。",
        "最大回撤": "净值从最高点到最低点的最大跌幅(%)，越大代表最坏情况越惨。",
        "回撤持续天数": "最长一次从顶部跌下、再回到新高所经历的天数，越久越磨人。",
        "年化波动率": "日收益波动 × √252，衡量价格颠簸程度，越大越刺激。",
        "夏普/卡玛": "夏普=(年化收益−无风险利率)÷波动率；<br>卡玛=年化收益÷最大回撤。<br>两个都是越高越好。",
        "区间投入本金": "选定图表区间内实际投入的资金（定投+一次性买入），不含分红再投。",
        "区间期末市值": "区间期末仍持有的份额 × 区间末净值，不含已到手现金分红。",
        "区间现金分红": "该区间内新收到的、未再投的现金分红。",
        "区间期末总资产": "区间期末市值 + 区间内累计现金分红。",
        "区间累计收益率": "时间加权口径：区间末净值 ÷ 区间初净值 − 1。<br>注意与上方“累计收益率”(资金加权)口径不同，不要直接横比。",
        "区间XIRR年化": "把区间内每笔投入/分红/期末资产当作现金流，算考虑时间权重的年化收益。",
        "区间年化收益率(时间加权)": "把区间净值序列年化（剔除投入节奏），反映区间窗口内组合本身的赚钱能力。",
        "区间胜率(正收益日占比)": "该区间内上涨交易日数 ÷ 区间交易日数。",
        "风险指标": "本金/市值类指标不在此列，这里只解释下方的回撤、波动率等风险项。"
    };

    function initMetricTooltip() {
        const style = document.createElement('style');
        style.textContent = `
        #metricTip{position:fixed;z-index:9999;max-width:300px;background:#1f2937;color:#f3f4f6;
        font-size:12px;line-height:1.6;padding:8px 11px;border-radius:8px;pointer-events:none;
        box-shadow:0 6px 20px rgba(0,0,0,.28);display:none;white-space:normal;}
        #metricTip b{color:#fbbf24;}`;
        document.head.appendChild(style);

        const tip = document.createElement('div');
        tip.id = 'metricTip';
        document.body.appendChild(tip);

        ['metrics', 'periodMetrics'].forEach(function (id) {
            const box = document.getElementById(id);
            if (!box) return;
            box.addEventListener('mouseover', function (e) {
                const cell = e.target.closest('[data-mkey]');
                if (!cell) { tip.style.display = 'none'; return; }
                const key = cell.dataset.mkey;
                const txt = METRIC_TIPS[key];
                if (!txt) { tip.style.display = 'none'; return; }
                tip.innerHTML = '<b>' + key + '</b><br>' + txt;
                tip.style.display = 'block';
            });
            box.addEventListener('mousemove', function (e) {
                if (tip.style.display === 'block') {
                    let x = e.clientX + 14, y = e.clientY + 14;
                    const r = tip.getBoundingClientRect();
                    if (x + r.width > window.innerWidth) x = e.clientX - r.width - 14;
                    if (y + r.height > window.innerHeight) y = e.clientY - r.height - 14;
                    tip.style.left = x + 'px';
                    tip.style.top = y + 'px';
                }
            });
            box.addEventListener('mouseout', function (e) {
                if (!e.relatedTarget || !e.relatedTarget.closest('[data-mkey]')) tip.style.display = 'none';
            });
        });
    }

    function xirr(cashFlows, dates, guess) {
        guess = guess === undefined ? 0.1 : guess;
        if (cashFlows.length !== dates.length || cashFlows.length < 2) return NaN;
        const paired = cashFlows.map(function (cf, i) { return { cf: cf, date: dates[i] }; });
        paired.sort(function (a, b) { return a.date - b.date; });
        const sortedFlows = paired.map(function (p) { return p.cf; });
        const sortedDates = paired.map(function (p) { return p.date; });
        const maxIterations = 100, tolerance = 1e-6;
        let rate = guess;
        for (let i = 0; i < maxIterations; i++) {
            let npv = 0, npvDerivative = 0;
            const baseDate = sortedDates[0];
            for (let j = 0; j < sortedFlows.length; j++) {
                const days = (sortedDates[j] - baseDate) / (1000 * 60 * 60 * 24);
                const term = Math.pow(1 + rate, days / 365);
                npv += sortedFlows[j] / term;
                npvDerivative -= sortedFlows[j] * days / 365 * Math.pow(1 + rate, days / 365 - 1);
            }
            if (Math.abs(npv) < tolerance) return rate;
            if (Math.abs(npvDerivative) < tolerance) break;
            rate -= npv / npvDerivative;
        }
        return NaN;
    }
    function renderPeriodMetrics() {
        const el = document.getElementById('periodMetrics');
        const labelEl = document.getElementById('periodRangeLabel');
        if (!el || !RD.reportData) return;
        const startInput = document.getElementById('chartStartDate');
        const endInput = document.getElementById('chartEndDate');
        if (!startInput || !endInput || !startInput.value || !endInput.value) return;
        const startStr = startInput.value, endStr = endInput.value;
        const dates = RD.reportData.dates.map(function (s) { return new Date(s + 'T00:00:00'); });
        const startD = new Date(startStr + 'T00:00:00'), endD = new Date(endStr + 'T00:00:00');
        let i0 = -1, i1 = -1;
        for (let i = 0; i < dates.length; i++) {
            if (i0 === -1 && dates[i] >= startD) i0 = i;
            if (dates[i] <= endD) i1 = i;
        }
        if (i0 === -1) i0 = 0;
        if (i1 === -1) i1 = dates.length - 1;
        if (i1 < i0) i1 = i0;
        const nvs = RD.reportData.netValues.slice(i0, i1 + 1);
        const assets = RD.reportData.assets.slice(i0, i1 + 1);
        const invests = (RD.reportData.invests || []).slice(i0, i1 + 1);
        const cashDivs = RD.reportData.cashDivs || [];
        const totalCashSeries = RD.reportData.totalCashSeries || [];
        const wDates = dates.slice(i0, i1 + 1);
        const n = nvs.length;
        const windowDays = (dates[i1] - dates[i0]) / 86400000;

        const intervalPrincipal = invests.reduce(function (a, b) { return a + (b || 0); }, 0);
        const mvEnd = assets[n - 1] - (totalCashSeries[i1] || 0);
        const cashDivInterval = (cashDivs[i1] || 0) - (i0 > 0 ? (cashDivs[i0 - 1] || 0) : 0);
        const totalAssetEnd = assets[n - 1];

        let cumReturn = NaN, xirrValInterval = NaN, annualReturnTwr = NaN, winRate = NaN;
        if (n >= 2 && nvs[0] > 0) {
            cumReturn = nvs[n - 1] / nvs[0] - 1;
            if (windowDays > 0) {
                annualReturnTwr = Math.pow(nvs[n - 1] / nvs[0], 365 / windowDays) - 1;
                const flows = [ -assets[0] ], flowDates = [ dates[i0] ];
                for (let j = i0 + 1; j <= i1; j++) { flows.push(-invests[j - i0]); flowDates.push(dates[j]); }
                flows.push(assets[n - 1]); flowDates.push(dates[i1]);
                xirrValInterval = xirr(flows, flowDates) * 100;
            }
            const dailyReturns = [];
            for (let i = 1; i < n; i++) dailyReturns.push((nvs[i] - nvs[i - 1]) / nvs[i - 1]);
            winRate = dailyReturns.length ? dailyReturns.filter(function (r) { return r > 0; }).length / dailyReturns.length * 100 : NaN;
        }
        const twrHtml = isNaN(annualReturnTwr) ? '-' : (annualReturnTwr * 100).toFixed(2) + '%';
        const winHtml = isNaN(winRate) ? '-' : winRate.toFixed(1) + '%';
        const cumHtml = isNaN(cumReturn) ? '-' : (cumReturn * 100).toFixed(2) + '%';

        let riskHtml;
        if (n >= MIN_TRADE_DAYS) {
            const dailyReturns = [];
            for (let i = 1; i < n; i++) dailyReturns.push((nvs[i] - nvs[i - 1]) / nvs[i - 1]);
            const mean = dailyReturns.reduce(function (a, b) { return a + b; }, 0) / dailyReturns.length;
            const variance = dailyReturns.reduce(function (a, b) { return a + Math.pow(b - mean, 2); }, 0) / dailyReturns.length;
            const annualVolatility = Math.sqrt(variance) * Math.sqrt(252);
            let peak = nvs[0], maxDrawdown = 0;
            for (let k = 0; k < nvs.length; k++) { const v = nvs[k]; if (v > peak) peak = v; const dd = (v - peak) / peak; if (dd < maxDrawdown) maxDrawdown = dd; }
            maxDrawdown *= 100;
            let sharpeRatio = NaN, calmarRatio = NaN;
            if (annualVolatility > 0) sharpeRatio = (annualReturnTwr - RISK_FREE_RATE) / annualVolatility;
            if (maxDrawdown !== 0) calmarRatio = annualReturnTwr / Math.abs(maxDrawdown / 100);
            let peakV = nvs[0], ddFrom = null, maxSpan = 0;
            for (let i = 0; i < n; i++) {
                if (nvs[i] > peakV) { peakV = nvs[i]; ddFrom = null; }
                else if (nvs[i] < peakV) {
                    if (ddFrom === null) ddFrom = i;
                    const span = (wDates[i] - wDates[ddFrom]) / 86400000;
                    if (span > maxSpan) maxSpan = span;
                }
            }
            riskHtml = '<div class="bg-red-50 p-4 rounded-lg text-center" data-mkey="最大回撤"><div class="text-sm text-slate-500">最大回撤</div><div class="text-2xl font-bold text-red-700">' + maxDrawdown.toFixed(2) + '%</div></div>' +
                '<div class="bg-red-50 p-4 rounded-lg text-center" data-mkey="回撤持续天数"><div class="text-sm text-slate-500">回撤持续天数</div><div class="text-2xl font-bold text-red-700">' + maxSpan.toFixed(0) + ' 天</div></div>' +
                '<div class="bg-red-50 p-4 rounded-lg text-center" data-mkey="年化波动率"><div class="text-sm text-slate-500">年化波动率</div><div class="text-2xl font-bold text-red-700">' + (annualVolatility * 100).toFixed(2) + '%</div></div>' +
                '<div class="bg-red-50 p-4 rounded-lg text-center" data-mkey="夏普/卡玛"><div class="text-sm text-slate-500">夏普/卡玛</div><div class="text-2xl font-bold text-red-700">' + (isNaN(sharpeRatio) ? '-' : sharpeRatio.toFixed(2)) + '/' + (isNaN(calmarRatio) ? '-' : calmarRatio.toFixed(2)) + '</div></div>';
        } else {
            riskHtml = '<div class="bg-gray-100 p-4 rounded-lg text-center col-span-4" data-mkey="风险指标"><div class="text-sm text-gray-600">风险指标</div><div class="text-xl font-medium text-gray-500">区间交易日不足' + MIN_TRADE_DAYS + '个，以下指标暂不可用</div></div>';
        }

        el.innerHTML = '' +
            '<div class="bg-sky-50 p-4 rounded-lg text-center" data-mkey="区间投入本金"><div class="text-sm text-slate-500">区间投入本金</div><div class="text-2xl font-bold text-sky-700">' + intervalPrincipal.toFixed(2) + ' 元</div></div>' +
            '<div class="bg-sky-50 p-4 rounded-lg text-center" data-mkey="区间期末市值"><div class="text-sm text-slate-500">区间期末市值</div><div class="text-2xl font-bold text-sky-700">' + mvEnd.toFixed(2) + ' 元</div></div>' +
            '<div class="bg-sky-50 p-4 rounded-lg text-center" data-mkey="区间现金分红"><div class="text-sm text-slate-500">区间现金分红</div><div class="text-2xl font-bold text-sky-700">' + cashDivInterval.toFixed(2) + ' 元</div></div>' +
            '<div class="bg-sky-50 p-4 rounded-lg text-center" data-mkey="区间期末总资产"><div class="text-sm text-slate-500">区间期末总资产</div><div class="text-2xl font-bold text-sky-700">' + totalAssetEnd.toFixed(2) + ' 元</div></div>' +
            '<div class="bg-green-50 p-4 rounded-lg text-center" data-mkey="区间累计收益率"><div class="text-sm text-slate-500">区间累计收益率</div><div class="text-2xl font-bold text-green-700">' + cumHtml + '</div></div>' +
            '<div class="bg-green-50 p-4 rounded-lg text-center" data-mkey="区间XIRR年化"><div class="text-sm text-slate-500">区间XIRR年化</div><div class="text-2xl font-bold text-green-700">' + (isNaN(xirrValInterval) ? '-' : xirrValInterval.toFixed(2) + '%') + '</div></div>' +
            '<div class="bg-green-50 p-4 rounded-lg text-center" data-mkey="区间年化收益率(时间加权)"><div class="text-sm text-slate-500">区间年化收益率(时间加权)</div><div class="text-2xl font-bold text-green-700">' + twrHtml + '</div></div>' +
            '<div class="bg-green-50 p-4 rounded-lg text-center" data-mkey="区间胜率(正收益日占比)"><div class="text-sm text-slate-500">区间胜率(正收益日占比)</div><div class="text-2xl font-bold text-green-700">' + winHtml + '</div></div>' +
            riskHtml;
        if (labelEl) labelEl.textContent = '（' + startStr + ' ~ ' + endStr + '，共 ' + n + ' 个交易日）';
    }
    function updateCharts() {
        const startDate = new Date(document.getElementById('chartStartDate').value + 'T00:00:00');
        const endDate = new Date(document.getElementById('chartEndDate').value + 'T00:00:00');
        const dates = RD.reportData.dates.map(function (s) { return new Date(s + 'T00:00:00'); });
        const filtered = dates.map(function (d, i) {
            return { date: d, asset: RD.reportData.assets[i], nv: RD.reportData.netValues[i] };
        }).filter(function (item) { return item.date >= startDate && item.date <= endDate; });
        const chartDates = filtered.map(function (d) { return formatDate(d.date); });
        const chartAssets = filtered.map(function (d) { return d.asset; });
        const chartNetValues = filtered.map(function (d) { return d.nv; });

        let benchmarkDataset = null;
        if (curBmId && filtered.length > 0) {
            const benchmark = getBenchmarkById(curBmId);
            if (benchmark && benchmark.data.length > 0) {
                const bmMap = new Map(benchmark.data.map(function (d) { return [d.date, d.nav]; }));
                const rawBenchValues = [];
                filtered.forEach(function (f) {
                    const nav = bmMap.get(formatDate(f.date));
                    rawBenchValues.push(nav !== undefined ? nav : null);
                });
                for (let i = 1; i < rawBenchValues.length; i++) {
                    if (rawBenchValues[i] === null) rawBenchValues[i] = rawBenchValues[i - 1];
                }
                let firstValidIdx = -1, firstBenchNav = null;
                for (let i = 0; i < rawBenchValues.length; i++) {
                    if (rawBenchValues[i] !== null) { firstValidIdx = i; firstBenchNav = rawBenchValues[i]; break; }
                }
                if (firstValidIdx !== -1) {
                    const firstNetValue = chartNetValues[firstValidIdx];
                    if (firstNetValue && firstBenchNav > 0) {
                        const scale = firstNetValue / firstBenchNav;
                        const scaledBenchValues = rawBenchValues.map(function (v) { return v !== null ? v * scale : null; });
                        benchmarkDataset = {
                            label: benchmark.name + ' (比较基准)',
                            data: scaledBenchValues,
                            borderColor: '#f97316', backgroundColor: 'transparent',
                            borderDash: [5, 5], borderWidth: 2, pointRadius: 0, tension: 0.1, spanGaps: false
                        };
                    }
                }
            }
        }
        const startNetValue = chartNetValues.length > 0 ? chartNetValues[0] : 1.0;
        const netDatasets = [{
            label: '组合净值', data: chartNetValues, borderColor: '#10b981',
            backgroundColor: 'rgba(16,185,129,0.1)', fill: true, tension: 0.1,
            pointRadius: 0, pointHoverRadius: 6, yAxisID: 'y'
        }];
        if (benchmarkDataset) { benchmarkDataset.yAxisID = 'y'; netDatasets.push(benchmarkDataset); }
        const allNumericValues = chartNetValues.concat(benchmarkDataset ? benchmarkDataset.data.filter(function (v) { return v !== null && !isNaN(v); }) : []);
        const minVal = Math.min.apply(null, allNumericValues);
        const maxVal = Math.max.apply(null, allNumericValues);
        const padding = (maxVal - minVal) * 0.05;
        const netCtx = document.getElementById('netValueChart').getContext('2d');
        if (netValueChart) netValueChart.destroy();
        netValueChart = new Chart(netCtx, {
            type: 'line',
            data: { labels: chartDates, datasets: netDatasets },
            options: {
                responsive: true, interaction: { mode: 'index', intersect: false },
                plugins: {
                    tooltip: { mode: 'index', callbacks: { label: function (context) { return context.dataset.label + ': ' + context.parsed.y.toFixed(4); } } },
                    legend: { labels: { usePointStyle: true } }
                },
                scales: {
                    x: { ticks: { maxTicksLimit: 15 } },
                    y: {
                        beginAtZero: false, position: 'left',
                        title: { display: true, text: '净值 (起始日 ' + startNetValue.toFixed(4) + ')' },
                        min: minVal - padding, max: maxVal + padding,
                        ticks: { callback: function (value) { return value.toFixed(4); } }
                    },
                    y1: {
                        position: 'right', title: { display: true, text: '相对起始日涨跌 (%)' },
                        grid: { drawOnChartArea: false }, min: minVal - padding, max: maxVal + padding,
                        ticks: { callback: function (value) { if (!startNetValue || startNetValue === 0) return ''; const pct = ((value - startNetValue) / startNetValue) * 100; return pct.toFixed(1) + '%'; } }
                    }
                }
            }
        });
        const assetCtx = document.getElementById('assetChart').getContext('2d');
        if (assetChart) assetChart.destroy();
        assetChart = new Chart(assetCtx, {
            type: 'line',
            data: { labels: chartDates, datasets: [{ label: '总资产', data: chartAssets, borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.1)', fill: true, tension: 0.1, pointRadius: 0 }] },
            options: { responsive: true, interaction: { mode: 'index' }, scales: { x: { ticks: { maxTicksLimit: 15 } } } }
        });
        renderAnalysisTable();
        renderPeriodMetrics();
    }
    async function renderAnalysisTable() {
        const validDates = RD.reportData.dates.map(function (s) { return new Date(s + 'T00:00:00'); });
        const netValues = RD.reportData.netValues;
        if (validDates.length === 0 || netValues.length === 0) return;
        const firstInvestmentDate = validDates[0];
        const latestPortfolioDate = validDates[validDates.length - 1];
        const chartStartInput = document.getElementById('chartStartDate');
        const chartEndInput = document.getElementById('chartEndDate');
        const chartStartDate = chartStartInput && chartStartInput.value ? new Date(chartStartInput.value + 'T00:00:00') : null;
        const chartEndDate = chartEndInput && chartEndInput.value ? new Date(chartEndInput.value + 'T00:00:00') : null;
        const adjustedChartStartDate = (chartStartDate && chartStartDate >= validDates[0] && chartStartDate <= validDates[validDates.length - 1]) ? chartStartDate : validDates[0];
        const adjustedChartEndDate = (chartEndDate && chartEndDate >= validDates[0] && chartEndDate <= validDates[validDates.length - 1]) ? chartEndDate : validDates[validDates.length - 1];
        let benchmarkData = null;
        if (curBmId) { const bm = getBenchmarkById(curBmId); benchmarkData = bm ? bm.data : null; }
        function getIndexForDate(targetDate) {
            if (!targetDate) return -1;
            for (let i = validDates.length - 1; i >= 0; i--) { if (validDates[i] <= targetDate) return i; }
            return 0;
        }
        const nodeConfigs = [
            { name: '第一笔投资', getIndex: function () { return 0; } },
            { name: '图表开始', getIndex: function () { return getIndexForDate(adjustedChartStartDate); } },
            { name: '图表结束', getIndex: function () { return getIndexForDate(adjustedChartEndDate); } },
            { name: '组合最新', getIndex: function () { return validDates.length - 1; } }
        ];
        const nodes = []; const seenIndices = new Set();
        nodeConfigs.forEach(function (config) {
            const idx = config.getIndex();
            if (idx >= 0 && idx < validDates.length) {
                if (seenIndices.has(idx)) { const ex = nodes.find(function (n) { return n.idx === idx; }); if (ex) ex.names.push(config.name); }
                else { nodes.push({ idx: idx, date: validDates[idx], names: [config.name] }); seenIndices.add(idx); }
            }
        });
        nodes.sort(function (a, b) { return a.idx - b.idx; });
        if (nodes.length === 0) return;
        let bmMap = null, firstValidBenchNav = null, firstValidBenchIdx = -1;
        if (benchmarkData) {
            bmMap = new Map(benchmarkData.map(function (d) { return [d.date, d.nav]; }));
            for (let i = 0; i < validDates.length; i++) {
                const nav = bmMap.get(formatDate(validDates[i]));
                if (nav !== undefined) { firstValidBenchNav = nav; firstValidBenchIdx = i; break; }
            }
        }
        const baseIdx = nodes[0].idx;
        const baseComboNav = netValues[baseIdx];
        let baseBenchNav = null;
        if (bmMap && firstValidBenchNav !== null && firstValidBenchIdx >= 0) {
            const scale = baseComboNav / firstValidBenchNav;
            const baseDateStr = formatDate(validDates[baseIdx]);
            const rawNav = bmMap.get(baseDateStr);
            if (rawNav !== undefined) baseBenchNav = rawNav * scale;
            else { for (let i = baseIdx; i >= 0; i--) { const nav = bmMap.get(formatDate(validDates[i])); if (nav !== undefined) { baseBenchNav = nav * scale; break; } } }
        }
        const rows = [];
        for (let i = 0; i < nodes.length; i++) {
            const node = nodes[i];
            const idx = node.idx;
            const dateStr = formatDate(node.date);
            const comboNav = netValues[idx];
            let benchNav = null;
            if (bmMap && firstValidBenchNav !== null && firstValidBenchIdx >= 0) {
                const scale = baseComboNav / firstValidBenchNav;
                const rawNav = bmMap.get(dateStr);
                if (rawNav !== undefined) benchNav = rawNav * scale;
                else { for (let j = idx; j >= 0; j--) { const nav = bmMap.get(formatDate(validDates[j])); if (nav !== undefined) { benchNav = nav * scale; break; } } }
            }
            const comboAccReturn = baseComboNav > 0 ? (comboNav / baseComboNav - 1) * 100 : 0;
            const benchAccReturn = (benchNav !== null && baseBenchNav !== null && baseBenchNav > 0) ? (benchNav / baseBenchNav - 1) * 100 : null;
            const excessReturn = benchAccReturn !== null ? comboAccReturn - benchAccReturn : null;
            let stageCombo = null, stageBench = null;
            if (i > 0) {
                const prevIdx = nodes[i - 1].idx;
                const prevCombo = netValues[prevIdx];
                stageCombo = prevCombo > 0 ? (comboNav / prevCombo - 1) * 100 : null;
                if (benchmarkData && benchNav !== null) {
                    const prevDateStr = formatDate(validDates[prevIdx]);
                    const prevRawNav = bmMap.get(prevDateStr);
                    let prevBenchNav = null;
                    if (prevRawNav !== undefined) prevBenchNav = prevRawNav * (baseComboNav / firstValidBenchNav);
                    else { for (let j = prevIdx; j >= 0; j--) { const nav = bmMap.get(formatDate(validDates[j])); if (nav !== undefined) { prevBenchNav = nav * (baseComboNav / firstValidBenchNav); break; } } }
                    stageBench = (prevBenchNav && prevBenchNav > 0) ? (benchNav / prevBenchNav - 1) * 100 : null;
                }
            }
            rows.push({
                name: node.names.join('/'), date: dateStr,
                comboNav: comboNav.toFixed(4),
                benchNav: benchNav !== null ? benchNav.toFixed(4) : '-',
                comboAcc: comboAccReturn.toFixed(2) + '%',
                benchAcc: benchAccReturn !== null ? benchAccReturn.toFixed(2) + '%' : '-',
                excess: excessReturn !== null ? excessReturn.toFixed(2) + '%' : '-',
                stageCombo: stageCombo !== null ? stageCombo.toFixed(2) + '%' : '-',
                stageBench: stageBench !== null ? stageBench.toFixed(2) + '%' : '-'
            });
        }
        const tbody = document.getElementById('analysisTableBody');
        if (!tbody) return;
        tbody.innerHTML = rows.map(function (r) {
            return '<tr class="hover:bg-gray-50">' +
                '<td class="px-4 py-2 border-b text-center font-medium">' + r.name + '</td>' +
                '<td class="px-4 py-2 border-b text-center font-mono">' + r.date + '</td>' +
                '<td class="px-4 py-2 border-b text-center">' + r.comboNav + '</td>' +
                '<td class="px-4 py-2 border-b text-center">' + r.benchNav + '</td>' +
                '<td class="px-4 py-2 border-b text-center ' + (parseFloat(r.comboAcc) >= 0 ? 'text-green-600' : 'text-red-600') + '">' + r.comboAcc + '</td>' +
                '<td class="px-4 py-2 border-b text-center ' + (r.benchAcc !== '-' && parseFloat(r.benchAcc) >= 0 ? 'text-green-600' : 'text-red-600') + '">' + r.benchAcc + '</td>' +
                '<td class="px-4 py-2 border-b text-center ' + (r.excess !== '-' && parseFloat(r.excess) >= 0 ? 'text-green-600' : 'text-red-600') + '">' + r.excess + '</td>' +
                '<td class="px-4 py-2 border-b text-center ' + (r.stageCombo !== '-' && parseFloat(r.stageCombo) >= 0 ? 'text-green-600' : 'text-red-600') + '">' + r.stageCombo + '</td>' +
                '<td class="px-4 py-2 border-b text-center ' + (r.stageBench !== '-' && parseFloat(r.stageBench) >= 0 ? 'text-green-600' : 'text-red-600') + '">' + r.stageBench + '</td>' +
                '</tr>';
        }).join('');
    }
    document.getElementById('chartStartDate').addEventListener('change', updateCharts);
    document.getElementById('chartEndDate').addEventListener('change', updateCharts);
    document.getElementById('benchmarkSelect').addEventListener('change', function (e) {
        curBmId = e.target.value ? parseInt(e.target.value) : null;
        updateCharts();
    });
    updateCharts();
    initMetricTooltip();
}

// 导出交互式 HTML 报告（Chart.js 内联，纯离线；Tailwind CDN；内嵌全部基准可切换）
async function exportReportHTML() {
    // 按当前模式分流：策略比较模式导出策略对比报告
    if (currentMode === 'sc') { await exportScReportHTML(); return; }
    if (!backtestResult.dates || backtestResult.dates.length === 0) { alert('请先运行回测再导出报告'); return; }
    const reportData = {
        dates: backtestResult.dates.map(function (d) { return formatDate(d); }),
        assets: backtestResult.assets,
        netValues: backtestResult.netValues,
        invests: backtestResult.invests || [],
        cashDivs: backtestResult.cashDivs || [],
        totalCashSeries: backtestResult.totalCashSeries || []
    };
    const benchmarksAll = await db.benchmarks.toArray();
    const benchmarksData = benchmarksAll.map(function (b) { return { id: b.id, name: b.name, data: b.data }; });
    const metricsHtml = document.getElementById('metrics').innerHTML;

    // 盈利概率 / 相关性分析（抓取当前已渲染的表格快照）
    const ppTableHtml = document.getElementById('profitProbabilityTable') ? document.getElementById('profitProbabilityTable').innerHTML : '';
    const ppVisible = document.getElementById('profitProbabilitySection') && document.getElementById('profitProbabilitySection').style.display !== 'none' && ppTableHtml.trim() !== '';
    const corrTableHtml = document.getElementById('correlationTable') ? document.getElementById('correlationTable').innerHTML : '';
    const corrLegendHtml = document.getElementById('correlationLegend') ? document.getElementById('correlationLegend').innerHTML : '';
    const corrVisible = document.getElementById('correlationSection') && document.getElementById('correlationSection').style.display !== 'none' && corrTableHtml.trim() !== '';
    const ppDesc = profitMode === 'dca'
        ? '以历史任意时点为起点、按投资计划规则进行定投模拟，持有满对应时长后的盈利概率、平均持有收益与年化收益率（资金加权；持有期以交易日近似，1 自然年 ≈ 252 交易日）。'
        : '历史任意时点一次性买入并持有满对应时长后的盈利概率、平均收益与年化收益率（时间加权净值口径；持有期以交易日近似，1 自然年 ≈ 252 交易日）。';
    const ppBodyHtml = ppVisible
        ? ('<p class="text-sm text-gray-500 mb-4">' + ppDesc + '</p>' + ppTableHtml + '<p class="text-xs text-gray-400 mt-3">盈利概率为历史业绩数据测算，不代表未来收益。</p>')
        : '';
    const corrBodyHtml = corrVisible
        ? ('<p class="text-sm text-gray-500 mb-4">投资计划中各基金日收益率（以交易日对齐）的皮尔逊相关系数矩阵。</p>' + corrTableHtml + corrLegendHtml + '<p class="text-xs text-gray-400 mt-3">本页面展示收益相关数据仅为历史数据测算，不构成收益保证或预示其未来表现。</p>')
        : '';

    // 同步主工具界面 5 个折叠面板的当前状态（容器含 hidden 类即代表已折叠）
    const _isHidden = function (id) { const el = document.getElementById(id); return el ? el.classList.contains('hidden') : false; };
    const ppCollapsed = _isHidden('profitProbCollapsible');
    const corrCollapsed = _isHidden('correlationCollapsible');
    const netCollapsed = _isHidden('netValueChartCollapsible');
    const assetCollapsed = _isHidden('assetChartCollapsible');
    const analysisCollapsed = _isHidden('analysisTableCollapsible');

    // 报告面板辅助构造器：普通卡片 / 可折叠卡片（交互与主工具一致）
    function rptPlain(title, bodyHtml) {
        return '<div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '    <h3 class="text-lg font-semibold text-gray-700 mb-3">' + title + '</h3>\n' +
            bodyHtml + '\n' +
            '</div>';
    }
    function rptCollapsible(title, targetId, bodyHtml, collapsed) {
        return '<div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '    <div class="flex items-center justify-between mb-3">\n' +
            '        <h3 class="text-lg font-semibold text-gray-700">' + title + '</h3>\n' +
            '        <button type="button" class="text-sm text-gray-500 hover:text-gray-700 px-2 py-1 rounded border" data-target="' + targetId + '" onclick="toggleRpt(this)">' + (collapsed ? '▼ 展开' : '▲ 折叠') + '</button>\n' +
            '    </div>\n' +
            '    <div id="' + targetId + '"' + (collapsed ? ' class="hidden"' : '') + '>' + bodyHtml + '</div>\n' +
            '</div>';
    }
    const startD = formatDate(backtestResult.dates[0]);
    const endD = formatDate(backtestResult.dates[backtestResult.dates.length - 1]);
    const data = { reportData: reportData, benchmarksData: benchmarksData, currentBenchmarkId: currentBenchmarkId };

    let chartJsSrc = '';
    try { chartJsSrc = await (await fetch(CHART_JS_CDN)).text(); } catch (e) { chartJsSrc = ''; }
    let chartJsBlock;
    if (chartJsSrc) chartJsBlock = '<scr' + 'ipt>' + chartJsSrc.replace(/<\/script>/gi, '<\\/script>') + '</scr' + 'ipt>';
    else chartJsBlock = '<scr' + 'ipt src="' + CHART_JS_CDN + '"></scr' + 'ipt>';

    // 投资计划概览表（与回测结果一并导出，便于分享查看）
    const planRowsHtml = investmentPlans.map(function (p, i) {
        const typeText = p.type === 'single' ? '单笔' : p.type === 'weekly' ? '每周定投' : '每月定投';
        const divText = p.div === 'reinvest' ? '红利再投资' : '现金分红';
        const cn = fundCodeName(p.fund);
        const periodText = p.type === 'single' ? p.startDate : (p.startDate + ' ~ ' + p.endDate);
        let durText = '—';
        if (p.type !== 'single') {
            const sd = new Date(p.startDate + 'T00:00:00');
            const ed = new Date(p.endDate + 'T00:00:00');
            if (!isNaN(sd) && !isNaN(ed) && ed >= sd) {
                durText = (Math.round((ed - sd) / 86400000) / 365).toFixed(2) + ' 年';
            }
        }
        return '<tr class="hover:bg-gray-50">' +
            '<td class="px-3 py-2 border-b text-center">' + (i + 1) + '</td>' +
            '<td class="px-3 py-2 border-b text-center font-mono text-xs">' + cn.code + '</td>' +
            '<td class="px-3 py-2 border-b text-center font-medium">' + (cn.name || cn.code) + '</td>' +
            '<td class="px-3 py-2 border-b text-center">' + typeText + '</td>' +
            '<td class="px-3 py-2 border-b text-center">' + p.amount.toFixed(0) + ' 元</td>' +
            '<td class="px-3 py-2 border-b text-center ' + (p.div === 'reinvest' ? 'text-emerald-600 font-medium' : 'text-amber-600') + '">' + divText + '</td>' +
            '<td class="px-3 py-2 border-b text-center font-mono text-xs">' + periodText + '</td>' +
            '<td class="px-3 py-2 border-b text-center text-xs text-gray-500">' + durText + '</td>' +
            '</tr>';
    }).join('');
    const planTableHtml = investmentPlans.length === 0
        ? '<p class="text-gray-500 text-sm">本次回测未添加投资计划。</p>'
        : '<div class="overflow-x-auto"><table class="min-w-full bg-white border border-gray-200 rounded-lg text-sm">' +
          '<thead class="bg-gray-100"><tr>' +
          '<th class="px-3 py-2 border-b text-center">#</th>' +
          '<th class="px-3 py-2 border-b text-center">代码</th>' +
          '<th class="px-3 py-2 border-b text-center">基金名称</th>' +
          '<th class="px-3 py-2 border-b text-center">投资方法</th>' +
          '<th class="px-3 py-2 border-b text-center">投资金额</th>' +
          '<th class="px-3 py-2 border-b text-center">分红方式</th>' +
          '<th class="px-3 py-2 border-b text-center">投资期限(起止)</th>' +
          '<th class="px-3 py-2 border-b text-center">投资年限</th>' +
          '</tr></thead><tbody>' + planRowsHtml + '</tbody></table></div>';

    const inner = '(' + buildReportInner.toString() + ')();';

    const benchmarkOptions = benchmarksData.map(function (b) {
        return '<option value="' + b.id + '"' + (b.id === currentBenchmarkId ? ' selected' : '') + '>' + b.name + '</option>';
    }).join('');

    const analysisTableInner =
        '                    <div class="overflow-x-auto">\n' +
        '                        <table class="min-w-full bg-white border border-gray-200 rounded-lg text-sm">\n' +
        '                            <thead class="bg-gray-100"><tr>\n' +
        '                                <th class="px-4 py-2 border-b text-center">节点</th><th class="px-4 py-2 border-b text-center">日期</th>\n' +
        '                                <th class="px-4 py-2 border-b text-center">组合净值</th><th class="px-4 py-2 border-b text-center">基准净值</th>\n' +
        '                                <th class="px-4 py-2 border-b text-center">组合累计收益</th><th class="px-4 py-2 border-b text-center">基准累计收益</th>\n' +
        '                                <th class="px-4 py-2 border-b text-center">超额收益</th><th class="px-4 py-2 border-b text-center">阶段收益(组合)</th>\n' +
        '                                <th class="px-4 py-2 border-b text-center">阶段收益(基准)</th>\n' +
        '                            </tr></thead>\n' +
        '                            <tbody id="analysisTableBody"></tbody>\n' +
        '                        </table>\n' +
        '                    </div>\n' +
        '                    <p class="text-xs text-gray-500 mt-2">* 净值已归一化至投资最早日期为1.0，基准已对齐组合起点。阶段收益为相邻节点间收益率。</p>';

    const html = '<!DOCTYPE html>\n' +
        '<html lang="zh-CN">\n' +
        '<head>\n' +
        '    <meta charset="UTF-8">\n' +
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n' +
        '    <title>基金组合回测报告</title>\n' +
        '    <scr' + 'ipt src="' + TAILWIND_CDN + '"></scr' + 'ipt>\n' +
        '    ' + chartJsBlock + '\n' +
        '    <scr' + 'ipt>\n' +
        '        function toggleRpt(b){var id=b.getAttribute("data-target");var c=document.getElementById(id);var col=c.classList.toggle("hidden");b.textContent=col?"▼ 展开":"▲ 折叠";if(!col){var cid=id==="rptNetValue"?"netValueChart":(id==="rptAsset"?"assetChart":null);if(cid){var ch=window.Chart&&Chart.getChart(cid);if(ch)ch.resize();}window.dispatchEvent(new Event("resize"));}}\n' +
        '    </scr' + 'ipt>\n' +
        '</head>\n' +
        '<body class="bg-gray-50 min-h-screen p-6">\n' +
        '    <div class="max-w-6xl mx-auto">\n' +
        '        <h1 class="text-3xl font-bold text-center mb-8 text-gray-800">📊 基金组合回测报告</h1>\n' +
        // 投资计划概览（置顶，常开）
        rptPlain('投资计划概览', planTableHtml) + '\n' +
        // 回测指标（常开）
        '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
        '            <h3 class="text-lg font-semibold text-gray-700 mb-3">回测指标</h3>\n' +
        '            <div id="metrics" class="grid grid-cols-2 md:grid-cols-4 gap-4">' + metricsHtml + '</div>\n' +
        '        </div>\n' +
        // 盈利概率（可折叠，状态同步主工具）
        (ppBodyHtml ? rptCollapsible('盈利概率', 'rptProfitProb', ppBodyHtml, ppCollapsed) + '\n' : '') +
        // 相关性分析（可折叠，状态同步主工具）
        (corrBodyHtml ? rptCollapsible('相关性分析', 'rptCorr', corrBodyHtml, corrCollapsed) + '\n' : '') +
        // 图表设置（常开：日期筛选 + 比较基准）
        '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
        '            <h3 class="text-lg font-semibold text-gray-700 mb-3">图表设置</h3>\n' +
        '            <div class="bg-gray-50 p-4 rounded-lg mb-6">\n' +
        '                <h4 class="text-base font-semibold mb-3 text-gray-700">图表日期范围筛选</h4>\n' +
        '                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">\n' +
        '                    <div><label class="block text-sm font-medium text-gray-700 mb-1">起始日期</label><input type="date" id="chartStartDate" class="w-full p-2 border border-gray-300 rounded-lg" value="' + startD + '" min="' + startD + '" max="' + endD + '"></div>\n' +
        '                    <div><label class="block text-sm font-medium text-gray-700 mb-1">结束日期</label><input type="date" id="chartEndDate" class="w-full p-2 border border-gray-300 rounded-lg" value="' + endD + '" min="' + startD + '" max="' + endD + '"></div>\n' +
        '                </div>\n' +
        '            </div>\n' +
        '            <div class="mb-4 flex items-center gap-3">\n' +
        '                <label class="text-sm font-medium text-gray-700">比较基准：</label>\n' +
        '                <select id="benchmarkSelect" class="p-2 border border-gray-500 rounded-lg bg-white shadow-sm">\n' +
        '                    <option value="">-- 无 --</option>\n' +
        benchmarkOptions +
        '                </select>\n' +
        '            </div>\n' +
        '        </div>\n' +
        // 组合净值 vs 比较基准（可折叠）
        rptCollapsible('组合净值 vs 比较基准', 'rptNetValue', '<canvas id="netValueChart"></canvas>', netCollapsed) + '\n' +
        // 组合总资产曲线（可折叠）
        rptCollapsible('组合总资产曲线', 'rptAsset', '<canvas id="assetChart"></canvas>', assetCollapsed) + '\n' +
        // 关键节点对比分析（可折叠）
        rptCollapsible('关键节点对比分析', 'rptAnalysis', analysisTableInner, analysisCollapsed) + '\n' +
        // 选定区间投资表现（常开）
        '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
        '            <h3 class="text-lg font-semibold mb-3 text-gray-700">📊 选定区间投资表现<span id="periodRangeLabel" class="text-sm font-normal text-gray-500"></span></h3>\n' +
        '            <div id="periodMetrics" class="grid grid-cols-2 md:grid-cols-4 gap-4"></div>\n' +
        '            <p class="text-xs text-gray-500 mt-2">* 区间指标随上方日期筛选动态更新。</p>\n' +
        '        </div>\n' +
        '        <p class="text-center text-xs text-gray-400 mt-4">本报告由「基金组合回测工具」生成，基准切换与日期筛选均可在浏览器中交互。</p>\n' +
        '    </div>\n' +
        '    <scr' + 'ipt>window.__RD__ = ' + JSON.stringify(data).replace(/<\/script>/gi, '<\\/script>') + ';</scr' + 'ipt>\n' +
        '    <scr' + 'ipt>' + inner + '</scr' + 'ipt>\n' +
        '</body>\n' +
        '</html>';

    const blob = new Blob([html], { type: 'text/html;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = '回测报告_' + startD + '_' + endD + '.html';
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
}

// 导出「定投策略比较」交互式 HTML 报告
// 曲线图内联 Chart.js 重绘，并保留「按持有期月数/按日期」「净值/XIRR年化」切换功能（导出文件内可实时切换）
async function exportScReportHTML() {
    if (!scResults || scResults.length === 0) { alert('请先运行策略比较再导出报告'); return; }
    if (!scChart) { alert('请先运行策略比较再导出报告'); return; }

    // 序列化「原始数据」而非已计算的数据集，以便在导出文件里复刻 drawScChart 的切换重算
    const scRaw = {
        initX: scChartXMode,
        initY: scChartYMode,
        xirrTitle: SC_XIRR_TITLE,
        netTitle: SC_NET_TITLE,
        xirrNote: SC_XIRR_NOTE,
        netNote: SC_NET_NOTE,
        results: scResults.map(function (r, idx) {
            if (!r._runningXirr) r._runningXirr = runningXirr(r);
            const cn = fundCodeName(r.item.fund);
            const isStopGain = r.item.strategy === '5';
            return {
                dates: r.dates.map(function (d) { return d.getTime(); }),
                invests: r.invests || [],
                assets: r.assets || [],
                peakPrincipal: r.peakPrincipal || [],
                stopGainEvents: r.stopGainEvents || [],
                runningXirr: r._runningXirr || [],
                label: (cn.name || r.item.fund) + '·' + SC_STRATEGIES[r.item.strategy],
                color: SC_COLORS[idx % SC_COLORS.length],
                isStopGain: isStopGain
            };
        })
    };

    // 内联 Chart.js（优先内联，失败则外链 CDN）
    let chartJsSrc = '';
    try { chartJsSrc = await (await fetch(CHART_JS_CDN)).text(); } catch (e) { chartJsSrc = ''; }
    let chartJsBlock;
    if (chartJsSrc) chartJsBlock = '<scr' + 'ipt>' + chartJsSrc.replace(/<\/script>/gi, '<\\/script>') + '</scr' + 'ipt>';
    else chartJsBlock = '<scr' + 'ipt src="' + CHART_JS_CDN + '"></scr' + 'ipt>';

    // 抓取当前已渲染的视图快照（指标卡/对比表/规则/提示）
    const metricsHtml = document.getElementById('scMetrics') ? document.getElementById('scMetrics').innerHTML : '';
    const tableEl = document.querySelector('#scResultArea table');
    const tableHtml = tableEl ? tableEl.outerHTML : '';
    const rulesEl = document.getElementById('scRules');
    const rulesHtml = rulesEl ? rulesEl.innerHTML : '';
    const errorsEl = document.getElementById('scErrors');
    const errorsHtml = errorsEl ? errorsEl.innerHTML : '';
    const dateStr = formatDate(new Date());

    const html = '<!DOCTYPE html>\n' +
        '<html lang="zh-CN">\n' +
        '<head>\n' +
        '    <meta charset="UTF-8">\n' +
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n' +
        '    <title>定投策略比较报告</title>\n' +
        '    <scr' + 'ipt src="' + TAILWIND_CDN + '"></scr' + 'ipt>\n' +
        '    ' + chartJsBlock + '\n' +
        '</head>\n' +
        '<body class="bg-gray-50 min-h-screen p-6">\n' +
        '    <div class="max-w-6xl mx-auto">\n' +
        '        <h1 class="text-3xl font-bold text-center mb-8 text-gray-800">📊 定投策略比较报告</h1>\n' +
        '        <p class="text-center text-sm text-gray-400 mb-8">生成日期：' + dateStr + '（曲线图支持鼠标悬停查看各点数值，并可切换 X 轴与 Y 轴口径）</p>\n' +
        // 指标卡（常开）
        (metricsHtml ? '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '            <h3 class="text-lg font-semibold text-gray-700 mb-3">策略指标对比</h3>\n' +
            '            <div class="grid grid-cols-2 md:grid-cols-4 gap-4">' + metricsHtml + '</div>\n' +
            '        </div>\n' : '') +
        // 曲线图（常开，带切换按钮，交互式）
        '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
        '            <div class="flex flex-wrap items-center justify-between gap-2 mb-3">\n' +
        '                <h3 id="scExpTitle" class="text-lg font-semibold text-gray-700"></h3>\n' +
        '                <div class="flex flex-wrap gap-2">\n' +
        '                    <button id="expXMonth" onclick="scSetX(\'month\')" class="px-3 py-1.5 bg-blue-600 text-white text-sm rounded">按持有期月数对齐</button>\n' +
        '                    <button id="expXDate" onclick="scSetX(\'date\')" class="px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100 text-sm rounded">按日期</button>\n' +
        '                    <button id="expYNet" onclick="scSetY(\'net\')" class="px-3 py-1.5 bg-blue-600 text-white text-sm rounded">净值</button>\n' +
        '                    <button id="expYXirr" onclick="scSetY(\'xirr\')" class="px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100 text-sm rounded">XIRR年化</button>\n' +
        '                </div>\n' +
        '            </div>\n' +
        '            <div style="height: 420px;"><canvas id="scChartExport"></canvas></div>\n' +
        '            <p id="scExpNote" class="text-xs text-gray-500 mt-2"></p>\n' +
        '        </div>\n' +
        // 对比表（常开）
        (tableHtml ? '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '            <h3 class="text-lg font-semibold text-gray-700 mb-3">策略对比明细</h3>\n' +
            '            <div class="overflow-x-auto">' + tableHtml + '</div>\n' +
            '        </div>\n' : '') +
        // 规则（常开）
        (rulesHtml ? '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '            <h3 class="text-lg font-semibold text-gray-700 mb-3">策略规则说明</h3>\n' +
            rulesHtml + '\n' +
            '        </div>\n' : '') +
        // 错误/提示（常开）
        (errorsHtml ? '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '            <h3 class="text-lg font-semibold text-gray-700 mb-3">提示与说明</h3>\n' +
            errorsHtml + '\n' +
            '        </div>\n' : '') +
        '        <p class="text-center text-xs text-gray-400 mt-4">本报告由「基金定投测算工具」生成，曲线图可在浏览器中交互切换。</p>\n' +
        '    </div>\n' +
        '    <scr' + 'ipt>window.__sc__ = ' + JSON.stringify(scRaw).replace(/<\/script>/gi, '<\\/script>') + ';</scr' + 'ipt>\n' +
        '    <scr' + 'ipt>\n' +
        '        var __sc = window.__sc__;\n' +
        '        var __x = __sc.initX, __y = __sc.initY;\n' +
        '        var __hasSG = __sc.results.some(function (r) { return r.isStopGain; });\n' +
        '        var __chart = null;\n' +
        '        var __on = "px-3 py-1.5 bg-blue-600 text-white text-sm rounded";\n' +
        '        var __off = "px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100 text-sm rounded";\n' +
        '        function __fmt(d) { var t = new Date(d); if (isNaN(t)) return d; var y = t.getFullYear(); var m = ("0" + (t.getMonth() + 1)).slice(-2); var day = ("0" + t.getDate()).slice(-2); return y + "-" + m + "-" + day; }\n' +
        '        function __build(xMode, yMode) {\n' +
        '            var isXirr = yMode === "xirr";\n' +
        '            var ds = [];\n' +
        '            __sc.results.forEach(function (r) {\n' +
        '                var startTs = r.dates[0]; var cum = 0;\n' +
        '                var pts = [], sgPts = [];\n' +
        '                var sgSet = (r.isStopGain && r.stopGainEvents.length) ? new Set(r.stopGainEvents) : null;\n' +
        '                r.dates.forEach(function (ts, i) {\n' +
        '                    cum += (r.invests[i] || 0);\n' +
        '                    var months = (ts - startTs) / (1000 * 60 * 60 * 24 * 30.4375);\n' +
        '                    var yVal;\n' +
        '                    if (isXirr) { var v = r.runningXirr[i]; if (v == null) return; yVal = +v.toFixed(2); }\n' +
        '                    else {\n' +
        '                        var nv;\n' +
        '                        if (r.isStopGain) { var peak = r.peakPrincipal[i]; nv = peak > 0 ? 1 + (r.assets[i] - cum) / peak : null; }\n' +
        '                        else { nv = cum > 0 ? r.assets[i] / cum : null; }\n' +
        '                        if (nv == null) return; yVal = +nv.toFixed(4);\n' +
        '                    }\n' +
        '                    var xVal = xMode === "date" ? ts : +months.toFixed(2);\n' +
        '                    pts.push({ x: xVal, y: yVal });\n' +
        '                    if (sgSet && sgSet.has(i)) sgPts.push({ x: xVal, y: yVal });\n' +
        '                });\n' +
        '                ds.push({ label: r.label, data: pts, borderColor: r.color, backgroundColor: r.color, borderWidth: 2, pointRadius: 0, tension: 0.1, fill: false });\n' +
        '                if (sgPts.length) ds.push({ label: r.label + "·止盈点", data: sgPts, borderColor: r.color, backgroundColor: r.color, pointStyle: "circle", pointRadius: 4, pointHoverRadius: 6, pointBorderColor: "#ffffff", pointBorderWidth: 1.5, showLine: false, fill: false, isStopGainMarker: true });\n' +
        '            });\n' +
        '            return ds;\n' +
        '        }\n' +
        '        function __render() {\n' +
        '            var isXirr = __y === "xirr"; var isDate = __x === "date";\n' +
        '            var ds = __build(__x, __y);\n' +
        '            var xMin = Infinity, xMax = -Infinity;\n' +
        '            ds.forEach(function (d) { d.data.forEach(function (p) { if (p.x != null && isFinite(p.x)) { if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x; } }); });\n' +
        '            if (!isFinite(xMin)) { xMin = undefined; xMax = undefined; }\n' +
        '            if (__chart) __chart.destroy();\n' +
        '            var ctx = document.getElementById("scChartExport").getContext("2d");\n' +
        '            __chart = new Chart(ctx, {\n' +
        '                type: "line", data: { datasets: ds }, options: {\n' +
        '                    responsive: true, maintainAspectRatio: false,\n' +
        '                    interaction: { mode: "nearest", intersect: false },\n' +
        '                    scales: {\n' +
        '                        x: { type: "linear", min: xMin, max: xMax,\n' +
        '                            title: { display: true, text: isDate ? "日期" : "持有期（月）" },\n' +
        '                            ticks: { maxTicksLimit: 12, callback: function (v) { return isDate ? __fmt(new Date(v)) : v + "月"; } } },\n' +
        '                        y: { title: { display: true, text: isXirr\n' +
        '                            ? "资金加权收益率 XIRR 年化（%）"\n' +
        '                            : (__hasSG ? "投资净值（普通策略=总资产/累计投入；止盈策略=1+最大本金收益率，起点=1.0）" : "投资净值（资金加权，总资产/累计投入，起点=1.0）") } }\n' +
        '                    },\n' +
        '                    plugins: {\n' +
        '                        legend: { position: "bottom", labels: { filter: function (item, data) { return !data.datasets[item.datasetIndex].isStopGainMarker; } } },\n' +
        '                        tooltip: { callbacks: {\n' +
        '                            title: function (items) { return isDate ? __fmt(new Date(items[0].parsed.x)) : (items[0].parsed.x + " 月"); },\n' +
        '                            label: function (item) { return item.dataset.isStopGainMarker ? item.dataset.label : (isXirr ? item.parsed.y.toFixed(2) + "%" : item.parsed.y.toFixed(4)); }\n' +
        '                        } }\n' +
        '                    }\n' +
        '                }\n' +
        '            });\n' +
        '        }\n' +
        '        function __applyX(m) { __x = m; document.getElementById("expXMonth").className = m === "month" ? __on : __off; document.getElementById("expXDate").className = m === "date" ? __on : __off; }\n' +
        '        function __applyY(m) { __y = m; document.getElementById("expYNet").className = m === "net" ? __on : __off; document.getElementById("expYXirr").className = m === "xirr" ? __on : __off; var t = document.getElementById("scExpTitle"); if (t) t.textContent = m === "xirr" ? __sc.xirrTitle : __sc.netTitle; var n = document.getElementById("scExpNote"); if (n) n.textContent = m === "xirr" ? __sc.xirrNote : __sc.netNote; }\n' +
        '        function scSetX(m) { __applyX(m); __render(); }\n' +
        '        function scSetY(m) { __applyY(m); __render(); }\n' +
        '        (function () { __applyX(__x); __applyY(__y); __render(); })();\n' +
        '    </scr' + 'ipt>\n' +
        '</body>\n' +
        '</html>';

    const blob = new Blob([html], { type: 'text/html;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = '策略比较报告_' + dateStr + '.html';
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
}



