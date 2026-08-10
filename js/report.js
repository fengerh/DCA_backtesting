/* report.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// ============ 导入 / 导出 / 报告 ============
const REPORT_VERSION = '20260708';
const CHART_JS_CDN = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js';
const TAILWIND_CDN = 'https://cdn.tailwindcss.com';

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
        // 年份边界控制：过滤非法年份（5 位及以上/越界 1900~当前年+2）的日期行，并同步过滤对应 nav/div
        const dates = [], nav = [], div = [];
        for (let i = 0; i < (f.dates || []).length; i++) {
            const s = f.dates[i];
            if (sanitizeDateInput(s) === null) continue;
            dates.push(new Date(s + 'T00:00:00'));
            nav.push(f.nav ? f.nav[i] : undefined);
            div.push(f.div ? f.div[i] : undefined);
        }
        out[k] = {
            dates: dates,
            nav: nav, div: div,
            minDate: f.minDate, maxDate: f.maxDate
        };
    }
    return out;
}

// 收集「指数估值比较」界面全部表单参数（单指数 + 双指数）
function collectValuationState() {
    const get = function (id) {
        const el = document.getElementById(id);
        return el ? el.value : '';
    };
    return {
        // 单指数
        valSingleIndex: get('valSingleIndex'),
        valSingleStart: get('valSingleStart'),
        valSingleEnd: get('valSingleEnd'),
        valSingleShowStart: get('valSingleShowStart'),
        valSingleShowEnd: get('valSingleShowEnd'),
        valSingleRollYears1: get('valSingleRollYears1'),
        valSingleRollYears2: get('valSingleRollYears2'),
        valSingleRollYears3: get('valSingleRollYears3'),
        valSingleMeanN: get('valSingleMeanN'),
        valSingleHi: get('valSingleHi'),
        valSingleLo: get('valSingleLo'),
        // 双指数
        valRatioA: get('valRatioA'),
        valRatioB: get('valRatioB'),
        valRatioStart: get('valRatioStart'),
        valRatioEnd: get('valRatioEnd'),
        valRatioMode: get('valRatioMode'),
        valRatioN: get('valRatioN'),
        valRatioRollYears1: get('valRatioRollYears1'),
        valRatioRollYears2: get('valRatioRollYears2'),
        valRatioShowStart: get('valRatioShowStart'),
        valRatioShowEnd: get('valRatioShowEnd'),
        valRatioHi: get('valRatioHi'),
        valRatioLo: get('valRatioLo')
    };
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
        benchmarks: benchmarks,
        valuationState: collectValuationState()
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
        // resultSection 在点击“启动回测”后才显示，导入时不提前展示
    }
    // 年份边界控制：清洗导入计划中的非法日期（5 位及以上年份/越界）为空白，避免污染回测
    investmentPlans.forEach(function (p) {
        if (p.startDate && sanitizeDateInput(p.startDate) === null) p.startDate = '';
        if (p.endDate && sanitizeDateInput(p.endDate) === null) p.endDate = '';
        (p.activeRedeems || []).forEach(function (r) {
            if (r.date && sanitizeDateInput(r.date) === null) r.date = '';
        });
    });
    renderPlanList();
    await loadBenchmarkList();

    // 还原「指数估值比较」参数（基准 id 需经 idMap 重映射）
    if (typeof snap.valuationState === 'object' && snap.valuationState) {
        // 先重建估值下拉（填充当前基准选项），再写回保存的参数，避免被自动填充覆盖
        if (typeof refreshValuationLists === 'function') refreshValuationLists();
        restoreValuationState(snap.valuationState, idMap);
    }

    if (investmentPlans.length > 0) runBacktest();
    alert('项目导入完成，已自动运行回测');
}

// 将保存的估值参数写回表单（指数 id 经 idMap 重映射），并恢复输入状态
function restoreValuationState(vs, idMap) {
    const remap = function (id) {
        if (id == null || id === '') return id;
        return (idMap && idMap[id] != null) ? String(idMap[id]) : String(id);
    };
    const set = function (id, val) {
        const el = document.getElementById(id);
        if (el && val != null) el.value = String(val);
    };
    // 单指数
    set('valSingleIndex', remap(vs.valSingleIndex));
    set('valSingleStart', vs.valSingleStart);
    set('valSingleEnd', vs.valSingleEnd);
    set('valSingleShowStart', vs.valSingleShowStart);
    set('valSingleShowEnd', vs.valSingleShowEnd);
    set('valSingleRollYears1', vs.valSingleRollYears1);
    set('valSingleRollYears2', vs.valSingleRollYears2);
    set('valSingleRollYears3', vs.valSingleRollYears3);
    set('valSingleMeanN', vs.valSingleMeanN);
    set('valSingleHi', vs.valSingleHi);
    set('valSingleLo', vs.valSingleLo);
    // 双指数
    set('valRatioA', remap(vs.valRatioA));
    set('valRatioB', remap(vs.valRatioB));
    set('valRatioStart', vs.valRatioStart);
    set('valRatioEnd', vs.valRatioEnd);
    set('valRatioMode', vs.valRatioMode);
    set('valRatioN', vs.valRatioN);
    set('valRatioRollYears1', vs.valRatioRollYears1);
    set('valRatioRollYears2', vs.valRatioRollYears2);
    set('valRatioShowStart', vs.valRatioShowStart);
    set('valRatioShowEnd', vs.valRatioShowEnd);
    set('valRatioHi', vs.valRatioHi);
    set('valRatioLo', vs.valRatioLo);
    // 恢复 N 日输入框禁用态
    if (typeof updateRatioModeInput === 'function') updateRatioModeInput();
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
        "累计收益率(资金加权)": "资金加权口径：(总资产 ÷ 本金 − 1)。<br>正数代表整体实际赚钱。",
        "XIRR年化(资金加权)": "把每笔投入/分红/市值都当作现金流，算考虑时间权重的年化内部收益率。<br>早投的钱权重更高，比累计收益率更公平。",
        "年化收益率(时间加权净值)": "把时间加权净值序列年化（剔除你的投入节奏），反映“组合本身”的赚钱能力。",
        "胜率(正收益日占比)": "上涨交易日数 ÷ 总交易日数，越高说明日子大多在涨。",
        "最大回撤": "净值从最高点到最低点的最大跌幅(%)，越大代表最坏情况越惨。",
        "回撤持续天数": "最大回撤从峰值到谷值所经历的天数，越久越磨人。",
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
            let peak = nvs[0], maxDrawdown = 0, worstPeakIdx = 0, worstTroughIdx = 0, peakIdx = 0;
            for (let k = 0; k < nvs.length; k++) { const v = nvs[k]; if (v > peak) { peak = v; peakIdx = k; } const dd = (v - peak) / peak; if (dd < maxDrawdown) { maxDrawdown = dd; worstPeakIdx = peakIdx; worstTroughIdx = k; } }
            maxDrawdown *= 100;
            let sharpeRatio = NaN, calmarRatio = NaN;
            if (annualVolatility > 0) sharpeRatio = (annualReturnTwr - RISK_FREE_RATE) / annualVolatility;
            if (maxDrawdown !== 0) calmarRatio = annualReturnTwr / Math.abs(maxDrawdown / 100);
            // 回撤持续天数 = 最大回撤区间（峰值→谷值）经历的自然日，与后续是否创新高无关
            let maxSpan = (wDates[worstTroughIdx] - wDates[worstPeakIdx]) / 86400000;
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
            return { date: d, asset: RD.reportData.assets[i], nv: RD.reportData.netValues[i], invest: (RD.reportData.invests || [])[i] || 0 };
        }).filter(function (item) { return item.date >= startDate && item.date <= endDate; });
        const chartDates = filtered.map(function (d) { return formatDate(d.date); });
        const chartAssets = filtered.map(function (d) { return d.asset; });
        const chartNetValues = filtered.map(function (d) { return d.nv; });
        // 累计净收益 = 总资产 - 累计投入本金
        let cumInvest = 0;
        const chartNetProfit = filtered.map(function (d) { cumInvest += d.invest; return d.asset - cumInvest; });

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
            label: '时间加权净值', data: chartNetValues, borderColor: '#10b981',
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
            data: { labels: chartDates, datasets: [
                { label: '总资产', data: chartAssets, borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.1)', fill: true, tension: 0.1, pointRadius: 0 },
                { label: '累计净收益', data: chartNetProfit, borderColor: '#10b981', borderDash: [6, 3], backgroundColor: 'transparent', fill: false, tension: 0.1, pointRadius: 0 }
            ] },
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
    // 按当前模式分流：策略比较导出策略对比报告；指数估值导出估值报告
    if (currentMode === 'sc') { await exportScReportHTML(); return; }
    if (currentMode === 'valuation') { await exportValuationReportHTML(); return; }
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
    const planTypeText = function (type) {
        if (type === 'single') return '单笔';
        if (type === 'weekly') return '每周定投';
        if (type === 'biweekly') return '每双周定投';
        if (type === 'maxDrawdown') return '最大回撤投资';
        return '每月定投';
    };
    // 止盈配置文本：模拟组合止盈为布尔开启 + 阈值/赎回比例
    const planStopGainText = function (p) {
        if (!p.stopGain) return '—';
        return '目标止盈 ' + (p.stopGainPct != null ? p.stopGainPct : 0) + '% · 赎回 ' + (p.stopGainSellRatio != null ? p.stopGainSellRatio : 100) + '%';
    };
    // 主动赎回配置文本：列出每个事件的日期与方式
    const planActiveRedeemText = function (p) {
        const arr = p.activeRedeems || [];
        if (arr.length === 0) return '—';
        return arr.map(function (ev) {
            const modeTxt = ev.mode === 'amount' ? ('按金额 ' + (ev.value != null ? ev.value : 0) + ' 元') : ('按比例 ' + (ev.value != null ? ev.value : 0) + '%');
            return (ev.date || '') + ' ' + modeTxt;
        }).join('<br>');
    };
    const planRowsHtml = investmentPlans.map(function (p, i) {
        const typeText = planTypeText(p.type);
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
            '<td class="px-3 py-2 border-b text-center ' + (p.stopGain ? 'text-amber-700' : 'text-gray-400') + '">' + planStopGainText(p) + '</td>' +
            '<td class="px-3 py-2 border-b text-center ' + ((p.activeRedeems && p.activeRedeems.length) ? 'text-orange-700' : 'text-gray-400') + ' text-xs">' + planActiveRedeemText(p) + '</td>' +
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
          '<th class="px-3 py-2 border-b text-center">止盈</th>' +
          '<th class="px-3 py-2 border-b text-center">主动赎回</th>' +
          '<th class="px-3 py-2 border-b text-center">投资期限(起止)</th>' +
          '<th class="px-3 py-2 border-b text-center">投资年限</th>' +
          '</tr></thead><tbody>' + planRowsHtml + '</tbody></table></div>';

    // 止盈 / 主动赎回执行明细（静态表格，按单基金列出每次触发日期、金额、净值、方式）
    const sgDetailHtml = backtestResult.hasStopGainPlan
        ? (function () {
            const byFund = backtestResult.stopGainByFund || {};
            const codes = Object.keys(byFund);
            if (codes.length === 0) return '<p class="text-gray-500 text-sm">已启用目标止盈，但回测区间内未触发。</p>';
            return '<div class="overflow-x-auto"><table class="min-w-full bg-white border border-gray-200 rounded-lg text-sm">' +
                '<thead class="bg-gray-100"><tr>' +
                '<th class="px-3 py-2 border-b text-center">基金</th>' +
                '<th class="px-3 py-2 border-b text-center">触发日期</th>' +
                '<th class="px-3 py-2 border-b text-center">赎回金额</th>' +
                '<th class="px-3 py-2 border-b text-center">赎回比例</th>' +
                '<th class="px-3 py-2 border-b text-center">净值</th>' +
                '</tr></thead><tbody>' +
                codes.map(function (code) {
                    const cn = fundCodeName(code);
                    const info = byFund[code];
                    return info.events.map(function (e) {
                        return '<tr class="hover:bg-gray-50">' +
                            '<td class="px-3 py-2 border-b text-center font-medium text-xs">' + cn.code + ' ' + (cn.name || '') + '</td>' +
                            '<td class="px-3 py-2 border-b text-center font-mono text-xs">' + e.dateStr + '</td>' +
                            '<td class="px-3 py-2 border-b text-center text-amber-700">' + e.proceeds.toFixed(2) + ' 元</td>' +
                            '<td class="px-3 py-2 border-b text-center">' + (e.ratio != null ? (e.ratio * 100).toFixed(0) + '%' : '—') + '</td>' +
                            '<td class="px-3 py-2 border-b text-center font-mono text-xs">' + e.nav.toFixed(4) + '</td>' +
                            '</tr>';
                    }).join('');
                }).join('') +
                '</tbody></table></div>';
        })()
        : null;
    const arDetailHtml = backtestResult.hasActiveRedeemPlan
        ? (function () {
            const byFund = backtestResult.activeRedeemByFund || {};
            const codes = Object.keys(byFund);
            if (codes.length === 0) return '<p class="text-gray-500 text-sm">已配置主动赎回，但回测区间内未触发。</p>';
            return '<div class="overflow-x-auto"><table class="min-w-full bg-white border border-gray-200 rounded-lg text-sm">' +
                '<thead class="bg-gray-100"><tr>' +
                '<th class="px-3 py-2 border-b text-center">基金</th>' +
                '<th class="px-3 py-2 border-b text-center">触发日期</th>' +
                '<th class="px-3 py-2 border-b text-center">赎回金额</th>' +
                '<th class="px-3 py-2 border-b text-center">赎回时持有份额</th>' +
                '<th class="px-3 py-2 border-b text-center">方式</th>' +
                '<th class="px-3 py-2 border-b text-center">净值</th>' +
                '</tr></thead><tbody>' +
                codes.map(function (code) {
                    const cn = fundCodeName(code);
                    const info = byFund[code];
                    return info.events.map(function (e) {
                        const modeTxt = e.mode === 'amount' ? '按金额' : (e.mode === 'shares' ? '按份额' : '按比例');
                        const holdTxt = e.holdShares != null ? e.holdShares.toFixed(2) : '—';
                        return '<tr class="hover:bg-gray-50">' +
                            '<td class="px-3 py-2 border-b text-center font-medium text-xs">' + cn.code + ' ' + (cn.name || '') + '</td>' +
                            '<td class="px-3 py-2 border-b text-center font-mono text-xs">' + e.dateStr + '</td>' +
                            '<td class="px-3 py-2 border-b text-center text-orange-700">' + e.proceeds.toFixed(2) + ' 元</td>' +
                            '<td class="px-3 py-2 border-b text-center font-mono text-xs">' + holdTxt + ' 份</td>' +
                            '<td class="px-3 py-2 border-b text-center text-xs">' + modeTxt + '</td>' +
                            '<td class="px-3 py-2 border-b text-center font-mono text-xs">' + e.nav.toFixed(4) + '</td>' +
                            '</tr>';
                    }).join('');
                }).join('') +
                '</tbody></table></div>';
        })()
        : null;
    // 汇总两块执行明细为一个卡片（无止盈无主动赎回时整卡隐藏）
    const redeemDetailHtml = (sgDetailHtml || arDetailHtml)
        ? rptPlain('止盈 / 主动赎回执行明细',
            (sgDetailHtml ? '<div class="mb-4"><div class="text-sm font-semibold text-amber-800 mb-2">目标止盈</div>' + sgDetailHtml + '</div>' : '') +
            (arDetailHtml ? '<div><div class="text-sm font-semibold text-orange-700 mb-2">主动赎回</div>' + arDetailHtml + '</div>' : ''))
        : '';

    const inner = '(' + buildReportInner.toString() + ')();';

    const benchmarkOptions = benchmarksData.map(function (b) {
        return '<option value="' + b.id + '"' + (b.id === currentBenchmarkId ? ' selected' : '') + '>' + b.name + '</option>';
    }).join('');

    const analysisTableInner =
        '                    <div class="overflow-x-auto">\n' +
        '                        <table class="min-w-full bg-white border border-gray-200 rounded-lg text-sm">\n' +
        '                            <thead class="bg-gray-100"><tr>\n' +
        '                                <th class="px-4 py-2 border-b text-center">节点</th><th class="px-4 py-2 border-b text-center">日期</th>\n' +
        '                                <th class="px-4 py-2 border-b text-center">时间加权净值</th><th class="px-4 py-2 border-b text-center">基准净值</th>\n' +
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
        // 止盈 / 主动赎回执行明细（有触发时展示，静态表格）
        (redeemDetailHtml ? redeemDetailHtml + '\n' : '') +
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
        // 时间加权净值 vs 比较基准（可折叠）
        rptCollapsible('时间加权净值 vs 比较基准', 'rptNetValue', '<canvas id="netValueChart"></canvas>', netCollapsed) + '\n' +
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

// 导出「定投策略比较」HTML 报告（投资净值曲线，资金加权，起点=1.0）
async function exportScReportHTML() {
    if (!scResults || scResults.length === 0) { alert('请先运行策略比较再导出报告'); return; }
    if (!scChart) { alert('请先运行策略比较再导出报告'); return; }

    // 序列化必要的原始数据（与当前主页面净值曲线口径一致）
    const scRaw = {
        results: scResults.map(function (r, idx) {
            const cn = fundCodeName(r.item.fund);
            const sgLabel = r.item.stopGain !== 'none' ? '·' + SC_STOPGAIN[r.item.stopGain] : '';
            // 止盈/主动赎回：事件仅存交易日下标，序列化下标数组 + 各条目累计金额
            return {
                dates: r.dates.map(function (d) { return d.getTime(); }),
                invests: r.invests || [],
                assets: r.assets || [],
                flows: r.flows || [],
                cashDivs: r.cashDivs || [],
                peakPrincipal: r.peakPrincipal || [],
                mdEventIdx: r.mdEventIdx || [],
                dcaEventIdx: r.dcaEventIdx || [],
                investStrategy: r.item.investStrategy,
                stopGain: r.item.stopGain,
                label: (cn.name || r.item.fund) + '·' + SC_INVEST[r.item.investStrategy] + sgLabel,
                color: SC_COLORS[idx % SC_COLORS.length],
                stopGainEvents: (r.stopGainEvents || []),
                activeRedeemEvents: (r.activeRedeemEvents || []),
                totalRedeemed: r.totalRedeemed || 0,
                activeRedeemed: r.activeRedeemed || 0
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

    // 止盈 / 主动赎回执行明细（按条目列出触发日期列表 + 累计赎回金额）
    const scHasRedeem = scResults.some(function (r) { return (r.stopGainEvents && r.stopGainEvents.length) || (r.activeRedeemEvents && r.activeRedeemEvents.length); });
    const scRedeemDetailHtml = scHasRedeem
        ? (function () {
            const rows = scResults.map(function (r, idx) {
                const cn = fundCodeName(r.item.fund);
                const sgLabel = r.item.stopGain !== 'none' ? '·' + SC_STOPGAIN[r.item.stopGain] : '';
                const name = (cn.name || r.item.fund) + '·' + SC_INVEST[r.item.investStrategy] + sgLabel;
                const sgDates = (r.stopGainEvents || []).map(function (i) { return formatDate(new Date(r.dates[i])); }).join('、');
                const arDates = (r.activeRedeemEvents || []).map(function (ev) { return formatDate(new Date(r.dates[ev.k])); }).join('、');
                const arHold = (r.activeRedeemEvents || []).map(function (ev) { return ev.holdShares != null ? ev.holdShares.toFixed(2) : '—'; }).join('、');
                return '<tr class="hover:bg-gray-50">' +
                    '<td class="px-3 py-2 border-b text-center font-medium text-xs">' + name + '</td>' +
                    '<td class="px-3 py-2 border-b text-center text-xs">' + (sgDates || '—') + '</td>' +
                    '<td class="px-3 py-2 border-b text-center text-amber-700">' + (r.totalRedeemed || 0).toFixed(0) + ' 元</td>' +
                    '<td class="px-3 py-2 border-b text-center text-xs">' + (arDates || '—') + '</td>' +
                    '<td class="px-3 py-2 border-b text-center text-xs">' + (arHold || '—') + ' 份</td>' +
                    '<td class="px-3 py-2 border-b text-center text-orange-700">' + (r.activeRedeemed || 0).toFixed(0) + ' 元</td>' +
                    '</tr>';
            }).join('');
            return '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
                '            <h3 class="text-lg font-semibold text-gray-700 mb-3">止盈 / 主动赎回执行明细</h3>\n' +
                '            <div class="overflow-x-auto"><table class="min-w-full bg-white border border-gray-200 rounded-lg text-sm">\n' +
                '                <thead class="bg-gray-100"><tr>\n' +
                '                    <th class="px-3 py-2 border-b text-center">条目</th>\n' +
                '                    <th class="px-3 py-2 border-b text-center">止盈触发日期</th>\n' +
                '                    <th class="px-3 py-2 border-b text-center">止盈累计金额</th>\n' +
                '                    <th class="px-3 py-2 border-b text-center">主动赎回日期</th>\n' +
                '                    <th class="px-3 py-2 border-b text-center">赎回时持有份额</th>\n' +
                '                    <th class="px-3 py-2 border-b text-center">主动赎回累计金额</th>\n' +
                '                </tr></thead>\n' +
                '                <tbody>' + rows + '</tbody>\n' +
                '            </table></div>\n' +
                '        </div>\n';
        })()
        : '';

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
        '        <p class="text-center text-sm text-gray-400 mb-8">生成日期：' + dateStr + '（曲线图支持鼠标悬停查看各点数值）</p>\n' +
        (metricsHtml ? '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '            <h3 class="text-lg font-semibold text-gray-700 mb-3">策略指标对比</h3>\n' +
            '            <div class="grid grid-cols-2 md:grid-cols-4 gap-4">' + metricsHtml + '</div>\n' +
            '        </div>\n' : '') +
        '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
        '            <h3 class="text-lg font-semibold mb-3 text-gray-700 flex items-center justify-between flex-wrap gap-2">\n' +
        '                <span id="scChartTitle">投资净值曲线（资金加权，起点=1.0）</span>\n' +
        '                <span class="inline-flex flex-row gap-2 items-center justify-end">\n' +
        '                    <span class="inline-flex rounded-lg border border-gray-300 overflow-hidden text-sm">\n' +
        '                        <button onclick="__setXMode(\'month\')" id="scXMonth" class="px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100">按持有期月数对齐</button>\n' +
        '                        <button onclick="__setXMode(\'date\')" id="scXDate" class="px-3 py-1.5 bg-blue-600 text-white">按日期</button>\n' +
        '                    </span>\n' +
        '                </span>\n' +
        '            </h3>\n' +
        '            <div class="flex items-center justify-end gap-2 mb-3">\n' +
        '                <span class="inline-flex justify-end min-w-[440px]">\n' +
        '                    <span id="scNetToggle" class="inline-flex rounded-lg border border-gray-300 overflow-hidden text-sm">\n' +
        '                        <button onclick="__setNetMode(\'mw\')" id="scNetMw" class="px-3 py-1.5 min-w-[84px] bg-blue-600 text-white">资金加权</button>\n' +
        '                        <button onclick="__setNetMode(\'portfolio\')" id="scNetPf" class="px-3 py-1.5 min-w-[84px] bg-white text-gray-700 hover:bg-gray-100">时间加权净值</button>\n' +
        '                    </span>\n' +
        '                    <span id="scXirrWindowGroup" class="inline-flex rounded-lg border border-gray-300 overflow-hidden text-sm" style="display:none;" title="可多选叠加：累计 / 1年 / 3年 / 5年滚动 XIRR">\n' +
        '                        <button onclick="__toggleWin(\'cum\')" id="scXirrCum" class="px-3 py-1.5 min-w-[84px] bg-blue-600 text-white">累计</button>\n' +
        '                        <button onclick="__toggleWin(\'y1\')" id="scXirrY1" class="px-3 py-1.5 min-w-[84px] bg-white text-gray-700 hover:bg-gray-100">1年滚动</button>\n' +
        '                        <button onclick="__toggleWin(\'y3\')" id="scXirrY3" class="px-3 py-1.5 min-w-[84px] bg-white text-gray-700 hover:bg-gray-100">3年滚动</button>\n' +
        '                        <button onclick="__toggleWin(\'y5\')" id="scXirrY5" class="px-3 py-1.5 min-w-[84px] bg-white text-gray-700 hover:bg-gray-100">5年滚动</button>\n' +
        '                    </span>\n' +
        '                </span>\n' +
        '                <span class="inline-flex rounded-lg border border-gray-300 overflow-hidden text-sm">\n' +
        '                    <button onclick="__setYMode(\'net\')" id="scYNet" class="px-3 py-1.5 bg-blue-600 text-white">净值</button>\n' +
        '                    <button onclick="__setYMode(\'xirr\')" id="scYXirr" class="px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100">XIRR年化</button>\n' +
        '                </span>\n' +
        '            </div>\n' +
        '            <div style="height: 420px;"><canvas id="scChartExport"></canvas></div>\n' +
        '            <p class="text-xs text-gray-500 mt-2" id="scChartNote">* 采用资金加权的"这一笔投资净值"（起点=1.0）：当日净值 = 当日总资产 ÷ 截至当日的累计已投入本金。</p>\n' +
        '        </div>\n' +
        (tableHtml ? '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '            <h3 class="text-lg font-semibold text-gray-700 mb-3">策略对比明细</h3>\n' +
            '            <div class="overflow-x-auto">' + tableHtml + '</div>\n' +
            '        </div>\n' : '') +
        (scRedeemDetailHtml ? scRedeemDetailHtml : '') +
        (rulesHtml ? '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '            <h3 class="text-lg font-semibold text-gray-700 mb-3">策略规则说明</h3>\n' +
            rulesHtml + '\n' +
            '        </div>\n' : '') +
        (errorsHtml ? '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
            '            <h3 class="text-lg font-semibold text-gray-700 mb-3">提示与说明</h3>\n' +
            errorsHtml + '\n' +
            '        </div>\n' : '') +
        '        <p class="text-center text-xs text-gray-400 mt-4">本报告由「基金定投测算工具」生成。</p>\n' +
        '    </div>\n' +
        '    <scr' + 'ipt>window.__sc__ = ' + JSON.stringify(scRaw).replace(/<\/script>/gi, '<\\/script>') + ';</scr' + 'ipt>\n' +
        '    <scr' + 'ipt>\n' +
        '        var __sc = window.__sc__;\n' +
        '        var __chart = null;\n' +
        '        var __xMode = "date";            // month | date\n' +
        '        var __netMode = "mw";           // mw 资金加权 | portfolio 时间加权净值\n' +
        '        var __yMode = "net";            // net | xirr\n' +
        '        var __wins = ["cum"];           // XIRR 勾选窗口\n' +
        '        var __WINDOWS = [\n' +
        '            { key: "cum", label: "累计", years: null, dash: [], alpha: 1.0 },\n' +
        '            { key: "y1", label: "1年", years: 1, dash: [2, 2], alpha: 0.9 },\n' +
        '            { key: "y3", label: "3年", years: 3, dash: [6, 3], alpha: 0.8 },\n' +
        '            { key: "y5", label: "5年", years: 5, dash: [10, 4], alpha: 0.7 }\n' +
        '        ];\n' +
        '        var __CLAMP_MIN = -100, __CLAMP_MAX = 100;\n' +
        '        function __fmt(d) { var t = new Date(d); if (isNaN(t)) return d; var y = t.getFullYear(); var m = ("0" + (t.getMonth() + 1)).slice(-2); var day = ("0" + t.getDate()).slice(-2); return y + "-" + m + "-" + day; }\n' +
        '        function __hexToRgba(hex, alpha) { var h = (hex || "#000000").replace("#", ""); var r = parseInt(h.substring(0, 2), 16), g = parseInt(h.substring(2, 4), 16), b = parseInt(h.substring(4, 6), 16); return "rgba(" + r + "," + g + "," + b + "," + alpha + ")"; }\n' +
        '        // 时间加权净值（份额法/TWR）：与主工具口径一致\n' +
        '        function __portfolioNetValues(assets, flows) {\n' +
        '            var nv = [];\n' +
        '            for (var i = 0; i < assets.length; i++) {\n' +
        '                if (i === 0) { nv.push(assets[i] > 0 ? 1.0 : null); continue; }\n' +
        '                var prev = assets[i - 1];\n' +
        '                if (prev > 0) nv.push((nv[i - 1] == null ? 1.0 : nv[i - 1]) * ((assets[i] - (flows[i] || 0)) / prev));\n' +
        '                else nv.push(assets[i] > 0 ? 1.0 : null);\n' +
        '            }\n' +
        '            return nv;\n' +
        '        }\n' +
        '        // 逐日资金加权收益率（XIRR 年化，%），与主工具 rollingXirr 同口径\n' +
        '        function __rollingXirr(r, years) {\n' +
        '            var N = r.dates.length;\n' +
        '            var out = new Array(N).fill(null);\n' +
        '            if (N < 2) return out;\n' +
        '            var inv = r.invests || [], cashDivs = r.cashDivs || [], assets = r.assets || [];\n' +
        '            var netFlow = new Array(N), holdVal = new Array(N);\n' +
        '            var prevCash = 0;\n' +
        '            for (var j = 0; j < N; j++) {\n' +
        '                var realized = cashDivs[j] - prevCash; prevCash = cashDivs[j];\n' +
        '                netFlow[j] = -(inv[j] || 0) + (realized > 0 ? realized : 0);\n' +
        '                holdVal[j] = assets[j] - cashDivs[j];\n' +
        '            }\n' +
        '            var YEAR_MS = 1000 * 60 * 60 * 24 * 365;\n' +
        '            var ts = r.dates;\n' +
        '            var winStart = new Array(N).fill(0);\n' +
        '            if (years != null) {\n' +
        '                var s = 0;\n' +
        '                for (var i = 0; i < N; i++) {\n' +
        '                    var cut = new Date(r.dates[i]); cut.setFullYear(cut.getFullYear() - years);\n' +
        '                    var cutTs = cut.getTime();\n' +
        '                    while (s < i && ts[s] < cutTs) s++;\n' +
        '                    winStart[i] = s;\n' +
        '                }\n' +
        '            }\n' +
        '            var openVal = function (i) { var si = winStart[i]; return si > 0 ? holdVal[si - 1] : 0; };\n' +
        '            var npv = function (rate, i) {\n' +
        '                var si = winStart[i], base = ts[si];\n' +
        '                var frac = function (jj) { return (ts[jj] - base) / YEAR_MS; };\n' +
        '                var sum = -openVal(i);\n' +
        '                for (var j = si; j <= i; j++) sum += netFlow[j] / Math.pow(1 + rate, frac(j));\n' +
        '                return sum + holdVal[i] / Math.pow(1 + rate, frac(i));\n' +
        '            };\n' +
        '            var npvDeriv = function (rate, i) {\n' +
        '                var si = winStart[i], base = ts[si];\n' +
        '                var frac = function (jj) { return (ts[jj] - base) / YEAR_MS; };\n' +
        '                var d = 0;\n' +
        '                for (var j = si; j <= i; j++) d -= netFlow[j] * frac(j) / Math.pow(1 + rate, frac(j) + 1);\n' +
        '                d -= holdVal[i] * frac(i) / Math.pow(1 + rate, frac(i) + 1);\n' +
        '                return d;\n' +
        '            };\n' +
        '            var prevRate = 0.1, cumInvest = inv[0] || 0;\n' +
        '            for (var i = 1; i < N; i++) {\n' +
        '                cumInvest += (inv[i] || 0);\n' +
        '                if (cumInvest <= 0) continue;\n' +
        '                if (years != null) {\n' +
        '                    var si = winStart[i];\n' +
        '                    if (si >= i) continue;\n' +
        '                    var winInv = 0;\n' +
        '                    for (var jj = si; jj <= i; jj++) winInv += (inv[jj] || 0);\n' +
        '                    if (openVal(i) <= 0 && winInv <= 0) continue;\n' +
        '                }\n' +
        '                var tol = 1e-7, rate = prevRate, ok = false;\n' +
        '                for (var it = 0; it < 60; it++) {\n' +
        '                    var v = npv(rate, i), dv = npvDeriv(rate, i);\n' +
        '                    if (Math.abs(v) < tol) { ok = true; break; }\n' +
        '                    if (Math.abs(dv) < tol) break;\n' +
        '                    rate -= v / dv;\n' +
        '                    if (rate <= -0.9999) rate = -0.9999 + 1e-6;\n' +
        '                    if (!isFinite(rate)) break;\n' +
        '                }\n' +
        '                if (!ok) {\n' +
        '                    var lo = -0.9999, hi = 100, fLo = npv(lo, i), fHi = npv(hi, i);\n' +
        '                    if (fLo * fHi <= 0) {\n' +
        '                        for (var it2 = 0; it2 < 100; it2++) {\n' +
        '                            var mid = (lo + hi) / 2, fM = npv(mid, i);\n' +
        '                            if (Math.abs(fM) < tol) { rate = mid; ok = true; break; }\n' +
        '                            if (fLo * fM < 0) { hi = mid; } else { lo = mid; }\n' +
        '                        }\n' +
        '                        if (!ok) rate = (lo + hi) / 2;\n' +
        '                    } else rate = null;\n' +
        '                }\n' +
        '                out[i] = (rate == null || !isFinite(rate)) ? null : Math.max(__CLAMP_MIN, Math.min(__CLAMP_MAX, rate * 100));\n' +
        '                if (out[i] != null) prevRate = rate;\n' +
        '            }\n' +
        '            return out;\n' +
        '        }\n' +
        '        function __scXirrSeries(r, key) {\n' +
        '            var w = null;\n' +
        '            for (var i = 0; i < __WINDOWS.length; i++) if (__WINDOWS[i].key === key) { w = __WINDOWS[i]; break; }\n' +
        '            if (!w) w = __WINDOWS[0];\n' +
        '            if (!r.__xirr) r.__xirr = {};\n' +
        '            if (!r.__xirr[key]) r.__xirr[key] = __rollingXirr(r, w.years);\n' +
        '            return r.__xirr[key];\n' +
        '        }\n' +
        '        // 按日期模式：生成规律化刻度，对齐到各周期的起始日（1月1日 / 4月1日 / 7月1日 / 10月1日 / 月初等）\n' +
        '        function __dateTickInfo(xMin, xMax) {\n' +
        '            if (!(xMin != null && xMax != null && isFinite(xMin) && isFinite(xMax) && xMax > xMin)) return { ticks: [], unit: "month" };\n' +
        '            var YEAR = 365.2425 * 24 * 60 * 60 * 1000;\n' +
        '            var spanYears = (xMax - xMin) / YEAR;\n' +
        '            var unit, stepMonths;\n' +
        '            if (spanYears < 0.75) { unit = "month"; stepMonths = 1; }\n' +
        '            else if (spanYears <= 2) { unit = "quarter"; stepMonths = 3; }\n' +
        '            else if (spanYears <= 5) { unit = "half"; stepMonths = 6; }\n' +
        '            else if (spanYears <= 12) { unit = "year"; stepMonths = 12; }\n' +
        '            else { unit = "multiYear"; stepMonths = spanYears > 24 ? 60 : 24; }\n' +
        '            var start = new Date(xMin);\n' +
        '            var y = start.getFullYear(), m = start.getMonth();\n' +
        '            if (unit !== "month") {\n' +
        '                // 对齐到各周期的起始月：year/multiYear→1月(0)；half→1月(0)或7月(6)；quarter→1/4/7/10月\n' +
        '                var alignMonth;\n' +
        '                if (unit === "year" || unit === "multiYear") alignMonth = 0;\n' +
        '                else if (unit === "half") alignMonth = (m < 6) ? 0 : 6;\n' +
        '                else alignMonth = Math.floor(m / 3) * 3;\n' +
        '                // 若该起始日早于 xMin 则顺延一个 step\n' +
        '                if (new Date(y, alignMonth, 1).getTime() < xMin) { m = alignMonth + stepMonths; while (m > 11) { m -= 12; y += 1; } }\n' +
        '                else { m = alignMonth; }\n' +
        '            }\n' +
        '            var ticks = [];\n' +
        '            for (var i = 0; i < 2000; i++) {\n' +
        '                var t = new Date(y, m, 1).getTime();\n' +
        '                if (t > xMax) break;\n' +
        '                if (t >= xMin) ticks.push(t);\n' +
        '                m += stepMonths; while (m > 11) { m -= 12; y += 1; }\n' +
        '            }\n' +
        '            if (ticks.length < 2) {\n' +
        '                var res = [];\n' +
        '                var dd = new Date(xMin);\n' +
        '                for (var j = 0; j < 12; j++) { var tt = new Date(dd.getFullYear(), dd.getMonth() + j, 1).getTime(); if (tt > xMax) break; if (tt >= xMin) res.push(tt); }\n' +
        '                return { ticks: res, unit: "month" };\n' +
        '            }\n' +
        '            return { ticks: ticks, unit: unit };\n' +
        '        }\n' +
        '        function __fmtDateTick(ts, unit) {\n' +
        '            var d = new Date(ts);\n' +
        '            var y = d.getFullYear(), m = d.getMonth();\n' +
        '            if (unit === "year" || unit === "multiYear") return String(y);\n' +
        '            if (unit === "half") return m === 0 ? y + "H1" : y + "H2";\n' +
        '            if (unit === "quarter") return y + "Q" + (Math.floor(m / 3) + 1);\n' +
        '            return y + "-" + String(m + 1).padStart(2, "0");\n' +
        '        }\n' +
        '        function __btnOn(id) { var el = document.getElementById(id); if (el) el.className = "px-3 py-1.5 min-w-[84px] bg-blue-600 text-white"; }\n' +
        '        function __btnOff(id) { var el = document.getElementById(id); if (el) el.className = "px-3 py-1.5 min-w-[84px] bg-white text-gray-700 hover:bg-gray-100"; }\n' +
        '        function __btnXOn(id) { var el = document.getElementById(id); if (el) el.className = "px-3 py-1.5 bg-blue-600 text-white"; }\n' +
        '        function __btnXOff(id) { var el = document.getElementById(id); if (el) el.className = "px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100"; }\n' +
        '        function __syncBtns() {\n' +
        '            if (__xMode === "date") { __btnXOn("scXDate"); __btnXOff("scXMonth"); } else { __btnXOn("scXMonth"); __btnXOff("scXDate"); }\n' +
        '            if (__netMode === "mw") { __btnOn("scNetMw"); __btnOff("scNetPf"); } else { __btnOn("scNetPf"); __btnOff("scNetMw"); }\n' +
        '            if (__yMode === "net") { __btnOn("scYNet"); __btnOff("scYXirr"); } else { __btnOn("scYXirr"); __btnOff("scYNet"); }\n' +
        '            var sg = document.getElementById("scXirrWindowGroup"), nt = document.getElementById("scNetToggle");\n' +
        '            if (sg) sg.style.display = __yMode === "xirr" ? "" : "none";\n' +
        '            if (nt) nt.style.display = __yMode === "net" ? "" : "none";\n' +
        '            __WINDOWS.forEach(function (w) {\n' +
        '                if (__wins.indexOf(w.key) !== -1) __btnOn("scXirr" + w.key.charAt(0).toUpperCase() + w.key.slice(1));\n' +
        '                else __btnOff("scXirr" + w.key.charAt(0).toUpperCase() + w.key.slice(1));\n' +
        '            });\n' +
        '        }\n' +
        '        function __activeWins() {\n' +
        '            var out = [];\n' +
        '            for (var i = 0; i < __WINDOWS.length; i++) if (__wins.indexOf(__WINDOWS[i].key) !== -1) out.push(__WINDOWS[i]);\n' +
        '            return out;\n' +
        '        }\n' +
        '        function __build() {\n' +
        '            var ds = [], isXirr = __yMode === "xirr", activeWins = isXirr ? __activeWins() : [null];\n' +
        '            var hasStopGain = __sc.results.some(function (x) { return x.stopGain && x.stopGain !== "none"; });\n' +
        '            __sc.results.forEach(function (r) {\n' +
        '                var startTs = r.dates[0];\n' +
        '                var inv = r.invests || [], assets = r.assets || [], flows = r.flows || [], cashDivs = r.cashDivs || [];\n' +
        '                var isStopGain = r.stopGain && r.stopGain !== "none";\n' +
        '                var sgSet = isStopGain && r.stopGainEvents ? new Set(r.stopGainEvents) : null;\n' +
        '                var mdSet = (r.investStrategy === "7" && r.mdEventIdx) ? new Set(r.mdEventIdx) : null;\n' +
        '                var dcaSet = (r.investStrategy !== "7" && r.dcaEventIdx) ? new Set(r.dcaEventIdx) : null;\n' +
        '                var pnvArr = (__netMode === "portfolio") ? __portfolioNetValues(assets, flows) : null;\n' +
        '                var color = r.color;\n' +
        '                var cnName = r.label.split("·")[0];\n' +
        '                activeWins.forEach(function (win, wi) {\n' +
        '                    var wantSg = wi === 0;\n' +
        '                    var rx = isXirr ? __scXirrSeries(r, win.key) : null;\n' +
        '                    var cum = 0, pts = [], sgPts = [], mdPts = [], dcaPts = [];\n' +
        '                    r.dates.forEach(function (ts, i) {\n' +
        '                        cum += (inv[i] || 0);\n' +
        '                        var months = (ts - startTs) / (1000 * 60 * 60 * 24 * 30.4375);\n' +
        '                        var yVal;\n' +
        '                        if (isXirr) { var v = rx[i]; if (v == null) return; yVal = +v.toFixed(2); }\n' +
        '                        else {\n' +
        '                            var nv;\n' +
        '                            if (__netMode === "portfolio") { nv = pnvArr ? pnvArr[i] : null; }\n' +
        '                            else if (isStopGain) { var peak = (r.peakPrincipal || [])[i]; nv = peak > 0 ? 1 + (assets[i] - cum) / peak : null; }\n' +
        '                            else { nv = cum > 0 ? assets[i] / cum : null; }\n' +
        '                            if (nv == null) return; yVal = +nv.toFixed(4);\n' +
        '                        }\n' +
        '                        var xVal = __xMode === "date" ? ts : +months.toFixed(2);\n' +
        '                        pts.push({ x: xVal, y: yVal });\n' +
        '                        if (wantSg && sgSet && sgSet.has(i)) sgPts.push({ x: xVal, y: yVal });\n' +
        '                        if (wantSg && mdSet && mdSet.has(i)) mdPts.push({ x: xVal, y: yVal });\n' +
        '                        if (wantSg && dcaSet && dcaSet.has(i)) dcaPts.push({ x: xVal, y: yVal });\n' +
        '                    });\n' +
        '                    var lineColor = isXirr && win.alpha < 1 ? __hexToRgba(color, win.alpha) : color;\n' +
        '                    var baseLabel = r.label.split("·").length > 1 ? r.label : r.label;\n' +
        '                    ds.push({ label: isXirr && activeWins.length > 1 ? baseLabel + "·" + win.label : baseLabel, data: pts, borderColor: lineColor, backgroundColor: lineColor, borderDash: isXirr ? win.dash : [], borderWidth: 2, pointRadius: 0, tension: 0.1, fill: false });\n' +
        '                    if (sgPts.length) ds.push({ label: cnName + "·止盈点", data: sgPts, borderColor: color, backgroundColor: color, pointStyle: "circle", pointRadius: 4, pointHoverRadius: 6, pointBorderColor: "#ffffff", pointBorderWidth: 1.5, showLine: false, fill: false, isStopGainMarker: true });\n' +
        '                    if (mdPts.length) ds.push({ label: cnName + "·回撤加仓", data: mdPts, borderColor: "#2563eb", backgroundColor: "#2563eb", pointStyle: "triangle", pointRadius: 7, pointHoverRadius: 9, showLine: false, fill: false, isMdMarker: true });\n' +
        '                    if (dcaPts.length) ds.push({ label: cnName + "·普通定投", data: dcaPts, borderColor: "#8b5cf6", backgroundColor: "#8b5cf6", pointStyle: "circle", pointRadius: 3, pointHoverRadius: 5, showLine: false, fill: false, isDcaMarker: true });\n' +
        '                });\n' +
        '            });\n' +
        '            return { ds: ds, isXirr: isXirr, activeWins: activeWins, hasStopGain: hasStopGain };\n' +
        '        }\n' +
'        function __showError(msg) {\n' +
        '            try { var c = document.getElementById("scChartExport"); if (c && c.parentNode) c.parentNode.innerHTML = \'<p class="text-rose-600 text-sm">图表渲染失败：\' + msg + \'</p>\'; } catch(e2) {}\n' +
        '        }\n' +
        '        function __render() {\n' +
        '            try {\n' +
        '            var built = __build(), ds = built.ds;\n' +
        '            var xMin = Infinity, xMax = -Infinity;\n' +
        '            ds.forEach(function (d) { d.data.forEach(function (p) { if (p.x != null && isFinite(p.x)) { if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x; } }); });\n' +
        '            if (!isFinite(xMin)) { xMin = undefined; xMax = undefined; }\n' +
        '            var tickInfo = (__xMode === "date" && xMin != null) ? __dateTickInfo(xMin, xMax) : null;\n' +
        '            if (__chart) { try { __chart.destroy(); } catch(e) {} __chart = null; }\n' +
        '            var canvasEl = document.getElementById("scChartExport");\n' +
        '            if (!canvasEl) { __showError("找不到 canvas 元素"); return; }\n' +
        '            var ctx = canvasEl.getContext("2d");\n' +
        '            var yTitle = built.isXirr\n' +
        '                ? "资金加权收益率 XIRR 年化（%）· " + built.activeWins.map(function (w) { return w.label; }).join(" / ")\n' +
        '                : (__netMode === "portfolio"\n' +
        '                    ? "时间加权净值（份额法：总资产÷总份额，起点=1.0）"\n' +
        '                    : (built.hasStopGain\n' +
        '                        ? "投资净值（普通策略=总资产/累计投入；止盈策略=1+最大本金收益率，起点=1.0）"\n' +
        '                        : "投资净值（资金加权，总资产/累计投入，起点=1.0）"));\n' +
        '            __chart = new Chart(ctx, {\n' +
        '                type: "line", data: { datasets: ds }, options: {\n' +
        '                    responsive: true, maintainAspectRatio: false,\n' +
        '                    interaction: { mode: "nearest", intersect: false },\n' +
        '                    scales: {\n' +
        '                        x: { type: "linear", min: xMin, max: xMax,\n' +
        '                            title: { display: true, text: __xMode === "date" ? "日期" : "持有期（月）" },\n' +
'                            ticks: __xMode === "date"\n' +
'                                ? { autoSkip: false, callback: function (v) {\n' +
'                                      var t = tickInfo && tickInfo.ticks.indexOf(+v.toFixed(0)) !== -1 ? +v.toFixed(0) : null;\n' +
'                                      return t != null ? __fmtDateTick(t, tickInfo.unit) : "";\n' +
'                                  } }\n' +
'                                : { maxTicksLimit: 12, callback: function (v) { return v + "月"; } },\n' +
'                            afterBuildTicks: __xMode === "date" ? function (axis) {\n' +
'                                if (tickInfo && tickInfo.ticks.length) axis.ticks = tickInfo.ticks.map(function (v) { return { value: v }; });\n' +
'                            } : undefined\n' +
'                        },\n' +
'                        y: { title: { display: true, text: yTitle } }\n' +
        '                    },\n' +
        '                    plugins: {\n' +
        '                        legend: { position: "bottom" },\n' +
        '                        tooltip: { callbacks: {\n' +
        '                            title: function (items) { return items.length ? (__xMode === "date" ? __fmt(items[0].parsed.x) : (items[0].parsed.x + " 月")) : ""; },\n' +
        '                            label: function (item) {\n' +
        '                                if (item.dataset && item.dataset.isStopGainMarker) return item.dataset.label;\n' +
        '                                if (item.dataset && item.dataset.isMdMarker) return item.dataset.label;\n' +
'                                if (item.dataset && item.dataset.isDcaMarker) return item.dataset.label;\n' +
'                                return built.isXirr ? item.dataset.label + "：" + item.parsed.y.toFixed(2) + "%" : item.parsed.y.toFixed(4);\n' +
'                            }\n' +
'                        } }\n' +
'                    }\n' +
'                }\n' +
'            });\n' +
'            var title = document.getElementById("scChartTitle");\n' +
'            if (title) title.textContent = built.isXirr\n' +
'                ? "投资收益率曲线（资金加权 XIRR 年化，%）· " + built.activeWins.map(function (w) { return w.label; }).join(" / ")\n' +
'                : "投资净值曲线（资金加权，起点=1.0）";\n' +
'            var note = document.getElementById("scChartNote");\n' +
'            if (note) note.textContent = built.isXirr\n' +
'                ? "* 资金加权 XIRR 年化：每日净外部现金流 = −当日投入 + 当日落袋现金；期末只计剩余持仓市值。曲线限幅 ±100%。可切换按持有期月数对齐/按日期。"\n' +
'                : (__netMode === "portfolio"\n' +
'                    ? "* 时间加权净值（份额法）：新投入按当时时间加权净值折算份额，加仓只增份额、不改变净值。可切换按持有期月数对齐/按日期。"\n' +
'                    : "* 资金加权净值（起点=1.0）：当日净值 = 当日总资产 ÷ 截至当日的累计已投入本金。可切换按持有期月数对齐/按日期。");\n' +
'            __syncBtns();\n' +
'            } catch (err) { __showError(err && err.message ? err.message : String(err)); }\n' +
'        }\n' +
'        function __setXMode(mode) { try { __xMode = mode; __render(); } catch(e) { __showError(e.message); } }\n' +
'        function __setNetMode(mode) { try { __netMode = mode; __render(); } catch(e) { __showError(e.message); } }\n' +
'        function __setYMode(mode) { try { __yMode = mode; __render(); } catch(e) { __showError(e.message); } }\n' +
'        function __toggleWin(key) {\n' +
'            try {\n' +
'            var idx = __wins.indexOf(key);\n' +
'            if (idx !== -1) __wins.splice(idx, 1); else __wins.push(key);\n' +
'            if (__wins.length === 0) __wins.push("cum");\n' +
'            __render();\n' +
'            } catch(e) { __showError(e.message); }\n' +
'        }\n' +
'        try { __render(); } catch(e) { __showError(e.message); }\n' +
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

// 导出「指数估值比较」HTML 报告（单指数点位图 + 双指数比值图，Chart.js 内联，纯离线）
async function exportValuationReportHTML() {
    const benchmarksAll = await db.benchmarks.toArray();
    if (benchmarksAll.length === 0) { alert('无基准数据，无法导出估值报告'); return; }
    const benchmarksData = benchmarksAll.map(function (b) { return { id: b.id, name: b.name, data: b.data }; });
    const vs = collectValuationState();
    const dateStr = formatDate(new Date());

    // 内联 Chart.js（优先内联，失败则外链 CDN）
    let chartJsSrc = '';
    try { chartJsSrc = await (await fetch(CHART_JS_CDN)).text(); } catch (e) { chartJsSrc = ''; }
    let chartJsBlock;
    if (chartJsSrc) chartJsBlock = '<scr' + 'ipt>' + chartJsSrc.replace(/<\/script>/gi, '<\\/script>') + '</scr' + 'ipt>';
    else chartJsBlock = '<scr' + 'ipt src="' + CHART_JS_CDN + '"></scr' + 'ipt>';

    const payload = { benchmarks: benchmarksData, vs: vs };
    const inner = '(' + buildValuationReportInner.toString() + ')();';

    const html = '<!DOCTYPE html>\n' +
        '<html lang="zh-CN">\n' +
        '<head>\n' +
        '    <meta charset="UTF-8">\n' +
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n' +
        '    <title>指数估值比较报告</title>\n' +
        '    <scr' + 'ipt src="' + TAILWIND_CDN + '"></scr' + 'ipt>\n' +
        '    ' + chartJsBlock + '\n' +
        '</head>\n' +
        '<body class="bg-gray-50 min-h-screen p-6">\n' +
        '    <div class="max-w-6xl mx-auto">\n' +
        '        <h1 class="text-3xl font-bold text-center mb-8 text-gray-800">📊 指数估值比较报告</h1>\n' +
        '        <p class="text-center text-sm text-gray-400 mb-8">生成日期：' + dateStr + '（图表支持鼠标悬停查看各点数值）</p>\n' +
        '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
        '            <h3 class="text-lg font-semibold text-gray-700 mb-3">估值参数</h3>\n' +
        '            <div class="grid grid-cols-1 md:grid-cols-2 gap-6" id="valParams"></div>\n' +
        '        </div>\n' +
        '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
        '            <div class="flex flex-wrap items-center justify-between mb-3">\n' +
        '                <h3 class="text-lg font-semibold text-gray-700">单指数点位 · 滚动百分位</h3>\n' +
        '                <div class="flex flex-wrap items-center gap-2 text-sm">\n' +
        '                    <span class="text-gray-500">展示区间</span>\n' +
        '                    <input type="date" id="valSingleShowStart" class="border rounded px-2 py-1 text-sm">\n' +
        '                    <span class="text-gray-400">~</span>\n' +
        '                    <input type="date" id="valSingleShowEnd" class="border rounded px-2 py-1 text-sm">\n' +
        '                    <button onclick="applyValuationSingleRange()" class="px-3 py-1 bg-indigo-600 hover:bg-indigo-700 text-white rounded text-sm">应用</button>\n' +
        '                </div>\n' +
        '            </div>\n' +
        '            <div style="height: 420px;"><canvas id="valSingleExport"></canvas></div>\n' +
        '            <p class="text-xs text-gray-500 mt-2" id="valSingleNote"></p>\n' +
        '        </div>\n' +
        '        <div class="bg-white p-6 rounded-xl shadow-md mb-6">\n' +
        '            <div class="flex flex-wrap items-center justify-between mb-3">\n' +
        '                <h3 class="text-lg font-semibold text-gray-700">双指数比值 · 滚动百分位</h3>\n' +
        '                <div class="flex flex-wrap items-center gap-2 text-sm">\n' +
        '                    <span class="text-gray-500">展示区间</span>\n' +
        '                    <input type="date" id="valRatioShowStart" class="border rounded px-2 py-1 text-sm">\n' +
        '                    <span class="text-gray-400">~</span>\n' +
        '                    <input type="date" id="valRatioShowEnd" class="border rounded px-2 py-1 text-sm">\n' +
        '                    <button onclick="applyValuationRatioRange()" class="px-3 py-1 bg-indigo-600 hover:bg-indigo-700 text-white rounded text-sm">应用</button>\n' +
        '                </div>\n' +
        '            </div>\n' +
        '            <div style="height: 420px;"><canvas id="valRatioExport"></canvas></div>\n' +
        '            <p class="text-xs text-gray-500 mt-2" id="valRatioNote"></p>\n' +
        '        </div>\n' +
        '        <p class="text-center text-xs text-gray-400 mt-4">本报告由「基金定投测算工具」生成，数据为历史测算，不构成收益保证。</p>\n' +
        '    </div>\n' +
        '    <scr' + 'ipt>window.__V__ = ' + JSON.stringify(payload).replace(/<\/script>/gi, '<\\/script>') + ';</scr' + 'ipt>\n' +
        '    <scr' + 'ipt>' + inner + '</scr' + 'ipt>\n' +
        '</body>\n' +
        '</html>';

    const blob = new Blob([html], { type: 'text/html;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = '估值报告_' + dateStr + '.html';
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
}

// 估值报告内嵌脚本：在报告页内独立运行，复用主工具的估值算法
function buildValuationReportInner() {
    var V = window.__V__;
    var benchmarks = V.benchmarks;
    var vs = V.vs;
    var DAYS = 252;

    function getBm(id) {
        if (id == null || id === '') return null;
        var k = String(id);
        for (var i = 0; i < benchmarks.length; i++) {
            if (String(benchmarks[i].id) === k) return benchmarks[i];
        }
        return null;
    }
    function fmt(v) { return (v == null || v === '') ? '—' : v; }
    function rollingMean(navs, n) {
        var m = Math.max(1, n); var out = new Array(navs.length); var sum = 0;
        for (var i = 0; i < navs.length; i++) { sum += navs[i]; if (i >= m) sum -= navs[i - m]; var cnt = Math.min(i + 1, m); out[i] = sum / cnt; }
        return out;
    }
    function rollingPercentile(navs, windowN) {
        var n = navs.length; var sorted = []; var out = new Array(n); var windowStart = 0;
        for (var i = 0; i < n; i++) {
            var val = navs[i];
            var lo = 0, hi = sorted.length;
            while (lo < hi) { var mid = (lo + hi) >> 1; if (sorted[mid] < val) lo = mid + 1; else hi = mid; }
            sorted.splice(lo, 0, val);
            if (i - windowStart + 1 > windowN) {
                var evict = navs[windowStart++];
                var elo = 0, ehi = sorted.length;
                while (elo < ehi) { var emid = (elo + ehi) >> 1; if (sorted[emid] < evict) elo = emid + 1; else ehi = emid; }
                sorted.splice(elo, 1);
            }
            out[i] = (sorted.length < windowN) ? null : (lo / sorted.length) * 100;
        }
        return out;
    }
    function commonDateRange(bmA, bmB) {
        var datesA = bmA.data.map(function (d) { return d.date; }).sort();
        var setB = {};
        bmB.data.forEach(function (d) { setB[d.date] = 1; });
        var common = datesA.filter(function (dt) { return setB[dt]; });
        if (common.length === 0) return null;
        return { start: common[0], end: common[common.length - 1] };
    }
    function buildRatioSeries(bmA, bmB, start, end) {
        var mapA = {}, mapB = {};
        bmA.data.forEach(function (d) { mapA[d.date] = d.nav; });
        bmB.data.forEach(function (d) { mapB[d.date] = d.nav; });
        var common = Object.keys(mapA).filter(function (dt) { return mapB[dt] != null && dt >= start && dt <= end; }).sort();
        if (common.length === 0) return null;
        var ratioByDate = [];
        for (var i = 0; i < common.length; i++) ratioByDate.push({ date: common[i], r: mapA[common[i]] / mapB[common[i]] });
        return ratioByDate;
    }
    function chartOpts(yTitle) {
        return { responsive: true, maintainAspectRatio: false, interaction: { mode: 'index', intersect: false },
            plugins: { legend: { labels: { boxWidth: 12 } }, tooltip: { mode: 'index', intersect: false } },
            scales: { x: { ticks: { maxTicksLimit: 12 } }, y: { position: 'left', title: { display: true, text: yTitle } },
                y1: { position: 'right', min: 0, max: 100, grid: { drawOnChartArea: false }, title: { display: true, text: '百分位 (%)' } } } };
    }

    // 参数概览
    function paramCard(title, items) {
        var rows = items.map(function (it) {
            return '<div class="flex justify-between py-1 border-b border-gray-100"><span class="text-sm text-gray-500">' + it[0] + '</span><span class="text-sm font-medium text-gray-800">' + it[1] + '</span></div>';
        }).join('');
        return '<div><div class="text-sm font-semibold text-indigo-700 mb-2">' + title + '</div>' + rows + '</div>';
    }
    var singleName = getBm(vs.valSingleIndex) ? getBm(vs.valSingleIndex).name : fmt(vs.valSingleIndex);
    var ratioAName = getBm(vs.valRatioA) ? getBm(vs.valRatioA).name : fmt(vs.valRatioA);
    var ratioBName = getBm(vs.valRatioB) ? getBm(vs.valRatioB).name : fmt(vs.valRatioB);
    document.getElementById('valParams').innerHTML =
        paramCard('单指数', [
            ['指数', singleName],
            ['点位区间', (fmt(vs.valSingleStart) + ' ~ ' + fmt(vs.valSingleEnd))],
            ['展示区间', (fmt(vs.valSingleShowStart) + ' ~ ' + fmt(vs.valSingleShowEnd))],
            ['滚动年度', [vs.valSingleRollYears1, vs.valSingleRollYears2, vs.valSingleRollYears3].filter(function (x) { return x && parseFloat(x) > 0; }).join(' / ') || '—'],
            ['百分位均值N', fmt(vs.valSingleMeanN)],
            ['阈值(高/低)', (fmt(vs.valSingleHi) + ' / ' + fmt(vs.valSingleLo))]
        ]) +
        paramCard('双指数', [
            ['指数 A / B', ratioAName + ' / ' + ratioBName],
            ['点位区间', (fmt(vs.valRatioStart) + ' ~ ' + fmt(vs.valRatioEnd))],
            ['展示区间', (fmt(vs.valRatioShowStart) + ' ~ ' + fmt(vs.valRatioShowEnd))],
            ['滚动年度', [vs.valRatioRollYears1, vs.valRatioRollYears2].filter(function (x) { return x && parseFloat(x) > 0; }).join(' / ') || '—'],
            ['口径', (vs.valRatioMode === 'ma' ? fmt(vs.valRatioN) + '日均值' : '点位') + '比值'],
            ['阈值(高/低)', (fmt(vs.valRatioHi) + ' / ' + fmt(vs.valRatioLo))]
        ]);

    // ---- 单指数图（可重入渲染，支持报告内调整展示区间） ----
    var singleChart = null;
    var singleDisp = { showStart: (vs.valSingleShowStart || vs.valSingleStart || ''), showEnd: (vs.valSingleShowEnd || vs.valSingleEnd || '') };
    var sSS = document.getElementById('valSingleShowStart');
    var sSE = document.getElementById('valSingleShowEnd');
    if (sSS) sSS.value = singleDisp.showStart;
    if (sSE) sSE.value = singleDisp.showEnd;
    function renderValuationSingle() {
        var bm = getBm(vs.valSingleIndex);
        if (!bm || !bm.data || !bm.data.length) {
            var holder = document.getElementById('valSingleExport');
            if (holder) holder.parentNode.innerHTML = '<p class="text-gray-500 text-sm">请先选择基准指数</p>';
            return;
        }
        var data = bm.data.slice().sort(function (a, b) { return a.date.localeCompare(b.date); });
        var dataFirst = data[0].date, dataLast = data[data.length - 1].date;
        // 展示日期输入设置 min/max，并钳制值到指数实际数据范围内，防止年份溢出
        if (sSS) { sSS.min = dataFirst; sSS.max = dataLast; }
        if (sSE) { sSE.min = dataFirst; sSE.max = dataLast; }
        var start = vs.valSingleStart, end = vs.valSingleEnd;
        var showStart = singleDisp.showStart || start, showEnd = singleDisp.showEnd || end;
        if (showStart && showStart < dataFirst) showStart = dataFirst;
        if (showStart && showStart > dataLast) showStart = dataLast;
        if (showEnd && showEnd < dataFirst) showEnd = dataFirst;
        if (showEnd && showEnd > dataLast) showEnd = dataLast;
        var hi = parseFloat(vs.valSingleHi) || 80, lo = parseFloat(vs.valSingleLo) || 20;
        var meanN = parseInt(vs.valSingleMeanN, 10) || 0;
        var rollYears = [vs.valSingleRollYears1, vs.valSingleRollYears2, vs.valSingleRollYears3]
            .map(function (x) { return parseFloat(x); })
            .filter(function (x) { return !isNaN(x) && x > 0; });
        var dates = data.map(function (d) { return d.date; });
        var navs = data.map(function (d) { return d.nav; });
        var pctBase = meanN > 1 ? rollingMean(navs, meanN) : navs;
        var pctSeries = [];
        for (var k = 0; k < rollYears.length; k++) pctSeries.push(rollingPercentile(pctBase, Math.max(1, rollYears[k]) * DAYS));
        var pctDates = [], pointVals = [], pctFiltered = [];
        for (var i = 0; i < navs.length; i++) {
            if (showStart && dates[i] < showStart) continue;
            if (showEnd && dates[i] > showEnd) continue;
            pctDates.push(dates[i]); pointVals.push(navs[i]);
        }
        pctFiltered = pctSeries.map(function (src) {
            var out = [];
            for (var j = 0; j < navs.length; j++) {
                if (showStart && dates[j] < showStart) continue;
                if (showEnd && dates[j] > showEnd) continue;
                out.push(src[j]);
            }
            return out;
        });
        var datasets = [{ label: bm.name + ' 点位', data: pointVals, yAxisID: 'y', borderColor: '#6366F1', backgroundColor: 'rgba(99,102,241,0.08)', fill: true, tension: 0.1, pointRadius: 0, borderWidth: 2 }];
        var colors = ['rgba(16,185,129,0.5)', 'rgba(139,92,246,0.5)', 'rgba(245,158,11,0.5)'];
        for (var k2 = 0; k2 < rollYears.length; k2++) datasets.push({ label: '百分位 ' + rollYears[k2] + ' 年 (%)' + (meanN > 1 ? ' / ' + meanN + '日均值' : ''), data: pctFiltered[k2], yAxisID: 'y1', borderColor: colors[k2 % colors.length], backgroundColor: 'transparent', fill: false, tension: 0.1, pointRadius: 0, borderWidth: 2 });
        if (hi > 0) datasets.push({ label: '高估线 ' + hi + '%', data: pctDates.map(function () { return hi; }), yAxisID: 'y1', borderColor: '#EF4444', borderDash: [6, 3], pointRadius: 0, borderWidth: 1.5, fill: false });
        if (lo > 0) datasets.push({ label: '低估线 ' + lo + '%', data: pctDates.map(function () { return lo; }), yAxisID: 'y1', borderColor: '#3B82F6', borderDash: [6, 3], pointRadius: 0, borderWidth: 1.5, fill: false });
        if (singleChart) singleChart.destroy();
        singleChart = new Chart(document.getElementById('valSingleExport').getContext('2d'), { type: 'line', data: { labels: pctDates, datasets: datasets }, options: chartOpts('点位') });
        var note = document.getElementById('valSingleNote');
        if (note) note.textContent = '口径：' + (meanN > 1 ? meanN + '日均值' : '点位') + '（百分位同口径）' + (rollYears.length ? '，滚动 ' + rollYears.join('/') + ' 年' : '，未填滚动年度不画百分位线') + '，阈值高 ' + hi + '% / 低 ' + lo + '%；数据不足所选滚动年度的起始段留白。';
    }
    window.applyValuationSingleRange = function () {
        singleDisp.showStart = sSS ? sSS.value : '';
        singleDisp.showEnd = sSE ? sSE.value : '';
        renderValuationSingle();
    };
    renderValuationSingle();

    // ---- 双指数比值图（可重入渲染，支持报告内调整展示区间） ----
    var ratioChart = null;
    var ratioDisp = { showStart: (vs.valRatioShowStart || vs.valRatioStart || ''), showEnd: (vs.valRatioShowEnd || vs.valRatioEnd || '') };
    var rSS = document.getElementById('valRatioShowStart');
    var rSE = document.getElementById('valRatioShowEnd');
    if (rSS) rSS.value = ratioDisp.showStart;
    if (rSE) rSE.value = ratioDisp.showEnd;
    function renderValuationRatio() {
        var note = document.getElementById('valRatioNote');
        var idA = vs.valRatioA, idB = vs.valRatioB;
        var bmA = getBm(idA), bmB = getBm(idB);
        if (!bmA || !bmB || String(idA) === String(idB)) {
            document.getElementById('valRatioExport').parentNode.innerHTML = '<p class="text-gray-500 text-sm">请先选择指数 A 与指数 B（且不相同）</p>';
            return;
        }
        var mode = vs.valRatioMode, n = parseInt(vs.valRatioN, 10) || 20;
        var hi = parseFloat(vs.valRatioHi) || 80, lo = parseFloat(vs.valRatioLo) || 20;
        var rollYears = [vs.valRatioRollYears1, vs.valRatioRollYears2]
            .map(function (x) { return parseFloat(x); })
            .filter(function (x) { return !isNaN(x) && x > 0; });
        var range = commonDateRange(bmA, bmB);
        var start = vs.valRatioStart || (range ? range.start : ''), end = vs.valRatioEnd || (range ? range.end : '');
        var showStart = ratioDisp.showStart || start, showEnd = ratioDisp.showEnd || end;
        if (!start || !end) {
            document.getElementById('valRatioExport').parentNode.innerHTML = '<p class="text-gray-500 text-sm">两个指数无共同交易日，无法计算比值</p>';
            return;
        }
        // 展示日期输入设置 min/max，并钳制值到共同交易日范围内，防止年份溢出
        if (rSS) { rSS.min = range ? range.start : ''; rSS.max = range ? range.end : ''; }
        if (rSE) { rSE.min = range ? range.start : ''; rSE.max = range ? range.end : ''; }
        if (range && showStart && showStart < range.start) showStart = range.start;
        if (range && showStart && showStart > range.end) showStart = range.end;
        if (range && showEnd && showEnd < range.start) showEnd = range.start;
        if (range && showEnd && showEnd > range.end) showEnd = range.end;
        var series = buildRatioSeries(bmA, bmB, start, end);
        if (!series || series.length === 0) {
            document.getElementById('valRatioExport').parentNode.innerHTML = '<p class="text-gray-500 text-sm">点位区间内无共同交易日，无法计算比值</p>';
            return;
        }
        var dates = series.map(function (s) { return s.date; });
        var ratiosRaw = series.map(function (s) { return s.r; });
        var ratioValsMain = mode === 'ma' ? rollingMean(ratiosRaw, n) : ratiosRaw;
        var pctSeries = [];
        for (var k = 0; k < rollYears.length; k++) pctSeries.push(rollingPercentile(ratioValsMain, Math.max(1, rollYears[k]) * DAYS));
        var pctDates = [], ratioVals = [], pctFiltered = [];
        for (var i = 0; i < ratiosRaw.length; i++) {
            if (showStart && dates[i] < showStart) continue;
            if (showEnd && dates[i] > showEnd) continue;
            pctDates.push(dates[i]); ratioVals.push(ratioValsMain[i]);
        }
        pctFiltered = pctSeries.map(function (src) {
            var out = [];
            for (var j = 0; j < ratiosRaw.length; j++) {
                if (showStart && dates[j] < showStart) continue;
                if (showEnd && dates[j] > showEnd) continue;
                out.push(src[j]);
            }
            return out;
        });
        var ratioLabel = bmA.name + ' / ' + bmB.name + (mode === 'ma' ? ' (' + n + '日均值)' : '');
        var datasets = [{ label: ratioLabel, data: ratioVals, yAxisID: 'y', borderColor: '#6366F1', backgroundColor: 'rgba(99,102,241,0.08)', fill: true, tension: 0.1, pointRadius: 0, borderWidth: 2 }];
        var colors = ['rgba(16,185,129,0.5)', 'rgba(139,92,246,0.5)', 'rgba(245,158,11,0.5)'];
        for (var k2 = 0; k2 < rollYears.length; k2++) datasets.push({ label: '比值百分位 ' + rollYears[k2] + ' 年 (%)', data: pctFiltered[k2], yAxisID: 'y1', borderColor: colors[k2 % colors.length], backgroundColor: 'transparent', fill: false, tension: 0.1, pointRadius: 0, borderWidth: 2 });
        if (hi > 0) datasets.push({ label: '高估线 ' + hi + '%', data: pctDates.map(function () { return hi; }), yAxisID: 'y1', borderColor: '#EF4444', borderDash: [6, 3], pointRadius: 0, borderWidth: 1.5, fill: false });
        if (lo > 0) datasets.push({ label: '低估线 ' + lo + '%', data: pctDates.map(function () { return lo; }), yAxisID: 'y1', borderColor: '#3B82F6', borderDash: [6, 3], pointRadius: 0, borderWidth: 1.5, fill: false });
        if (ratioChart) ratioChart.destroy();
        ratioChart = new Chart(document.getElementById('valRatioExport').getContext('2d'), { type: 'line', data: { labels: pctDates, datasets: datasets }, options: chartOpts('比值') });
        if (note) note.textContent = '口径：' + (mode === 'ma' ? n + '日均值' : '点位') + '比值（百分位同口径），滚动 ' + (rollYears.join('/') || '—') + ' 年，阈值高 ' + hi + '% / 低 ' + lo + '%；数据不足所选滚动年度的起始段留白。';
    }
    window.applyValuationRatioRange = function () {
        ratioDisp.showStart = rSS ? rSS.value : '';
        ratioDisp.showEnd = rSE ? rSE.value : '';
        renderValuationRatio();
    };
    renderValuationRatio();
}



