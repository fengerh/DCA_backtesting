/* strategy.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// ================= 定投策略比较沙盒 =================
let scItems = [];
let scResults = [];
let scChart = null;
let scChartXMode = 'month';   // 'month' | 'date'
let scChartYMode = 'net';     // 'net' | 'xirr'
let currentMode = 'combo';    // 'combo' | 'sc' —— 供导出报告按当前模式分流
let scCommonStart = null;
const SC_COLORS = ['#2563eb','#dc2626','#16a34a','#d97706','#7c3aed','#0891b2','#db2777','#65a30d'];
const SC_STRATEGIES = {
    '1': '一次性投资',
    '2': '普通定额定投',
    '5': '目标止盈定投',
    '4': '均线智能定投',
    '6': '估值不定额',
    '3': '价值平均定投(VA)'
};
// 下拉框显示顺序：数字键会被 JS 自动按数值升序排序，故显式指定
const SC_STRATEGY_ORDER = ['1', '2', '5', '4', '6', '3'];


function setMode(mode) {
    const combo = document.getElementById('comboBacktestRoot');
    const sc = document.getElementById('strategyCompareRoot');
    const bCombo = document.getElementById('modeCombo');
    const bSc = document.getElementById('modeStrategy');
    const hasFundData = Object.keys(fundsData).length > 0;
    if (mode === 'sc') {
        // 切换到策略比较：彻底隐藏 combo 模式所有内容
        combo.style.display = 'none';
        sc.style.display = 'block';
        // 显式隐藏 combo 内的子卡片（防止父容器隐藏后子元素 display:block 干扰）
        const comboCards = ['planListSection','resultSection','profitProbabilitySection','correlationSection','chartFilter','benchmarkSelectorWrapper','periodMetricsSection'];
        comboCards.forEach(id => { const el = document.getElementById(id); if (el) el.style.display = 'none'; });
        if (scItems.length === 0 && hasFundData) addScItem();
    } else {
        // 切换到组合回测：彻底隐藏策略比较内容
        combo.style.display = 'block';
        sc.style.display = 'none';
        // 显式隐藏 scResultArea
        const scResult = document.getElementById('scResultArea');
        if (scResult) scResult.classList.add('hidden');
        // 如果有基金数据，恢复回测卡片可见性
        if (hasFundData) {
            document.getElementById('planListSection').style.display = 'block';
            document.getElementById('resultSection').style.display = 'block';
        }
    }
    const activeClass = 'px-5 py-2 rounded-lg font-medium transition bg-indigo-600 text-white';
    const idleClass = 'px-5 py-2 rounded-lg font-medium transition bg-gray-200 text-gray-700 hover:bg-gray-300';
    bCombo.className = mode === 'combo' ? activeClass : idleClass;
    bSc.className = mode === 'sc' ? activeClass : idleClass;
    currentMode = mode;
}

function addScItem() {
    const funds = Object.keys(fundsData);
    if (funds.length === 0) { alert('请先在上方「数据管理」中上传基金净值数据'); return; }
    const f = funds[0];
    scItems.push({
        id: 'sc_' + Date.now() + '_' + Math.floor(Math.random() * 1000),
        fund: f, strategy: '2',
        freq: 'monthly', weekday: '1', dayOfMonth: 'first',
        baseAmount: 1000,
        maDays: 250, lowCoef: 1.5, highCoef: 0.5,
        stopGainPct: 8, stopGainSellRatio: 100,
        valWindow: 250, valK: 1.0,
        startDate: fundsData[f].minDate,
        endDate: fundsData[f].maxDate,
        div: 'reinvest'
    });
    renderScItems();
}
function deleteScItem(id) { scItems = scItems.filter(x => x.id !== id); renderScItems(); }

function renderScItems() {
    const container = document.getElementById('scItemList');
    if (scItems.length === 0) { container.innerHTML = '<p class="text-gray-500 text-sm">暂无对比条目，点击下方「＋ 添加对比条目」。</p>'; return; }
    container.innerHTML = scItems.map(scRowHtml).join('');
    scItems.forEach(item => {
        const row = container.querySelector('[data-id="' + item.id + '"]');
        if (!row) return;
        row.querySelectorAll('[data-field]').forEach(el => {
            el.addEventListener('change', e => {
                const f = e.target.dataset.field;
                let v = e.target.value;
                if (e.target.type === 'number') v = parseFloat(v);
                item[f] = v;
                if (f === 'fund') {
                    item.startDate = fundsData[v].minDate;
                    item.endDate = fundsData[v].maxDate;
                    renderScItems();
                } else if (f === 'strategy' || f === 'freq') {
                    renderScItems();
                }
            });
        });
        const del = row.querySelector('[data-act="del"]');
        if (del) del.addEventListener('click', () => deleteScItem(item.id));
    });
}

function scRowHtml(item) {
    const fundOpts = Object.keys(fundsData).map(k => `<option value="${k}" ${k === item.fund ? 'selected' : ''}>${k}</option>`).join('');
    const stratOpts = SC_STRATEGY_ORDER.map(k => `<option value="${k}" ${k === item.strategy ? 'selected' : ''}>${SC_STRATEGIES[k]}</option>`).join('');
    const freqOpts = `
        <option value="weekly" ${item.freq === 'weekly' ? 'selected' : ''}>每周</option>
        <option value="biweekly" ${item.freq === 'biweekly' ? 'selected' : ''}>每双周</option>
        <option value="monthly" ${item.freq === 'monthly' ? 'selected' : ''}>每月</option>`;
    const wdNames = ['周日','周一','周二','周三','周四','周五','周六'];
    const wdOpts = [1,2,3,4,5].map(d => `<option value="${d}" ${String(item.weekday) === String(d) ? 'selected' : ''}>${wdNames[d]}</option>`).join('');
    const domOpts = ['first','1','2','3','4','5','6','7','8','9','10','15','20','28'].map(d => {
        const t = d === 'first' ? '每月首个交易日' : (d + '号');
        return `<option value="${d}" ${String(item.dayOfMonth) === String(d) ? 'selected' : ''}>${t}</option>`;
    }).join('');
    const divOpts = `<option value="reinvest" ${item.div === 'reinvest' ? 'selected' : ''}>红利再投资</option><option value="cash" ${item.div === 'cash' ? 'selected' : ''}>现金分红</option>`;

    // 禁用态：一次性投资→整块置灰；weekly/biweekly→每月几号置灰；monthly→周几置灰
    const schedDisabled = item.strategy === '1';
    const freqDisabledAttr = schedDisabled ? 'disabled' : '';
    const weekdayDisabled = schedDisabled || item.freq === 'monthly';
    const domDisabled = schedDisabled || item.freq !== 'monthly';
    const disCls = 'disabled:bg-gray-100 disabled:text-gray-400 disabled:cursor-not-allowed';

    // 定投周期 + 周几/每月几号（互斥共用一个槽位）；一次性投资时置灰
    const showDom = item.freq === 'monthly';
    const daySlot = showDom
        ? `<label class="block text-xs text-gray-600 mb-1 ${domDisabled ? 'opacity-50' : ''}">每月几号</label>
           <select data-field="dayOfMonth" ${domDisabled ? 'disabled' : ''} class="w-full p-2 border rounded-lg text-sm ${disCls}">${domOpts}</select>`
        : `<label class="block text-xs text-gray-600 mb-1 ${weekdayDisabled ? 'opacity-50' : ''}">周几</label>
           <select data-field="weekday" ${weekdayDisabled ? 'disabled' : ''} class="w-full p-2 border rounded-lg text-sm ${disCls}">${wdOpts}</select>`;
    const scheduleBlock = `
        <div class="flex items-start gap-2">
            <div class="flex-1 min-w-0">
                <label class="block text-xs text-gray-600 mb-1">定投周期</label>
                <select data-field="freq" ${freqDisabledAttr} class="w-full p-2 border rounded-lg text-sm ${disCls} ${schedDisabled ? 'bg-gray-100 text-gray-400' : ''}">${freqOpts}</select>
            </div>
            <div class="flex-1 min-w-0">${daySlot}</div>
        </div>`;

    const edGrey = item.strategy === '1';
    const fd = fundsData[item.fund] || {};   // 所属基金数据范围，用于约束日期输入
    const extra = scExtraHtml(item);
    return `
    <div class="border rounded-lg p-3 bg-gray-50" data-id="${item.id}">
      <!-- 第一行：基金(1/3) / 策略 / 定投周期(含周几或每月几号互斥) / 基础每期金额(半宽) -->
      <div class="grid grid-cols-1 md:grid-cols-6 gap-3 items-start">
        <div class="md:col-span-2"><label class="block text-xs text-gray-600 mb-1">基金</label><select data-field="fund" class="w-full p-2 border rounded-lg text-sm">${fundOpts}</select></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">策略</label><select data-field="strategy" class="w-full p-2 border rounded-lg text-sm">${stratOpts}</select></div>
        <div class="md:col-span-2">${scheduleBlock}</div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">基础每期金额</label><input type="number" data-field="baseAmount" value="${item.baseAmount}" min="0" class="w-full p-2 border rounded-lg text-sm"></div>
      </div>
      <!-- 第二行：开始/结束/分红(各半宽) + 止盈/均线/估值参数(占后半行) -->
      <div class="grid grid-cols-1 md:grid-cols-6 gap-3 items-start mt-3">
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">开始日期</label><input type="date" data-field="startDate" value="${item.startDate}" min="${fd.minDate || ''}" max="${fd.maxDate || ''}" class="w-full p-2 border rounded-lg text-sm"></div>
        <div class="md:col-span-1 ${edGrey ? 'opacity-50 cursor-not-allowed' : ''}"><label class="block text-xs text-gray-600 mb-1">结束日期</label><input type="date" data-field="endDate" value="${edGrey ? item.startDate : item.endDate}" ${edGrey ? 'disabled' : ''} min="${fd.minDate || ''}" max="${fd.maxDate || ''}" class="w-full p-2 border rounded-lg text-sm disabled:bg-gray-100 disabled:text-gray-400 disabled:cursor-not-allowed"></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">分红方式</label><select data-field="div" class="w-full p-2 border rounded-lg text-sm">${divOpts}</select></div>
        <div class="md:col-span-3">${extra}</div>
      </div>
      <div class="mt-2 text-left"><button data-act="del" class="text-red-500 hover:text-red-700 text-sm font-medium">删除此条目</button></div>
    </div>`;
}

function scExtraHtml(item) {
    const f = (label, field, attrs, val) => `<div class="min-w-0">
        <label class="block text-xs leading-tight text-gray-600 mb-1 truncate">${label}</label>
        <input type="number" data-field="${field}" value="${val}" ${attrs} class="w-16 p-2 border border-gray-300 rounded-lg text-sm">
    </div>`;
    const note = (t) => `<div class="text-[10px] text-gray-400 flex-1 self-center whitespace-nowrap">说明：${t}</div>`;
    if (item.strategy === '4') {
        return `<div class="w-full">
            <div class="flex items-center gap-2">
                <span class="text-xs font-medium text-indigo-600 whitespace-nowrap self-center">均线智能</span>
                <div class="flex items-end gap-2">${f('均线天数', 'maDays', 'min="5"', item.maDays)}${f('低位系数', 'lowCoef', 'step="0.1"', item.lowCoef)}${f('高位系数', 'highCoef', 'step="0.1"', item.highCoef)}</div>
                ${note('低于均线多投、高于均线少投（系数>1多投，<1少投）')}
            </div>
        </div>`;
    }
    if (item.strategy === '5') {
        return `<div class="w-full">
            <div class="flex items-center gap-2">
                <span class="text-xs font-medium text-emerald-600 whitespace-nowrap self-center">目标止盈</span>
                <div class="flex items-end gap-2">${f('止盈阈值(%)', 'stopGainPct', 'step="0.5"', item.stopGainPct)}${f('赎回比例(%)', 'stopGainSellRatio', 'step="1" min="0" max="100"', item.stopGainSellRatio)}</div>
                ${note('累计收益达阈值时赎回设定比例份额，随后继续定投')}
            </div>
        </div>`;
    }
    if (item.strategy === '6') {
        return `<div class="w-full">
            <div class="flex items-center gap-2">
                <span class="text-xs font-medium text-amber-600 whitespace-nowrap self-center">估值不定额</span>
                <div class="flex items-end gap-2">${f('历史窗口', 'valWindow', 'min="20"', item.valWindow)}${f('系数 k', 'valK', 'step="0.1"', item.valK)}</div>
                ${note('按历史分位调整投入：分位越低投越多（k越大调整越灵敏）')}
            </div>
        </div>`;
    }
    return '';
}

// 单条对比条目的策略模拟：复用与组合回测一致的分红/资产/指标口径
function simulateStrategy(item) {
    const fund = fundsData[item.fund];
    if (!fund) return null;
    const startTs = new Date(item.startDate + 'T00:00:00').getTime();
    const endTs = new Date(item.endDate + 'T00:00:00').getTime();
    if (isNaN(startTs) || isNaN(endTs) || endTs < startTs) return null;

    // 取该基金自身在 [start,end] 内交易日序列（单基金，无需前向填充）
    const dates = [], dateStrs = [], navs = [], divs = [];
    for (let i = 0; i < fund.dates.length; i++) {
        const ts = fund.dates[i].getTime();
        if (ts >= startTs && ts <= endTs) {
            dates.push(new Date(fund.dates[i]));
            dateStrs.push(formatDate(fund.dates[i]));
            navs.push(fund.nav[i]);
            divs.push(fund.div[i] || 0);
        }
    }
    const N = dates.length;
    if (N < 2) return null;

    const dow = new Array(N), dom = new Array(N);
    for (let k = 0; k < N; k++) { dow[k] = dates[k].getUTCDay(); dom[k] = parseInt(dateStrs[k].split('-')[2], 10); }

    const reinvest = item.div === 'reinvest';
    const strat = item.strategy;

    const isInvest = new Array(N).fill(false);
    if (strat === '1') {
        isInvest[0] = true;
    } else {
        const freq = item.freq;
        if (freq === 'weekly' || freq === 'biweekly') {
            const wd = parseInt(item.weekday);
            let firstIdx = -1;
            for (let k = 0; k < N; k++) { if (dow[k] === wd) { firstIdx = k; break; } }
            if (firstIdx >= 0) {
                for (let k = firstIdx; k < N; k++) {
                    if (dow[k] === wd) {
                        if (freq === 'biweekly') {
                            const weekDiff = Math.round((dates[k] - dates[firstIdx]) / (7 * 86400000));
                            if (weekDiff % 2 !== 0) continue;
                        }
                        isInvest[k] = true;
                    }
                }
            }
        } else {
            if (item.dayOfMonth === 'first') {
                let lastMonth = -1;
                for (let k = 0; k < N; k++) {
                    const ym = dateStrs[k].slice(0, 7);
                    if (ym !== lastMonth) { isInvest[k] = true; lastMonth = ym; }
                }
            } else {
                const domN = parseInt(item.dayOfMonth);
                for (let k = 0; k < N; k++) { if (dom[k] === domN) isInvest[k] = true; }
            }
        }
    }
    const investIdx = [];
    for (let k = 0; k < N; k++) if (isInvest[k]) investIdx.push(k);
    if (investIdx.length === 0) return null;

    let maArr = null;
    if (strat === '4') {
        const maDays = Math.max(2, parseInt(item.maDays) || 250);
        maArr = new Array(N).fill(null);
        let sum = 0; const q = [];
        for (let k = 0; k < N; k++) {
            sum += navs[k]; q.push(navs[k]);
            if (q.length > maDays) sum -= q.shift();
            maArr[k] = sum / q.length;
        }
    }
    let pctArr = null;
    if (strat === '6') {
        const win = Math.max(5, parseInt(item.valWindow) || 250);
        pctArr = new Array(N).fill(0.5);
        const hist = [];
        for (let k = 0; k < N; k++) {
            hist.push(navs[k]);
            if (hist.length > win) hist.shift();
            const cur = navs[k];
            let below = 0;
            for (let j = 0; j < hist.length; j++) if (hist[j] <= cur) below++;
            pctArr[k] = hist.length > 1 ? below / hist.length : 0.5;
        }
    }

    let shares = 0, totalCash = 0, totalInvested = 0, costBasis = 0, stopGainCount = 0, investCount = 0, totalRedeemed = 0, totalDividendCash = 0;
    let runPrincipal = 0, maxPrincipal = 0, runMaxPrincipal = 0;   // 本轮本金(累计) / 全局最大本金(峰值,不重置) / 本轮峰值本金(随本轮重置)
    const cashFlows = [], flowDates = [], assets = [], invests = [], cashDivs = [], peakPrincipal = [];
    const stopGainEvents = [];   // 记录每次止盈触发的交易日下标
    const holdAssets = [], holdCost = []; // 每日持仓市值 / 持仓成本（不含已落袋现金）

    for (let k = 0; k < N; k++) {
        const nav = navs[k];
        const date = dates[k];
        if (shares > 0 && divs[k] > 0) {
            const totalDiv = shares * divs[k];
            if (reinvest) shares += totalDiv / nav;
            else { totalCash += totalDiv; totalDividendCash += totalDiv; cashFlows.push(totalDiv); flowDates.push(date); }
        }
        let amt = 0;
        if (isInvest[k]) {
            if (strat === '1') amt = item.baseAmount;
            else if (strat === '2') amt = item.baseAmount;
            else if (strat === '3') { investCount++; amt = investCount * item.baseAmount - shares * nav; }
            else if (strat === '4') {
                const ma = maArr[k];
                let factor = 1;
                if (ma !== null && ma > 0) factor = nav < ma ? parseFloat(item.lowCoef) : parseFloat(item.highCoef);
                amt = item.baseAmount * factor;
            } else if (strat === '5') amt = item.baseAmount;
            else if (strat === '6') { const factor = 1 + (0.5 - pctArr[k]) * parseFloat(item.valK); amt = item.baseAmount * factor; }
        }
        // 目标止盈：每日检查累计收益，达阈值则赎回设定比例并继续定投
        // 收益率标准以"本轮最大投入本金(runMaxPrincipal)"为分母：
        //   ① 部分赎回不缩小分母 → 不会次日立即误触发；
        //   ② 分母取截至昨日的本轮峰值，不含今日尚未产生收益的投入 → 100%赎回能正常触发。
        if (strat === '5') {
            const th = parseFloat(item.stopGainPct) / 100;
            const sellRatio = Math.min(1, Math.max(0, parseFloat(item.stopGainSellRatio) / 100));
            const roundPrincipal = runMaxPrincipal;   // 本轮峰值本金(截至昨日，不含今日未投部分)
            if (roundPrincipal > 0 && sellRatio > 0) {
                const holdMv = shares * nav;          // 只算持仓市值，不含已落袋现金
                if ((holdMv - costBasis) / roundPrincipal >= th) {
                    const sellShares = shares * sellRatio;
                    const proceeds = sellShares * nav;
                    shares -= sellShares; totalCash += proceeds; totalRedeemed += proceeds;
                    cashFlows.push(proceeds); flowDates.push(date);
                    costBasis *= (1 - sellRatio);
                    stopGainCount++;
                    stopGainEvents.push(k);      // 记录本次止盈触发的交易日下标
                    // 全部赎回：本轮本金已落袋，重置后下一周期重新累计（仅全部赎回才重置，部分赎回不重置）
                    if (sellRatio >= 1 || shares < 1e-9) {
                        runPrincipal = 0;
                        runMaxPrincipal = 0;   // 同步重置本轮峰值本金
                    }
                }
            }
        }
        if (amt !== 0 && isInvest[k]) {
            if (amt > 0) {
                shares += amt / nav; totalInvested += amt; costBasis += amt; runPrincipal += amt;
                cashFlows.push(-amt); flowDates.push(date);
            } else {
                let sellShares = (-amt) / nav;
                if (sellShares > shares) sellShares = shares;
                if (sellShares > 0) {
                    const proceeds = sellShares * nav;
                    shares -= sellShares; totalCash += proceeds; totalRedeemed += proceeds;
                    cashFlows.push(proceeds); flowDates.push(date);
                    const denom = shares + sellShares;
                    if (denom > 0) costBasis *= shares / denom;
                }
            }
        }
        runMaxPrincipal = Math.max(runMaxPrincipal, runPrincipal);   // 记录本轮峰值投入本金
        maxPrincipal = Math.max(maxPrincipal, runPrincipal);   // 记录全局峰值(用于最终指标)
        peakPrincipal.push(maxPrincipal);                      // 记录每日全局峰值本金(止盈净值曲线分母)
        assets.push(shares * nav + totalCash);
        invests.push(amt > 0 ? amt : 0);
        cashDivs.push(totalCash);
        holdAssets.push(shares * nav);
        holdCost.push(costBasis);
    }

    const finalDate = dates[N - 1];
    const finalAsset = assets[N - 1];
    // 期末现金流只计剩余持仓市值，已赎回/分红现金此前已作为正现金流流出，避免重复计数导致 XIRR 失真
    cashFlows.push(shares * navs[N - 1]); flowDates.push(finalDate);
    const xirrVal = xirr(cashFlows, flowDates) * 100;
    const totalReturn = totalInvested > 0 ? (finalAsset / totalInvested - 1) * 100 : 0;
    const netProfit = finalAsset - totalInvested;                 // = 总赎回+市值+累积现金分红−总定投
    const maxPrincipalReturn = maxPrincipal > 0 ? netProfit / maxPrincipal * 100 : 0;   // 收益率(峰值本金)
    const m = computeMetrics(dates, assets, invests);
    return { item, dates, assets, invests, cashDivs, holdAssets, holdCost, totalInvested, finalAsset, totalReturn, xirrVal, stopGainCount, maxPrincipal, maxPrincipalReturn, totalRedeemed, totalDividendCash, peakPrincipal, stopGainEvents, firstInvestDate: dates[investIdx[0]], metrics: m };
}

function runStrategyCompare() {
    if (scItems.length === 0) { alert('请先添加对比条目'); return; }
    const fundsInItems = [...new Set(scItems.map(x => x.fund))];
    scCommonStart = null;
    let commonStart = null, commonEnd = null;
    if (fundsInItems.length > 1) {
        // 多基金对比：以历史较短基金的可用区间为准（其起始日期即比较最早起点）
        let maxMin = -Infinity, minMax = Infinity;
        fundsInItems.forEach(f => {
            const fd = fundsData[f];
            const mn = new Date(fd.minDate + 'T00:00:00').getTime();
            const mx = new Date(fd.maxDate + 'T00:00:00').getTime();
            if (mn > maxMin) maxMin = mn;
            if (mx < minMax) minMax = mx;
        });
        commonStart = maxMin; commonEnd = minMax; scCommonStart = commonStart;
    }
    scResults = [];
    const errors = [];
    scItems.forEach(item => {
        const eff = Object.assign({}, item);
        if (commonStart !== null) {
            const st = new Date(item.startDate + 'T00:00:00').getTime();
            const en = new Date(item.endDate + 'T00:00:00').getTime();
            eff.startDate = formatDate(new Date(Math.max(st, commonStart)));
            eff.endDate = formatDate(new Date(Math.min(en, commonEnd)));
        }
        const r = simulateStrategy(eff);
        if (!r) { errors.push((fundCodeName(item.fund).name || item.fund) + '·' + SC_STRATEGIES[item.strategy] + '：区间数据不足'); return; }
        scResults.push(r);
    });
    if (scResults.length === 0) { alert('没有可运行的对比条目：' + errors.join('；')); return; }
    renderScResults(errors);
}

// 止盈间隔统计（自然日）：首笔投资→首次止盈、以及相邻止盈触发日的日期差，供卡片悬停提示使用
function sgIntervalText(r) {
    const ev = r.stopGainEvents || [];
    if (ev.length === 0) return '无触发';
    const baseDate = r.firstInvestDate || r.dates[0];
    const gaps = [];
    for (let i = 0; i < ev.length; i++) {
        const start = (i === 0) ? baseDate : r.dates[ev[i - 1]];
        const end = r.dates[ev[i]];
        gaps.push({ days: (end.getTime() - start.getTime()) / 86400000, start, end });
    }
    const days = gaps.map(g => g.days);
    const max = Math.max(...days), min = Math.min(...days);
    const avg = days.reduce((a, b) => a + b, 0) / days.length;
    const maxGap = gaps.find(g => g.days === max), minGap = gaps.find(g => g.days === min);
    const ymd = d => `${d.getFullYear()}.${d.getMonth() + 1}.${d.getDate()}`;
    return `最长间隔 ${max.toFixed(0)} 天（区间 ${ymd(maxGap.start)} - ${ymd(maxGap.end)}）\n`
        + `最短间隔 ${min.toFixed(0)} 天（区间 ${ymd(minGap.start)} - ${ymd(minGap.end)}）\n`
        + `平均间隔 ${avg.toFixed(1)} 天`;
}

function renderScResults(errors) {
    const tbody = document.getElementById('scTableBody');
    let rows = '';
    scResults.forEach(r => {
        const it = r.item; const m = r.metrics; const cn = fundCodeName(it.fund);
        rows += `<tr class="border-b hover:bg-gray-50">
            <td class="px-1.5 py-1.5 text-center font-mono text-xs whitespace-nowrap">${cn.code}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${cn.name || cn.code}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${SC_STRATEGIES[it.strategy]}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.totalInvested.toFixed(0)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.finalAsset.toFixed(0)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap ${it.strategy === '5' ? 'text-gray-400' : (r.totalReturn >= 0 ? 'text-emerald-600 font-medium' : 'text-rose-600 font-medium')}">${it.strategy === '5' ? '—' : r.totalReturn.toFixed(2) + '%'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isNaN(r.xirrVal) ? '-' : r.xirrVal.toFixed(2) + '%'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isNaN(m.maxDrawdown) ? '-' : m.maxDrawdown.toFixed(2) + '%'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isNaN(m.annualVolatility) ? '-' : (m.annualVolatility * 100).toFixed(2) + '%'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isNaN(m.sharpeRatio) ? '-' : m.sharpeRatio.toFixed(2)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${it.strategy === '5' ? r.stopGainCount : '—'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.maxPrincipal.toFixed(0)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap ${it.strategy === '5' ? (r.maxPrincipalReturn >= 0 ? 'text-emerald-600 font-medium' : 'text-rose-600 font-medium') : 'text-gray-400'}">${it.strategy === '5' ? r.maxPrincipalReturn.toFixed(2) + '%' : '—'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.totalRedeemed.toFixed(0)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.totalDividendCash.toFixed(0)}</td>
        </tr>`;
    });
    tbody.innerHTML = rows;

    document.getElementById('scRules').innerHTML = scResults.map(r => {
        const it = r.item; const cn = fundCodeName(it.fund);
        return `<div class="text-sm"><span class="font-medium">${cn.name || it.fund} · ${SC_STRATEGIES[it.strategy]}：</span>${scRuleText(it)}</div>`;
    }).join('');

    let errHtml = '';
    if (errors.length) errHtml += '<p class="text-amber-600 text-sm mt-2">⚠ ' + errors.join('；') + '</p>';
    if (scCommonStart !== null) errHtml += '<p class="text-xs text-gray-400 mt-1">* 多基金对比：各条目区间已对齐至历史较短基金的可用区间（' + formatDate(new Date(scCommonStart)) + ' 起）。</p>';
    document.getElementById('scErrors').innerHTML = errHtml;

    const metricsGrid = document.getElementById('scMetrics');
    metricsGrid.innerHTML = scResults.map(r => {
        const isSG = r.item.strategy === '5';
        const xirrTxt = isNaN(r.xirrVal) ? '-' : r.xirrVal.toFixed(2) + '%';
        const retTxt = isSG ? '—' : (isNaN(r.totalReturn) ? '-' : r.totalReturn.toFixed(2) + '%');
        // 主数字统一为 XIRR 年化，配色随 XIRR 正负（绿/红）
        const cardBg = (r.xirrVal >= 0) ? 'bg-emerald-50' : 'bg-rose-50';
        const cardText = (r.xirrVal >= 0) ? 'text-emerald-700' : 'text-rose-600';
        const cn = fundCodeName(r.item.fund);
        // 止盈卡片：峰值本金收益率移入悬停提示，并补充止盈次数与间隔明细
        let tip = '';
        if (isSG) {
            tip = `峰值本金收益率 ${r.maxPrincipalReturn.toFixed(2)}%（最大本金 ${r.maxPrincipal.toFixed(0)}）\n`
                + `止盈明细：\n· 止盈次数：${r.stopGainCount} 次\n· ${sgIntervalText(r)}`;
        }
        return `<div class="${cardBg} p-3 rounded-lg text-center"${tip ? ` title="${tip}"` : ''}>
            <div class="text-xs text-gray-500">${cn.name || r.item.fund} <br/>· ${SC_STRATEGIES[r.item.strategy]}</div>
            <div class="text-lg font-bold ${cardText}">${xirrTxt}</div>
            <div class="text-xs text-gray-500">XIRR年化 · 累计收益 ${retTxt}</div>
        </div>`;
    }).join('');

    document.getElementById('scResultArea').classList.remove('hidden');
    drawScChart();
}

function scRuleText(it) {
    const wdNames = ['周日','周一','周二','周三','周四','周五','周六'];
    const freqText = it.strategy === '1' ? '一次性' :
        (it.freq === 'weekly' ? '每周' + wdNames[+it.weekday] + '定投' :
         it.freq === 'biweekly' ? '每双周' + wdNames[+it.weekday] + '定投' :
         '每月' + (it.dayOfMonth === 'first' ? '首个交易日' : it.dayOfMonth + '号') + '定投');
    switch (it.strategy) {
        case '1': return `起始日一次性投入 ${it.baseAmount} 元，之后不再投入。`;
        case '2': return `每期固定投入 ${it.baseAmount} 元（${freqText}）。`;
        case '3': return `价值平均：目标市值每期递增 ${it.baseAmount} 元，低于目标多买、高于目标卖出。`;
        case '4': return `均线智能：净值低于 ${it.maDays} 日均线时投入 ×${it.lowCoef}，高于时 ×${it.highCoef}（基准每期 ${it.baseAmount} 元）。`;
        case '5': return `普通定投（每期 ${it.baseAmount} 元，${freqText}），累计收益达 ${it.stopGainPct}% 时赎回 ${it.stopGainSellRatio}% 份额并继续定投。`;
        case '6': return `估值不定额：按近 ${it.valWindow} 交易日净值分位调整投入，系数 k=${it.valK}（基准每期 ${it.baseAmount} 元）。`;
    }
    return '';
}

// 逐日资金加权收益率（XIRR 年化，%），与策略对比表里的 XIRR 同口径：
//   每日净外部现金流 = −当日投入 + 当日落袋现金（赎回/现金分红，取增量）；
//   期末价值只计剩余持仓市值（assets − cashDivs），已落袋现金此前已作正流，避免重复计数。
//   对第 i 天用前缀现金流 [0..i] 求一次 XIRR；止盈策略天然被正确计入（赎回=正流、期末只算持仓），
//   曲线终点等于表格 XIRR 值。结果缓存到 r._runningXirr，切换 x 轴口径时不重算。
function runningXirr(r) {
    const N = r.dates.length;
    const out = new Array(N).fill(null);
    if (N < 2) return out;
    const baseTs = r.dates[0].getTime();
    const dayFrac = j => (r.dates[j].getTime() - baseTs) / (1000 * 60 * 60 * 24 * 365);
    const inv = r.invests || [], cashDivs = r.cashDivs || [], assets = r.assets || [];
    const netFlow = new Array(N), holdVal = new Array(N);
    let prevCash = 0, cumInvest = 0;
    for (let j = 0; j < N; j++) {
        const realized = cashDivs[j] - prevCash; prevCash = cashDivs[j];
        netFlow[j] = -(inv[j] || 0) + (realized > 0 ? realized : 0);
        holdVal[j] = assets[j] - cashDivs[j];   // 持仓市值（不含已落袋现金）
    }
    const npv = (rate, i) => {
        let s = 0;
        for (let j = 0; j <= i; j++) s += netFlow[j] / Math.pow(1 + rate, dayFrac(j));
        return s + holdVal[i] / Math.pow(1 + rate, dayFrac(i));
    };
    const npvDeriv = (rate, i) => {
        let d = 0;
        for (let j = 0; j <= i; j++) d -= netFlow[j] * dayFrac(j) / Math.pow(1 + rate, dayFrac(j) + 1);
        d -= holdVal[i] * dayFrac(i) / Math.pow(1 + rate, dayFrac(i) + 1);
        return d;
    };
    let prevRate = 0.1;
    cumInvest = inv[0] || 0;                       // 计入第0天投入：一次性投资整笔落在 inv[0]
    for (let i = 1; i < N; i++) {
        cumInvest += (inv[i] || 0);
        if (cumInvest <= 0) continue;             // 尚未投入，XIRR 无意义
        const tol = 1e-7;
        let rate = prevRate, ok = false;
        for (let it = 0; it < 60; it++) {
            const v = npv(rate, i), d = npvDeriv(rate, i);
            if (Math.abs(v) < tol) { ok = true; break; }
            if (Math.abs(d) < tol) break;
            rate -= v / d;
            if (rate <= -0.9999) { rate = -0.9999 + 1e-6; }
            if (!isFinite(rate)) break;
        }
        if (!ok) {                                 // 牛顿法失败（多次变号）→ 二分兜底
            let lo = -0.9999, hi = 100;
            const fLo = npv(lo, i), fHi = npv(hi, i);
            if (fLo * fHi <= 0) {
                for (let it = 0; it < 100; it++) {
                    const mid = (lo + hi) / 2, fM = npv(mid, i);
                    if (Math.abs(fM) < tol) { rate = mid; ok = true; break; }
                    if (fLo * fM < 0) { hi = mid; } else { lo = mid; }
                }
                if (!ok) rate = (lo + hi) / 2;
            } else rate = null;
        }
        out[i] = (rate == null || !isFinite(rate)) ? null : rate * 100;
        if (out[i] != null) prevRate = rate;
    }
    return out;
}

function drawScChart() {
    const canvas = document.getElementById('scChart');
    if (!canvas) return;
    if (scChart) scChart.destroy();
    const hasStopGain = scResults.some(x => x.item.strategy === '5');
    const isXirr = scChartYMode === 'xirr';
    const datasets = [];
    scResults.forEach((r, idx) => {
        const startTs = r.dates[0].getTime();
        const inv = r.invests || [];
        const isStopGain = r.item.strategy === '5';   // 止盈策略：净值采用"1+最大本金收益率"口径
        const sgSet = isStopGain && r.stopGainEvents ? new Set(r.stopGainEvents) : null;
        const rx = isXirr ? (r._runningXirr || (r._runningXirr = runningXirr(r))) : null;
        let cum = 0;
        const pts = [], sgPts = [];
        r.dates.forEach((d, i) => {
            cum += (inv[i] || 0);
            const months = (d.getTime() - startTs) / (1000 * 60 * 60 * 24 * 30.4375);
            let yVal;
            if (isXirr) {
                const v = rx[i];
                if (v == null) return;                 // 尚未投入等无意义点跳过
                yVal = +v.toFixed(2);
            } else {
                // 资金加权净值：当日总资产（持仓市值+落袋现金）/ 截至当日累计已投入本金，起点=1.0
                // 止盈赎回现金与现金分红现金同口径计入总资产，曲线连续、不归零、不断开
                let nv;
                if (isStopGain) {
                    // 口径Y：净值 = 1 + (总资产−累计投入) / 截至当日最大本金(峰值,不重置)
                    // 分母取最大本金而非累计投入，止盈赎回后曲线停在 1+阈值，不再因继续定投被稀释假跌
                    const peak = r.peakPrincipal[i];
                    nv = peak > 0 ? 1 + (r.assets[i] - cum) / peak : null;
                } else {
                    nv = cum > 0 ? r.assets[i] / cum : null;
                }
                if (nv == null) return;
                yVal = +nv.toFixed(4);
            }
            // 日期模式：x 取真实时间戳，按自然日历错开；月数模式：x 取距起始日的月数，从0对齐
            const xVal = scChartXMode === 'date' ? d.getTime() : +months.toFixed(2);
            pts.push({ x: xVal, y: yVal });
            if (sgSet && sgSet.has(i)) sgPts.push({ x: xVal, y: yVal });
        });
        const color = SC_COLORS[idx % SC_COLORS.length];
        const cn = fundCodeName(r.item.fund);
        datasets.push({
            label: (cn.name || r.item.fund) + '·' + SC_STRATEGIES[r.item.strategy],
            data: pts, borderColor: color, backgroundColor: color,
            borderWidth: 2, pointRadius: 0, tension: 0.1, fill: false
        });
        if (sgPts.length) {
            datasets.push({
                label: (cn.name || r.item.fund) + '·止盈点',
                data: sgPts, borderColor: color, backgroundColor: color,
                pointStyle: 'circle', pointRadius: 4, pointHoverRadius: 6,
                pointBorderColor: '#ffffff', pointBorderWidth: 1.5,
                showLine: false, fill: false, isStopGainMarker: true
            });
        }
    });
    // 计算数据点真实 x 范围，避免 linear 轴做 nice 圆整时向两端扩展（出现起点早几个月/终点晚几个月）
    let xMin = Infinity, xMax = -Infinity;
    datasets.forEach(ds => ds.data.forEach(p => {
        if (p.x != null && isFinite(p.x)) { if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x; }
    }));
    if (!isFinite(xMin)) { xMin = undefined; xMax = undefined; }
    scChart = new Chart(canvas.getContext('2d'), {
        type: 'line',
        data: { datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'nearest', intersect: false },
            scales: {
                x: {
                    type: 'linear',
                    min: xMin,
                    max: xMax,
                    title: { display: true, text: scChartXMode === 'date' ? '日期' : '持有期（月）' },
                    ticks: { maxTicksLimit: 12, callback: v => scChartXMode === 'date' ? formatDate(new Date(v)) : v + '月' }
                },
                y: { title: { display: true, text: isXirr
                    ? '资金加权收益率 XIRR 年化（%）'
                    : (hasStopGain
                        ? '投资净值（普通策略=总资产/累计投入；止盈策略=1+最大本金收益率，起点=1.0）'
                        : '投资净值（资金加权，总资产/累计投入，起点=1.0）') } }
            },
            plugins: {
                legend: { position: 'bottom', labels: { filter: (item, data) => !data.datasets[item.datasetIndex].isStopGainMarker } },
                tooltip: { callbacks: {
                    title: items => scChartXMode === 'date'
                        ? formatDate(new Date(items[0].parsed.x)) : (items[0].parsed.x + ' 月'),
                    label: item => item.dataset.isStopGainMarker ? item.dataset.label
                        : (isXirr ? item.parsed.y.toFixed(2) + '%' : item.parsed.y.toFixed(4))
                } }
            }
        }
    });
}

function setScXMode(mode) {
    scChartXMode = mode;
    const m = document.getElementById('scXMonth');
    const d = document.getElementById('scXDate');
    if (m && d) {
        const on = 'px-3 py-1.5 bg-blue-600 text-white';
        const off = 'px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100';
        m.className = mode === 'month' ? on : off;
        d.className = mode === 'date' ? on : off;
    }
    if (scResults.length) drawScChart();
}

function setScYMode(mode) {
    scChartYMode = mode;
    const n = document.getElementById('scYNet');
    const x = document.getElementById('scYXirr');
    if (n && x) {
        const on = 'px-3 py-1.5 bg-blue-600 text-white';
        const off = 'px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100';
        n.className = mode === 'net' ? on : off;
        x.className = mode === 'xirr' ? on : off;
    }
    const t = document.getElementById('scChartTitle');
    if (t) t.textContent = mode === 'xirr'
        ? '投资收益率曲线（资金加权 XIRR 年化，%）'
        : '投资净值曲线（资金加权，起点=1.0）';
    const note = document.getElementById('scChartNote');
    if (note) note.innerHTML = mode === 'xirr'
        ? '* 采用资金加权的 XIRR 年化收益率（与策略对比表"XIRR年化"同口径，曲线终点=该值）：每日净外部现金流 = −当日投入 + 当日落袋现金（赎回/现金分红），期末价值只计剩余持仓市值，已落袋现金此前已作正流避免重复计数。该口径不受持续定投稀释，能直接看到资金加权年化随时间的真实走势；止盈赎回作为正流计入，曲线不会因赎回而假跌。可切换"按持有期月数对齐"或"按日期"调整 x 轴。'
        : '* 采用资金加权的"这一笔投资净值"（起点=1.0）：当日净值 = 当日总资产 ÷ 截至当日的累计已投入本金。该口径保留了投入节奏的影响——上涨市中单笔（一次投入）会跑在定投（分批投入）上方，能直接看出策略差异；可切换"按持有期月数对齐"（各条目从0月对齐便于跨条目对比）或"按日期"（按真实日历日期错开显示），并可在"净值 / XIRR年化"间切换 y 轴口径。';
    if (scResults.length) drawScChart();
}



