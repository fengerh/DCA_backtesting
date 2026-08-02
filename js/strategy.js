/* strategy.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// ================= 定投策略比较沙盒 =================
let scItems = [];
let scResults = [];
let scPoolCap = Infinity;      // 全局共享资金池上限（留空/0/NaN = 不限制）
let scPool = { remaining: Infinity };   // 本次比较的共享资金池状态，跨条目结转
let scChart = null;
let scChartXMode = 'month';   // 'month' | 'date'
let scChartYMode = 'net';     // 'net' | 'xirr'
let scNetMode = 'mw';         // 'mw' 资金加权 | 'portfolio' 时间加权净值(份额法)
// XIRR 曲线当前勾选的窗口口径集合（多选叠加），默认仅"累计"，保证首屏行为与既有一致
let scXirrWindows = new Set(['cum']);
let currentMode = 'combo';    // 'combo' | 'sc' —— 供导出报告按当前模式分流
let scCommonStart = null;
const SC_COLORS = ['#2563eb','#dc2626','#16a34a','#d97706','#7c3aed','#0891b2','#db2777','#65a30d'];
// 投资方式（纯投入策略），与旧 SC_STRATEGIES 数字键对齐，新增 '7' 最大回撤投资
const SC_INVEST = {
    '1': '一次性投资',
    '2': '普通定额定投',
    '3': '价值平均定投(VA)',
    '4': '均线智能定投',
    '6': '估值不定额',
    '7': '最大回撤投资'
};
// 投资方式简称（图例/表格用）
const SC_INVEST_SHORT = {
    '1': '单笔', '2': '定额', '3': 'VA', '4': '均线', '6': '估值', '7': '回撤'
};
// 下拉框显示顺序：数字键会被 JS 自动按数值升序排序，故显式指定
const SC_INVEST_ORDER = ['1', '2', '7', '3', '4', '6'];
// 止盈方式（与投资方式解耦）
const SC_STOPGAIN = {
    'none': '不止盈',
    'target': '目标止盈',
    'drawdown': '回撤比例止盈',
    'gainratio': '固定收益比例止盈'
};
const SC_STOPGAIN_SHORT = {
    'none': '不止盈', 'target': '目标止盈', 'drawdown': '回撤止盈', 'gainratio': '比例止盈'
};
const SC_STOPGAIN_ORDER = ['none', 'target', 'drawdown', 'gainratio'];


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
        // 如果有基金数据，恢复计划列表可见性（回测结果板块在点击“启动回测”后才显示）
        if (hasFundData) {
            document.getElementById('planListSection').style.display = 'block';
            // 对齐策略比较：有基金数据且无任何计划时，自动生成一条默认计划
            if (investmentPlans.length === 0) addPlan();
        }
        // 切换回组合回测时刷新基准列表：确保 currentBenchmarkId 失效时回退默认选中第一个，并刷新 radio/select 状态
        if (typeof loadBenchmarkList === 'function') loadBenchmarkList();
    }
    // 一体化分段控件样式：选中=白底浮起，未选=透明灰字
    const activeClass = 'px-5 py-2 rounded-md font-medium transition bg-white text-indigo-700 shadow-sm';
    const idleClass = 'px-5 py-2 rounded-md font-medium transition text-gray-600 hover:text-gray-800';
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
        fund: f,
        investStrategy: '2',   // '1'一次性 '2'定额 '3'VA '4'均线 '6'估值 '7'最大回撤
        stopGain: 'none',      // 'none'不止盈 'target'目标止盈 'drawdown'回撤比例止盈 'gainratio'固定收益比例止盈
        freq: 'monthly', weekday: '1', dayOfMonth: 'first',
        baseAmount: 1000,
        maDays: 250, lowCoef: 1.5, highCoef: 0.5,   // 投资'4'均线
        valWindow: 250, valK: 1.0,                   // 投资'6'估值
        mdDays: 120, mdPct: 10,                       // 投资'7'最大回撤
        mdContinuous: false,                          // 投资'7'连续投资：窗口持续计算，满足回撤阈值即每期投一笔
        stopGainPct: 8, stopGainDrawdown: 10, stopGainSellRatio: 100,  // 止盈参数
        startDate: fundsData[f].minDate,
        endDate: fundsData[f].maxDate,
        div: 'reinvest'
    });
    renderScItems();
}
function deleteScItem(id) { scItems = scItems.filter(x => x.id !== id); renderScItems(); }
function clearAllScItems() {
    if (scItems.length === 0) return;
    if (!confirm('清空所有对比条目？')) return;
    scItems = [];
    renderScItems();
    const err = document.getElementById('scErrors');
    if (err) err.innerHTML = '';
    const hint = document.getElementById('scNavGapHint');
    if (hint) { hint.classList.add('hidden'); hint.innerHTML = ''; }
}

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
                if (e.target.type === 'checkbox') v = e.target.checked;
                item[f] = v;
                if (f === 'fund') {
                    item.startDate = fundsData[v].minDate;
                    item.endDate = fundsData[v].maxDate;
                    renderScItems();
                } else if (f === 'investStrategy' || f === 'stopGain' || f === 'freq' || f === 'mdContinuous') {
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
    const invOpts = SC_INVEST_ORDER.map(k => `<option value="${k}" ${k === item.investStrategy ? 'selected' : ''}>${SC_INVEST[k]}</option>`).join('');
    const sgOpts = SC_STOPGAIN_ORDER.map(k => `<option value="${k}" ${k === item.stopGain ? 'selected' : ''}>${SC_STOPGAIN[k]}</option>`).join('');
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

    // 禁用态：一次性('1')/最大回撤('7')为事件驱动或单次，整块置灰定期日程；weekly/biweekly→每月几号置灰；monthly→周几置灰
    const schedDisabled = item.investStrategy === '1' || item.investStrategy === '7';
    const freqDisabledAttr = schedDisabled ? 'disabled' : '';
    const weekdayDisabled = schedDisabled || item.freq === 'monthly';
    const domDisabled = schedDisabled || item.freq !== 'monthly';
    const disCls = 'disabled:bg-gray-100 disabled:text-gray-400 disabled:cursor-not-allowed';

    // 定投周期 + 周几/每月几号（互斥共用一个槽位）；一次性/最大回撤时置灰
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

    const edGrey = item.investStrategy === '1';
    const fd = fundsData[item.fund] || {};   // 所属基金数据范围，用于约束日期输入
    const extra = scExtraHtml(item);
    return `
    <div class="border rounded-lg p-3 bg-gray-50" data-id="${item.id}">
      <!-- 第一行：基金(1/3) / 投资方式 / 止盈方式 / 定投周期(含周几或每月几号互斥) -->
      <div class="grid grid-cols-1 md:grid-cols-6 gap-3 items-start">
        <div class="md:col-span-2"><label class="block text-xs text-gray-600 mb-1">基金</label><select data-field="fund" class="w-full p-2 border rounded-lg text-sm">${fundOpts}</select></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">投资方式</label><select data-field="investStrategy" class="w-full p-2 border rounded-lg text-sm">${invOpts}</select></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">止盈方式</label><select data-field="stopGain" class="w-full p-2 border rounded-lg text-sm">${sgOpts}</select></div>
        <div class="md:col-span-2">${scheduleBlock}</div>
      </div>
      <!-- 第二行：基础每期金额 / 开始/结束/分红 + 投资参数/止盈参数(占后半行) -->
      <div class="grid grid-cols-1 md:grid-cols-6 gap-3 items-start mt-3">
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">基础每期金额</label><input type="number" data-field="baseAmount" value="${item.baseAmount}" min="0" class="w-full p-2 border rounded-lg text-sm"></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">开始日期</label><input type="date" data-field="startDate" value="${item.startDate}" min="${fd.minDate || ''}" max="${fd.maxDate || ''}" class="w-full p-2 border rounded-lg text-sm"></div>
        <div class="md:col-span-1 ${edGrey ? 'opacity-50 cursor-not-allowed' : ''}"><label class="block text-xs text-gray-600 mb-1">结束日期</label><input type="date" data-field="endDate" value="${edGrey ? item.startDate : item.endDate}" ${edGrey ? 'disabled' : ''} min="${fd.minDate || ''}" max="${fd.maxDate || ''}" class="w-full p-2 border rounded-lg text-sm disabled:bg-gray-100 disabled:text-gray-400 disabled:cursor-not-allowed"></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">分红方式</label><select data-field="div" class="w-full p-2 border rounded-lg text-sm">${divOpts}</select></div>
        <div class="md:col-span-2">${extra}</div>
      </div>
      <div class="mt-2 text-left"><button data-act="del" class="text-red-500 hover:text-red-700 text-sm font-medium">删除此条目</button></div>
    </div>`;
}

function scExtraHtml(item) {
    const f = (label, field, attrs, val) => `<div class="min-w-0">
        <label class="block text-xs leading-tight text-gray-600 mb-1 truncate">${label}</label>
        <input type="number" data-field="${field}" value="${val}" ${attrs} class="w-16 p-2 border border-gray-300 rounded-lg text-sm">
    </div>`;
    const note = (t) => `<div class="text-[10px] text-gray-400 flex-1 self-center min-w-0 leading-snug">说明：${t}</div>`;
    // 投资方式专属参数
    let investPart = '';
    if (item.investStrategy === '4') {
        investPart = `<div class="flex items-center gap-2">
            <span class="text-xs font-medium text-indigo-600 whitespace-nowrap self-center">均线智能</span>
            <div class="flex items-end gap-2">${f('均线天数', 'maDays', 'min="5"', item.maDays)}${f('低位系数', 'lowCoef', 'step="0.1"', item.lowCoef)}${f('高位系数', 'highCoef', 'step="0.1"', item.highCoef)}</div>
            ${note('低于均线多投、高于均线少投（系数>1多投，<1少投）')}
        </div>`;
    } else if (item.investStrategy === '6') {
        investPart = `<div class="flex items-center gap-2">
            <span class="text-xs font-medium text-amber-600 whitespace-nowrap self-center">估值不定额</span>
            <div class="flex items-end gap-2">${f('历史窗口', 'valWindow', 'min="20"', item.valWindow)}${f('系数 k', 'valK', 'step="0.1"', item.valK)}</div>
            ${note('按历史分位调整投入：分位越低投越多（k越大调整越灵敏）')}
        </div>`;
    } else if (item.investStrategy === '7') {
        investPart = `<div class="flex items-center gap-2">
            <span class="text-xs font-medium text-rose-600 whitespace-nowrap self-center">最大回撤</span>
            <div class="flex items-end gap-2">${f('回撤窗口(日)', 'mdDays', 'min="5"', item.mdDays)}${f('回撤阈值(%)', 'mdPct', 'step="0.5"', item.mdPct)}</div>
            <label class="flex items-center gap-1 text-xs font-medium text-gray-700 cursor-pointer whitespace-nowrap"><input type="checkbox" data-field="mdContinuous" ${item.mdContinuous ? 'checked' : ''} class="w-4 h-4"> 连续投资</label>
            ${note(item.mdContinuous ? '窗口持续计算：回撤达阈值即每个交易日投一笔，回撤收窄至阈值以下即停止，可反复触发。' : '近窗口内从最高净值回撤达阈值时投一笔（baseAmount），创阶段新高后可再触发。')}
        </div>`;
    }
    // 止盈方式专属参数
    let sgPart = '';
    if (item.stopGain === 'target' || item.stopGain === 'gainratio') {
        const label = item.stopGain === 'target' ? '目标止盈' : '固定收益比例止盈';
        sgPart = `<div class="flex items-center gap-2 mt-1">
            <span class="text-xs font-medium text-emerald-600 whitespace-nowrap self-center">${label}</span>
            <div class="flex items-end gap-2">${f('收益阈值(%)', 'stopGainPct', 'step="0.5"', item.stopGainPct)}${f('赎回比例(%)', 'stopGainSellRatio', 'step="1" min="0" max="100"', item.stopGainSellRatio)}</div>
            ${note('累计收益达阈值时赎回设定比例份额，随后继续定投')}
        </div>`;
    } else if (item.stopGain === 'drawdown') {
        sgPart = `<div class="flex items-center gap-2 mt-1">
            <span class="text-xs font-medium text-sky-600 whitespace-nowrap self-center">回撤比例止盈</span>
            <div class="flex items-end gap-2">${f('回撤阈值(%)', 'stopGainDrawdown', 'step="0.5"', item.stopGainDrawdown)}${f('赎回比例(%)', 'stopGainSellRatio', 'step="1" min="0" max="100"', item.stopGainSellRatio)}</div>
            ${note('从近期高点回撤达阈值时赎回设定比例份额（移动止盈）')}
        </div>`;
    }
    if (!investPart && !sgPart) return '';
    return `<div class="w-full flex flex-col gap-1">${investPart}${sgPart}</div>`;
}

// 单条对比条目的策略模拟：复用与组合回测一致的分红/资产/指标口径
function simulateStrategy(item, pool) {
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
    const inv = item.investStrategy;
    const sg = item.stopGain;

    const isInvest = new Array(N).fill(false);
    if (inv === '1' || inv === '7') {
        // '1' 一次性：首日投一次；'7' 最大回撤为事件驱动，不生成定期日程（由循环内回撤条件触发）
        if (inv === '1') isInvest[0] = true;
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
    if (investIdx.length === 0 && inv !== '7') return null;   // 最大回撤为事件驱动，允许无定期日程

    let maArr = null;
    if (inv === '4') {
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
    if (inv === '6') {
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

    let shares = 0, totalCash = 0, totalInvested = 0, costBasis = 0, stopGainCount = 0, investCount = 0, totalRedeemed = 0, totalDividendCash = 0, sgPeakNav = 0;
    let runPrincipal = 0, maxPrincipal = 0, runMaxPrincipal = 0;   // 本轮本金(累计) / 全局最大本金(峰值,不重置) / 本轮峰值本金(随本轮重置)
    const cashFlows = [], flowDates = [], assets = [], invests = [], cashDivs = [], peakPrincipal = [];
    const flows = [];   // 每日带符号净外部现金流（买入>0，赎回<0，不含分红），供时间加权净值(份额法)使用
    const stopGainEvents = [];   // 记录每次止盈触发的交易日下标
    const holdAssets = [], holdCost = []; // 每日持仓市值 / 持仓成本（不含已落袋现金）

    // 最大回撤投资（inv==='7'）：滑动窗口最高净值，回撤达阈值即投一笔（baseAmount），窗口重置后可再次触发
    const mdDays = inv === '7' ? Math.max(5, parseInt(item.mdDays) || 120) : 0;
    const mdPctTh = inv === '7' ? (parseFloat(item.mdPct) || 10) / 100 : 0;
    const win = [];

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
            if (inv === '1' || inv === '2') amt = item.baseAmount;
            else if (inv === '3') { investCount++; amt = investCount * item.baseAmount - shares * nav; }
            else if (inv === '4') {
                const ma = maArr[k];
                let factor = 1;
                if (ma !== null && ma > 0) factor = nav < ma ? parseFloat(item.lowCoef) : parseFloat(item.highCoef);
                amt = item.baseAmount * factor;
            } else if (inv === '6') { const factor = 1 + (0.5 - pctArr[k]) * parseFloat(item.valK); amt = item.baseAmount * factor; }
        }
        // 最大回撤投资（事件驱动，inv==='7'）
        if (inv === '7' && mdDays > 0) {
            win.push(nav);
            if (win.length > mdDays) win.shift();
            let windowHigh = win[0];
            for (let w = 1; w < win.length; w++) if (win[w] > windowHigh) windowHigh = win[w];
            const hit = windowHigh > 0 && (windowHigh - nav) / windowHigh >= mdPctTh;
            if (item.mdContinuous) {
                // 连续投资：窗口持续滚动计算，满足回撤阈值即每交易日投一笔；不满足即不投（不重置窗口，可反复触发）
                if (hit) amt = item.baseAmount;
            } else if (hit) {
                amt = item.baseAmount;
                win.length = 0;   // 单笔模式：重置窗口，需重新累积新高峰后才可再次触发
            }
        }
        // 止盈（按 stopGain 类型；原策略5 行为由 investStrategy:'2'+stopGain:'target' 等价替代）
        // 收益率标准以"本轮最大投入本金(runMaxPrincipal)"为分母（target/gainratio）；
        // 回撤比例止盈(drawdown)以"近期高点 sgPeakNav"为基准判断是否从高点回撤达阈值。
        if (sg !== 'none') {
            const sellRatio = Math.min(1, Math.max(0, parseFloat(item.stopGainSellRatio) / 100));
            let trigger = false;
            if (sg === 'target' || sg === 'gainratio') {
                const th = parseFloat(item.stopGainPct) / 100;
                const roundPrincipal = runMaxPrincipal;   // 本轮峰值本金(截至昨日，不含今日未投部分)
                if (roundPrincipal > 0 && sellRatio > 0) {
                    const holdMv = shares * nav;          // 只算持仓市值，不含已落袋现金
                    if ((holdMv - costBasis) / roundPrincipal >= th) trigger = true;
                }
            } else if (sg === 'drawdown') {
                const th = parseFloat(item.stopGainDrawdown) / 100;
                sgPeakNav = Math.max(sgPeakNav, nav);     // 追踪近期高点
                if (sgPeakNav > 0 && sellRatio > 0 && shares > 1e-9 && (sgPeakNav - nav) / sgPeakNav >= th) trigger = true;
            }
            if (trigger) {
                const sellShares = shares * sellRatio;
                const proceeds = sellShares * nav;
                shares -= sellShares; totalCash += proceeds; totalRedeemed += proceeds;
                if (pool) pool.remaining += proceeds;   // 止盈赎回回充资金池，可再投
                cashFlows.push(proceeds); flowDates.push(date);
                costBasis *= (1 - sellRatio);
                stopGainCount++;
                stopGainEvents.push(k);      // 记录本次止盈触发的交易日下标
                if (sg === 'drawdown') sgPeakNav = nav;   // 移动止盈：赎回后从当前净值重新追踪高点
                // 全部赎回：本轮本金已落袋，重置后下一周期重新累计
                if (sellRatio >= 1 || shares < 1e-9) {
                    runPrincipal = 0;
                    runMaxPrincipal = 0;   // 同步重置本轮峰值本金
                }
            }
        }
        let didInvest = 0;   // 实际成功买入的金额（资金池不足被跳过时为 0）
        if (amt !== 0) {
            if (amt > 0) {
                if (!pool || pool.remaining >= amt) {
                    shares += amt / nav; totalInvested += amt; costBasis += amt; runPrincipal += amt;
                    cashFlows.push(-amt); flowDates.push(date);
                    if (pool) {
                        pool.remaining -= amt;
                        if (pool.minRemaining === undefined || pool.remaining < pool.minRemaining) pool.minRemaining = pool.remaining;
                    }
                    didInvest = amt;
                }
                // 资金池不足：跳过整笔买入（不计现金流、不增份额）
            } else {
                let sellShares = (-amt) / nav;
                if (sellShares > shares) sellShares = shares;
                if (sellShares > 0) {
                    const proceeds = sellShares * nav;
                    shares -= sellShares; totalCash += proceeds; totalRedeemed += proceeds;
                    if (pool) pool.remaining += proceeds;   // 止盈赎回回充资金池，可再投
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
        invests.push(didInvest);
        flows.push(amt < 0 ? amt : didInvest);   // 带符号现金流：买入只记实际成交，赎回保持原样
        cashDivs.push(totalCash);
        holdAssets.push(shares * nav);
        holdCost.push(costBasis);
    }

    const finalDate = dates[N - 1];
    const finalAsset = assets[N - 1];
    // 期末现金流只计剩余持仓市值，已赎回/分红现金此前已作为正现金流流出，避免重复计数导致 XIRR 失真
    cashFlows.push(shares * navs[N - 1]); flowDates.push(finalDate);
    if (pool) pool.remaining += shares * navs[N - 1];   // 期末清算释放资本，回充供后续条目使用
    const xirrVal = xirr(cashFlows, flowDates) * 100;
    const totalReturn = totalInvested > 0 ? (finalAsset / totalInvested - 1) * 100 : 0;
    const netProfit = finalAsset - totalInvested;                 // = 总赎回+市值+累积现金分红−总定投
    const maxPrincipalReturn = maxPrincipal > 0 ? netProfit / maxPrincipal * 100 : 0;   // 收益率(峰值本金)
    const m = computeMetrics(dates, assets, invests);
    return { item, dates, assets, invests, flows, cashDivs, holdAssets, holdCost, totalInvested, finalAsset, totalReturn, xirrVal, stopGainCount, maxPrincipal, maxPrincipalReturn, totalRedeemed, totalDividendCash, peakPrincipal, stopGainEvents, firstInvestDate: dates[investIdx[0]], metrics: m };
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
    // 解析全局共享资金池（留空/0/NaN = 不限制）
    const _scPoolEl = document.getElementById('scPool');
    const _scRaw = _scPoolEl ? parseFloat(_scPoolEl.value) : NaN;
    scPoolCap = (isFinite(_scRaw) && _scRaw > 0) ? _scRaw : Infinity;
    // minRemaining 记录运行过程中的最低余额，用于换算"峰值占用"
    // （每条目期末清算会把资本全额回充，故终值 remaining 恒≈上限，不能作为占用指标）
    scPool = { remaining: scPoolCap, minRemaining: scPoolCap };
    scItems.forEach(item => {
        const eff = Object.assign({}, item);
        if (commonStart !== null) {
            const st = new Date(item.startDate + 'T00:00:00').getTime();
            const en = new Date(item.endDate + 'T00:00:00').getTime();
            eff.startDate = formatDate(new Date(Math.max(st, commonStart)));
            eff.endDate = formatDate(new Date(Math.min(en, commonEnd)));
        }
        const r = simulateStrategy(eff, scPool);
        if (!r) { errors.push((fundCodeName(item.fund).name || item.fund) + '·' + SC_INVEST[item.investStrategy] + (item.stopGain !== 'none' ? '·' + SC_STOPGAIN[item.stopGain] : '') + '：区间数据不足'); return; }
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

// HTML 文本/属性转义，防止基金名称中的特殊字符破坏结构
function escAttr(s) {
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function hexToRgba(hex, alpha) {
    const h = (hex || '#000000').replace('#', '');
    const r = parseInt(h.substring(0, 2), 16);
    const g = parseInt(h.substring(2, 4), 16);
    const b = parseInt(h.substring(4, 6), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function renderScResults(errors) {
    const tbody = document.getElementById('scTableBody');
    let rows = '';
    scResults.forEach(r => {
        const it = r.item; const m = r.metrics; const cn = fundCodeName(it.fund);
        const fullName = cn.name || cn.code;
        const isSG = it.stopGain !== 'none';
        rows += `<tr class="border-b hover:bg-gray-50">
            <td class="px-1.5 py-1.5 text-center font-mono text-xs whitespace-nowrap">${cn.code}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${escAttr(fullName)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${SC_INVEST[it.investStrategy]}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${SC_STOPGAIN[it.stopGain]}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.totalInvested.toFixed(0)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.finalAsset.toFixed(0)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap ${isSG ? 'text-gray-400' : (r.totalReturn >= 0 ? 'text-emerald-600 font-medium' : 'text-rose-600 font-medium')}">${isSG ? '—' : r.totalReturn.toFixed(2) + '%'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isNaN(r.xirrVal) ? '-' : r.xirrVal.toFixed(2) + '%'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isNaN(m.maxDrawdown) ? '-' : m.maxDrawdown.toFixed(2) + '%'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isNaN(m.annualVolatility) ? '-' : (m.annualVolatility * 100).toFixed(2) + '%'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isNaN(m.sharpeRatio) ? '-' : m.sharpeRatio.toFixed(2)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${isSG ? r.stopGainCount : '—'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.maxPrincipal.toFixed(0)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap sc-col-maxpr ${isSG ? (r.maxPrincipalReturn >= 0 ? 'text-emerald-600 font-medium' : 'text-rose-600 font-medium') : 'text-gray-400'}">${isSG ? r.maxPrincipalReturn.toFixed(2) + '%' : '—'}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.totalRedeemed.toFixed(0)}</td>
            <td class="px-1.5 py-1.5 text-center text-xs whitespace-nowrap">${r.totalDividendCash.toFixed(0)}</td>
        </tr>`;
    });
    tbody.innerHTML = rows;

    // 隐藏「最大本金收益率」列：该列仅止盈策略有值，其余均为「—」；若无止盈条目则整列隐藏
    const showMaxPR = scResults.some(r => r.item.stopGain !== 'none');
    const firstMaxPR = tbody.querySelector('.sc-col-maxpr');
    if (firstMaxPR) {
        const idx = firstMaxPR.cellIndex;
        const th = tbody.closest('table') && tbody.closest('table').querySelector('thead tr') && tbody.closest('table').querySelector('thead tr').cells[idx];
        const disp = showMaxPR ? '' : 'none';
        if (th) th.style.display = disp;
        tbody.querySelectorAll('.sc-col-maxpr').forEach(td => td.style.display = disp);
    }

    document.getElementById('scRules').innerHTML = scResults.map(r => {
        const it = r.item; const cn = fundCodeName(it.fund);
        return `<div class="text-sm"><span class="font-medium">${cn.name || it.fund} · ${SC_INVEST[it.investStrategy]}${it.stopGain !== 'none' ? '·' + SC_STOPGAIN[it.stopGain] : ''}：</span>${scRuleText(it)}</div>`;
    }).join('');

    let errHtml = '';
    if (errors.length) errHtml += '<p class="text-amber-600 text-sm mt-2">⚠ ' + errors.join('；') + '</p>';
    if (scCommonStart !== null) errHtml += '<p class="text-xs text-gray-400 mt-1">* 多基金对比：各条目区间已对齐至历史较短基金的可用区间（' + formatDate(new Date(scCommonStart)) + ' 起）。</p>';
    document.getElementById('scErrors').innerHTML = errHtml;

    const metricsGrid = document.getElementById('scMetrics');
    let gridHtml = scResults.map((r, idx) => {
        const isSG = r.item.stopGain !== 'none';
        const xirrTxt = isNaN(r.xirrVal) ? '-' : r.xirrVal.toFixed(2) + '%';
        const retTxt = isSG ? '—' : (isNaN(r.totalReturn) ? '-' : r.totalReturn.toFixed(2) + '%');
        // 卡片底色/文字与下方曲线颜色保持一致
        const color = SC_COLORS[idx % SC_COLORS.length];
        const cardBg = hexToRgba(color, 0.12);
        const cn = fundCodeName(r.item.fund);
        // 止盈卡片：峰值本金收益率移入悬停提示，并补充止盈次数与间隔明细
        let tip = '';
        if (isSG) {
            tip = `峰值本金收益率 ${r.maxPrincipalReturn.toFixed(2)}%（最大本金 ${r.maxPrincipal.toFixed(0)}）\n`
                + `止盈明细：\n· 止盈次数：${r.stopGainCount} 次\n· ${sgIntervalText(r)}`;
        }
        return `<div class="p-3 rounded-lg text-center" style="background-color:${cardBg}"${tip ? ` title="${tip}"` : ''}>
            <div class="text-xs text-gray-500">${cn.name || r.item.fund} <br/>· ${SC_INVEST[r.item.investStrategy]}${r.item.stopGain !== 'none' ? '·' + SC_STOPGAIN[r.item.stopGain] : ''}</div>
            <div class="text-lg font-bold" style="color:${color}">${xirrTxt}</div>
            <div class="text-xs text-gray-500">XIRR年化 · 累计收益 ${retTxt}</div>
        </div>`;
    }).join('');
    metricsGrid.innerHTML = gridHtml;

    // 资金池剩余/上限：显示在「可用最大资金池(元)」输入框左侧
    const scPoolStatEl = document.getElementById('scPoolStat');
    if (scPoolStatEl) {
        if (isFinite(scPoolCap)) {
            const used = scPoolCap - (isFinite(scPool.minRemaining) ? scPool.minRemaining : scPoolCap);
            scPoolStatEl.textContent = `资金池峰值占用 / 上限：${used.toFixed(0)} / ${scPoolCap.toFixed(0)} 元`;
            scPoolStatEl.title = '所有策略共享此资金池；峰值占用 = 上限 − 运行过程中的最低余额（赎回与期末清算会回充，故终值余额不代表占用）';
            scPoolStatEl.classList.remove('hidden');
        } else {
            scPoolStatEl.textContent = '';
            scPoolStatEl.classList.add('hidden');
        }
    }

    document.getElementById('scResultArea').classList.remove('hidden');
    drawScChart();
}

function scRuleText(it) {
    const wdNames = ['周日','周一','周二','周三','周四','周五','周六'];
    const freqText = it.investStrategy === '1' ? '一次性' :
        (it.freq === 'weekly' ? '每周' + wdNames[+it.weekday] + '定投' :
         it.freq === 'biweekly' ? '每双周' + wdNames[+it.weekday] + '定投' :
         '每月' + (it.dayOfMonth === 'first' ? '首个交易日' : it.dayOfMonth + '号') + '定投');
    let investText;
    switch (it.investStrategy) {
        case '1': investText = `起始日一次性投入 ${it.baseAmount} 元，之后不再投入。`; break;
        case '2': investText = `每期固定投入 ${it.baseAmount} 元（${freqText}）。`; break;
        case '3': investText = `价值平均：目标市值每期递增 ${it.baseAmount} 元，低于目标多买、高于目标卖出。`; break;
        case '4': investText = `均线智能：净值低于 ${it.maDays} 日均线时投入 ×${it.lowCoef}，高于时 ×${it.highCoef}（基准每期 ${it.baseAmount} 元）。`; break;
        case '6': investText = `估值不定额：按近 ${it.valWindow} 交易日净值分位调整投入，系数 k=${it.valK}（基准每期 ${it.baseAmount} 元）。`; break;
        case '7': investText = it.mdContinuous
            ? `最大回撤连续投资：窗口内从区间最高净值回撤达 ${it.mdPct}% 即每个交易日投入 ${it.baseAmount} 元，回撤收窄至阈值以下即停止，可反复触发（不按周期定投）。`
            : `最大回撤投资：近 ${it.mdDays} 日内从区间最高净值回撤达 ${it.mdPct}% 时投入一笔 ${it.baseAmount} 元（创阶段新高后可再次触发，不按周期定投）。`; break;
        default: investText = '';
    }
    if (it.stopGain === 'none') return investText;
    let sgText;
    if (it.stopGain === 'target') {
        sgText = `目标止盈：累计收益达 ${it.stopGainPct}% 时赎回 ${it.stopGainSellRatio}% 份额并继续定投。`;
    } else if (it.stopGain === 'gainratio') {
        sgText = `固定收益比例止盈：累计收益率达 ${it.stopGainPct}% 时赎回 ${it.stopGainSellRatio}% 份额并继续定投。`;
    } else if (it.stopGain === 'drawdown') {
        sgText = `回撤比例止盈（移动止盈）：从近期高点回撤达 ${it.stopGainDrawdown}% 时赎回 ${it.stopGainSellRatio}% 份额并继续定投。`;
    }
    return investText + ' ' + sgText;
}

// XIRR 曲线展示限幅（%）：仅作用于图表逐日曲线，不影响策略对比表 XIRR 列。
// 超出上下界的值截断到边界，防止短持有期过度年化/伪根导致的几千几万极端值撑爆 y 轴。
const XIRR_CHART_CLAMP_MIN = -100;
const XIRR_CHART_CLAMP_MAX = 100;

// 滚动 XIRR 窗口配置：单一数据源，供计算 / 按钮同步 / 图例三处共用。
// key 同时用作 r._rollXirr 的缓存键；dash 为 Chart.js borderDash；alpha 控制线条透明度层次。
const SC_XIRR_WINDOWS = [
    { key: 'cum', label: '累计', years: null, dash: [],      alpha: 1.0, btnId: 'scXirrCum' },
    { key: 'y1',  label: '1年',  years: 1,    dash: [2, 2],  alpha: 0.9, btnId: 'scXirrY1'  },
    { key: 'y3',  label: '3年',  years: 3,    dash: [6, 3],  alpha: 0.8, btnId: 'scXirrY3'  },
    { key: 'y5',  label: '5年',  years: 5,    dash: [10, 4], alpha: 0.7, btnId: 'scXirrY5'  }
];

// 逐日资金加权收益率（XIRR 年化，%），与策略对比表里的 XIRR 同口径：
//   每日净外部现金流 = −当日投入 + 当日落袋现金（赎回/现金分红，取增量）；
//   期末价值只计剩余持仓市值（assets − cashDivs），已落袋现金此前已作正流，避免重复计数。
//   止盈策略天然被正确计入（赎回=正流、期末只算持仓）。
//
// years === null（累计口径）：对第 i 天用前缀现金流 [0..i] 求一次 XIRR，曲线终点等于表格 XIRR 值。
// years = N（N 年滚动口径）：只取最近 N 年窗口 [s..i] 的现金流，并在窗口起点补一笔负流
//   −holdVal[s−1]（窗口开始前一日的持仓市值），语义为"在窗口起点按当时市值买入现有持仓"，
//   时间基准 dayFrac 以 dates[s] 重新归零以保证年化正确。
//   当持有期不足 N 年（dates[i] − N年 < dates[0]）时 s 回落为 0 且不补期初负流，
//   公式自动还原为累计口径 —— 即"不足窗口退化为累计 XIRR"。
//
// 窗口起点按真实日期回溯定位（dates 是交易日序列，不能用固定索引偏移近似一年）；
// 因 i 单调递增故 s 亦单调递增，用双指针滑动 O(N) 完成定位。
// 结果由调用方缓存到 r._rollXirr[key]，切换 x 轴口径 / 勾选窗口时不重算。
function rollingXirr(r, years) {
    const N = r.dates.length;
    const out = new Array(N).fill(null);
    if (N < 2) return out;
    const inv = r.invests || [], cashDivs = r.cashDivs || [], assets = r.assets || [];
    const netFlow = new Array(N), holdVal = new Array(N);
    let prevCash = 0, cumInvest = 0;
    for (let j = 0; j < N; j++) {
        const realized = cashDivs[j] - prevCash; prevCash = cashDivs[j];
        netFlow[j] = -(inv[j] || 0) + (realized > 0 ? realized : 0);
        holdVal[j] = assets[j] - cashDivs[j];   // 持仓市值（不含已落袋现金）
    }
    const YEAR_MS = 1000 * 60 * 60 * 24 * 365;
    const ts = r.dates.map(d => d.getTime());
    // 第 i 天对应的窗口起点索引（累计口径恒为 0），按真实日期回溯，闰年由 setFullYear 处理
    const winStart = new Array(N).fill(0);
    if (years != null) {
        let s = 0;
        for (let i = 0; i < N; i++) {
            const cut = new Date(r.dates[i].getTime());
            cut.setFullYear(cut.getFullYear() - years);
            const cutTs = cut.getTime();
            while (s < i && ts[s] < cutTs) s++;
            winStart[i] = s;
        }
    }
    // 期初负流：窗口起点前一日的持仓市值；s===0（含不足窗口退化）时为 0，不补负流
    const openVal = i => {
        const s = winStart[i];
        return s > 0 ? holdVal[s - 1] : 0;
    };
    const npv = (rate, i) => {
        const s = winStart[i], base = ts[s];
        const frac = j => (ts[j] - base) / YEAR_MS;
        let sum = -openVal(i);                     // 期初持仓视为在窗口起点买入（负流，t=0）
        for (let j = s; j <= i; j++) sum += netFlow[j] / Math.pow(1 + rate, frac(j));
        return sum + holdVal[i] / Math.pow(1 + rate, frac(i));
    };
    const npvDeriv = (rate, i) => {
        const s = winStart[i], base = ts[s];
        const frac = j => (ts[j] - base) / YEAR_MS;
        let d = 0;                                 // 期初负流 t=0，对 rate 求导为 0，无需计入
        for (let j = s; j <= i; j++) d -= netFlow[j] * frac(j) / Math.pow(1 + rate, frac(j) + 1);
        d -= holdVal[i] * frac(i) / Math.pow(1 + rate, frac(i) + 1);
        return d;
    };
    let prevRate = 0.1;
    cumInvest = inv[0] || 0;                       // 计入第0天投入：一次性投资整笔落在 inv[0]
    for (let i = 1; i < N; i++) {
        cumInvest += (inv[i] || 0);
        if (cumInvest <= 0) continue;             // 尚未投入，XIRR 无意义
        // 滚动窗口：窗口内既无期初持仓也无任何投入时无从计算（例如已全部止盈赎回后的空仓段）
        if (years != null) {
            const s = winStart[i];
            if (s >= i) continue;
            let winInv = 0;
            for (let j = s; j <= i; j++) winInv += (inv[j] || 0);
            if (openVal(i) <= 0 && winInv <= 0) continue;
        }
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
        out[i] = (rate == null || !isFinite(rate)) ? null
            : Math.max(XIRR_CHART_CLAMP_MIN, Math.min(XIRR_CHART_CLAMP_MAX, rate * 100));
        if (out[i] != null) prevRate = rate;
    }
    return out;
}

// 累计口径薄封装，保持向后兼容
function runningXirr(r) {
    return rollingXirr(r, null);
}

// 取某条结果在指定窗口下的 XIRR 序列，惰性计算并按 key 缓存。
// scResults 重新赋值时结果对象整体重建，缓存随之自然失效，无需手动清理。
function scXirrSeries(r, key) {
    if (!r._rollXirr) r._rollXirr = {};
    if (!r._rollXirr[key]) {
        const w = SC_XIRR_WINDOWS.find(x => x.key === key) || SC_XIRR_WINDOWS[0];
        r._rollXirr[key] = rollingXirr(r, w.years);
    }
    return r._rollXirr[key];
}

// 时间加权净值（份额法/TWR）：新投入按当时时间加权净值折算成份额计入，时间加权净值 = 总资产 ÷ 总份额。
// 等价于时间加权净值(TWR)：扣除当日新增现金流后递推，加仓日只增份额、不改净值，曲线不再突变。
// 与 computeMetrics(js/backtest.js) 口径一致，对分红/止盈赎回鲁棒。起点恒为 1.0，首笔投入前返回 null。
function portfolioNetValues(assets, flows) {
    const nv = [];
    for (let i = 0; i < assets.length; i++) {
        if (i === 0) { nv.push(assets[i] > 0 ? 1.0 : null); continue; }
        const prev = assets[i - 1];
        if (prev > 0) nv.push((nv[i - 1] == null ? 1.0 : nv[i - 1]) * ((assets[i] - (flows[i] || 0)) / prev));
        else nv.push(assets[i] > 0 ? 1.0 : null);
    }
    return nv;
}

// 生成规律化日期刻度：以季度末(3/31,6/30,9/30,12/31)为基础网格，按数据跨度抽稀为
// 月末 / 季末 / 半年末 / 年末 / 多年末。产出刻度均为规整日历时点，且落在 [xMin,xMax] 闭区间内。
// 返回 { ticks: number[] 升序时间戳, unit: 'month'|'quarter'|'half'|'year'|'multiYear' }
function scDateTickInfo(xMin, xMax) {
    if (!(xMin != null && xMax != null && isFinite(xMin) && isFinite(xMax) && xMax > xMin)) {
        return { ticks: [], unit: 'month' };
    }
    const YEAR = 365.2425 * 24 * 60 * 60 * 1000;
    const spanYears = (xMax - xMin) / YEAR;
    // 自适应粒度：跨度越长刻度越稀
    let unit, stepMonths;
    if (spanYears < 0.75) { unit = 'month'; stepMonths = 1; }          // 短周期 -> 月末
    else if (spanYears <= 2) { unit = 'quarter'; stepMonths = 3; }      // 季度末
    else if (spanYears <= 5) { unit = 'half'; stepMonths = 6; }         // 半年末
    else if (spanYears <= 12) { unit = 'year'; stepMonths = 12; }       // 年末
    else { unit = 'multiYear'; stepMonths = spanYears > 24 ? 60 : 24; } // 每2/5年末

    // 从包含 xMin 的当月起，逐月推进，按 stepMonths 取季末/半年末/年末等规整点
    const start = new Date(xMin);
    let y = start.getFullYear(), m = start.getMonth(); // m: 0-11
    // 规整对齐：month 取当月月末逐月推进；其余模式对齐到更粗网格的起点
    if (unit !== 'month') {
        let alignMonth; // 0-11，对应目标网格的起始月末月
        if (unit === 'year' || unit === 'multiYear') alignMonth = 11;      // 年末 12/31
        else if (unit === 'half') alignMonth = (m <= 5) ? 5 : 11;          // 半年末 6/30 或 12/31
        else alignMonth = Math.floor(m / 3) * 3 + 2;                       // 季末 3/31,6/30,9/30,12/31
        // 若该网格起点月末已晚于(含) xMin 则用之，否则顺延一个 step
        if (new Date(y, alignMonth + 1, 0).getTime() < xMin) {
            m = alignMonth + stepMonths; if (m > 11) { m -= 12; y += 1; }
        } else {
            m = alignMonth;
        }
    }
    const ticks = [];
    const guard = 2000; // 安全阀，避免死循环
    for (let i = 0; i < guard; i++) {
        const ts = new Date(y, m + 1, 0).getTime(); // 当月最后一天(自然处理闰年2月)
        if (ts > xMax) break;
        if (ts >= xMin) ticks.push(ts);
        m += stepMonths;
        while (m > 11) { m -= 12; y += 1; }
    }
    // 规整刻度不足 2 个时，回退为区间均分并显示 YYYY-MM，保证轴不为空
    if (ticks.length < 2) {
        const n = 4;
        const out = [];
        for (let i = 0; i <= n; i++) out.push(Math.round(xMin + (xMax - xMin) * i / n));
        return { ticks: out, unit: 'month' };
    }
    return { ticks, unit };
}

// 按粒度格式化刻度短标签：year->'2023'，half->'2023H1'，quarter->'2023Q2'，month->'2023-05'
function scFormatDateTick(ts, unit) {
    const d = new Date(ts);
    const y = d.getFullYear(), m = d.getMonth(); // m: 0-11
    if (unit === 'year' || unit === 'multiYear') return String(y);
    if (unit === 'half') return m === 5 ? y + 'H1' : y + 'H2';
    if (unit === 'quarter') return y + 'Q' + (Math.floor(m / 3) + 1);
    return y + '-' + String(m + 1).padStart(2, '0'); // month
}

function drawScChart() {
    const canvas = document.getElementById('scChart');
    if (!canvas) return;
    if (scChart) scChart.destroy();
    const hasStopGain = scResults.some(x => x.item.stopGain !== 'none');
    const isXirr = scChartYMode === 'xirr';
    // XIRR 模式下按勾选顺序取窗口（保持 SC_XIRR_WINDOWS 的固定次序）；净值模式恒为单条曲线
    const activeWins = isXirr
        ? SC_XIRR_WINDOWS.filter(w => scXirrWindows.has(w.key))
        : [null];
    const datasets = [];
    scResults.forEach((r, idx) => {
        const startTs = r.dates[0].getTime();
        const inv = r.invests || [];
        const isStopGain = r.item.stopGain !== 'none';   // 止盈策略：净值采用"1+最大本金收益率"口径
        const sgSet = isStopGain && r.stopGainEvents ? new Set(r.stopGainEvents) : null;
        // 时间加权净值(份额法/TWR)口径：按结果预计算一次，net 模式且 scNetMode==='portfolio' 时启用
        const pnvArr = (scNetMode === 'portfolio') ? portfolioNetValues(r.assets, r.flows || []) : null;
        const color = SC_COLORS[idx % SC_COLORS.length];
        const cn = fundCodeName(r.item.fund);
        const sgLabel = r.item.stopGain !== 'none' ? '·' + SC_STOPGAIN[r.item.stopGain] : '';
        const baseLabel = (cn.name || r.item.fund) + '·' + SC_INVEST[r.item.investStrategy] + sgLabel;

        activeWins.forEach((win, wi) => {
            // 多窗口叠加时止盈点只挂在第一条曲线上，避免同一时点重复堆叠标记
            const wantSg = wi === 0;
            const rx = isXirr ? scXirrSeries(r, win.key) : null;
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
                    if (scNetMode === 'portfolio') {
                        // 时间加权净值(份额法/TWR)：新投入按当时时间加权净值折算份额，总资产÷总份额=时间加权净值；
                        // 加仓只增份额、不改净值，曲线在加仓日不再突变。
                        nv = pnvArr ? pnvArr[i] : null;
                    } else if (isStopGain) {
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
                if (wantSg && sgSet && sgSet.has(i)) sgPts.push({ x: xVal, y: yVal });
            });
            // 同一条目的多个窗口共用主色，靠线型与透明度区分层次
            const lineColor = isXirr && win.alpha < 1 ? hexToRgba(color, win.alpha) : color;
            datasets.push({
                label: isXirr && activeWins.length > 1 ? baseLabel + '·' + win.label : baseLabel,
                data: pts, borderColor: lineColor, backgroundColor: lineColor,
                borderDash: isXirr ? win.dash : [],
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
    });
    // 计算数据点真实 x 范围，避免 linear 轴做 nice 圆整时向两端扩展（出现起点早几个月/终点晚几个月）
    let xMin = Infinity, xMax = -Infinity;
    datasets.forEach(ds => ds.data.forEach(p => {
        if (p.x != null && isFinite(p.x)) { if (p.x < xMin) xMin = p.x; if (p.x > xMax) xMax = p.x; }
    }));
    if (!isFinite(xMin)) { xMin = undefined; xMax = undefined; }
    // 按日期模式：生成规律化刻度（季末/半年末/年末等），避免 linear 轴自动取点无规律
    const dateTickInfo = (scChartXMode === 'date' && xMin != null) ? scDateTickInfo(xMin, xMax) : null;
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
                    ticks: scChartXMode === 'date'
                        ? { autoSkip: false, callback: v => {
                              // 仅标注落在规律刻度上的值，其余留空保持整齐
                              const t = dateTickInfo && dateTickInfo.ticks.includes(+v.toFixed(0))
                                  ? +v.toFixed(0) : null;
                              return t != null ? scFormatDateTick(t, dateTickInfo.unit) : '';
                          } }
                        : { maxTicksLimit: 12, callback: v => v + '月' },
                    // date 模式：用 afterBuildTicks 直接替换为规律刻度，防止 maxTicksLimit 二次抽稀
                    ...(scChartXMode === 'date' ? {
                        afterBuildTicks: (axis) => {
                            axis.ticks = (dateTickInfo && dateTickInfo.ticks.length)
                                ? dateTickInfo.ticks.map(v => ({ value: v })) : axis.ticks;
                        }
                    } : {})
                },
                y: { title: { display: true, text: isXirr
                    ? '资金加权收益率 XIRR 年化（%）· ' + activeWins.map(w => w.label).join(' / ')
                    : (scNetMode === 'portfolio'
                        ? '时间加权净值（份额法：总资产÷总份额，起点=1.0）'
                        : (hasStopGain
                            ? '投资净值（普通策略=总资产/累计投入；止盈策略=1+最大本金收益率，起点=1.0）'
                            : '投资净值（资金加权，总资产/累计投入，起点=1.0）')) } }
            },
            plugins: {
                legend: { position: 'bottom', labels: { filter: (item, data) => !data.datasets[item.datasetIndex].isStopGainMarker } },
                tooltip: { callbacks: {
                    title: items => scChartXMode === 'date'
                        ? formatDate(new Date(items[0].parsed.x)) : (items[0].parsed.x + ' 月'),
                    // 多曲线叠加时数值需带上曲线名，否则无法分辨属于哪个窗口
                    label: item => item.dataset.isStopGainMarker ? item.dataset.label
                        : (isXirr ? item.dataset.label + '：' + item.parsed.y.toFixed(2) + '%'
                                  : item.parsed.y.toFixed(4))
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
        ? '投资收益率曲线（资金加权 XIRR 年化，%）' + scXirrWindowSuffix()
        : (scNetMode === 'portfolio'
            ? '时间加权净值曲线（份额法，起点=1.0）'
            : '投资净值曲线（资金加权，起点=1.0）');
    const note = document.getElementById('scChartNote');
    if (note) note.innerHTML = mode === 'xirr'
        ? SC_XIRR_NOTE
        : (scNetMode === 'portfolio'
            ? '* 采用"时间加权净值（份额法）"（起点=1.0）：新投入的一笔钱按当时时间加权净值折算成份额计入，时间加权净值 = 当日总资产 ÷ 总份额。加仓只增加份额、不改变净值，故最大回撤等事件驱动策略在加仓日曲线不再突变，只反映市场本身涨跌。注意：该口径剔除了投入时点差异，同一基金上不同策略曲线会重合；如需看投入节奏带来的策略差异，请切回"资金加权"。可切换"按持有期月数对齐"或"按日期"调整 x 轴。'
            : '* 采用资金加权的"资金加权净值"（起点=1.0）：当日净值 = 当日总资产 ÷ 截至当日的累计已投入本金。该口径保留了投入节奏的影响——上涨市中单笔（一次投入）会跑在定投（分批投入）上方，能直接看出策略差异；但事件驱动加仓（如最大回撤）会在加仓日因分母跳增而突变/被压低。若想消除该突变，可切到"时间加权净值"。可切换"按持有期月数对齐"或"按日期"调整 x 轴。');
    // 两组按钮互斥：净值模式只显示"资金加权/时间加权净值"组，XIRR 模式只显示滚动窗口组（display 切换，按需显示）
    const netToggle = document.getElementById('scNetToggle');
    if (netToggle) {
        const show = mode === 'net';
        netToggle.style.display = show ? '' : 'none';
        netToggle.style.opacity = '';
        netToggle.style.pointerEvents = '';
    }
    // 滚动窗口组与之对称：仅 XIRR 模式显示
    const winGroup = document.getElementById('scXirrWindowGroup');
    if (winGroup) {
        const show = mode === 'xirr';
        winGroup.style.display = show ? '' : 'none';
        winGroup.style.opacity = '';
        winGroup.style.pointerEvents = '';
    }
    syncScNetModeBtns();
    syncScXirrWindowBtns();
    if (scResults.length) drawScChart();
}

// 同步"资金加权/时间加权净值"按钮高亮
function syncScNetModeBtns() {
    const mw = document.getElementById('scNetMw');
    const pf = document.getElementById('scNetPf');
    if (!mw && !pf) return;
    const on = 'px-3 py-1.5 bg-blue-600 text-white';
    const off = 'px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100';
    if (mw) mw.className = scNetMode === 'mw' ? on : off;
    if (pf) pf.className = scNetMode === 'portfolio' ? on : off;
}

// 切换净值口径：'mw' 资金加权（默认）| 'portfolio' 时间加权净值(份额法/TWR)
function setScNetMode(mode) {
    scNetMode = mode;
    syncScNetModeBtns();
    const t = document.getElementById('scChartTitle');
    if (t) t.textContent = scNetMode === 'portfolio'
        ? '时间加权净值曲线（份额法，起点=1.0）'
        : '投资净值曲线（资金加权，起点=1.0）';
    const note = document.getElementById('scChartNote');
    if (note) note.innerHTML = scNetMode === 'portfolio'
        ? '* 采用"时间加权净值（份额法）"（起点=1.0）：新投入的一笔钱按当时时间加权净值折算成份额计入，时间加权净值 = 当日总资产 ÷ 总份额。加仓只增加份额、不改变净值，故最大回撤等事件驱动策略在加仓日曲线不再突变，只反映市场本身涨跌。注意：该口径剔除了投入时点差异，同一基金上不同策略曲线会重合；如需看投入节奏带来的策略差异，请切回"资金加权"。可切换"按持有期月数对齐"或"按日期"调整 x 轴。'
        : '* 采用资金加权的"资金加权净值"（起点=1.0）：当日净值 = 当日总资产 ÷ 截至当日的累计已投入本金。该口径保留了投入节奏的影响——上涨市中单笔（一次投入）会跑在定投（分批投入）上方，能直接看出策略差异；但事件驱动加仓（如最大回撤）会在加仓日因分母跳增而突变/被压低。若想消除该突变，可切到"时间加权净值"。可切换"按持有期月数对齐"或"按日期"调整 x 轴。';
    if (scResults.length) drawScChart();
}

// XIRR 模式说明文案（含滚动窗口口径与不足期退化规则）
const SC_XIRR_NOTE = '* 采用资金加权的 XIRR 年化收益率（"累计"口径与策略对比表"XIRR年化"一致，曲线终点=该值）：每日净外部现金流 = −当日投入 + 当日落袋现金（赎回/现金分红），期末价值只计剩余持仓市值，已落袋现金此前已作正流避免重复计数；止盈赎回作为正流计入，曲线不会因赎回而假跌。<br>* <b>累计</b>＝自投资起点至当日；<b>1/3/5年滚动</b>＝只统计最近 N 年区间内的资金加权年化，窗口起点的既有持仓按当时市值视为在该点买入（计为期初流出）。持有期不足 N 年时自动退化为累计口径，故曲线前段与"累计"重合。<br>* 窗口按钮可多选叠加：同一条目的不同窗口共用主色、以线型与深浅区分。一般 1 年线波动最剧烈（反映近期表现），累计线最平滑（长期表现）。曲线限幅 ±100%，防止短期过度年化撑爆 y 轴。可切换"按持有期月数对齐"或"按日期"调整 x 轴。';

// 图表标题中的窗口后缀，如"· 累计 / 1年"；仅勾选累计时不加后缀，保持原标题不变
function scXirrWindowSuffix() {
    const sel = SC_XIRR_WINDOWS.filter(w => scXirrWindows.has(w.key));
    if (sel.length === 1 && sel[0].key === 'cum') return '';
    return '· ' + sel.map(w => w.label).join(' / ');
}

// 同步滚动窗口多选按钮的高亮（元素不存在时安全跳过，index.html 无此按钮组）
function syncScXirrWindowBtns() {
    const on = 'px-3 py-1.5 bg-blue-600 text-white';
    const off = 'px-3 py-1.5 bg-white text-gray-700 hover:bg-gray-100';
    SC_XIRR_WINDOWS.forEach(w => {
        const el = document.getElementById(w.btnId);
        if (!el) return;
        el.className = scXirrWindows.has(w.key) ? on : off;
    });
}

// 切换某个 XIRR 窗口的勾选状态（多选叠加）。取消最后一个时回落到"累计"，保证图表始终有曲线。
function toggleScXirrWindow(key) {
    if (scXirrWindows.has(key)) scXirrWindows.delete(key);
    else scXirrWindows.add(key);
    if (scXirrWindows.size === 0) scXirrWindows.add('cum');
    syncScXirrWindowBtns();
    const t = document.getElementById('scChartTitle');
    if (t && scChartYMode === 'xirr') {
        t.textContent = '投资收益率曲线（资金加权 XIRR 年化，%）' + scXirrWindowSuffix();
    }
    if (scResults.length) drawScChart();
}



