/* backtest.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// XIRR
function xirr(cashFlows, dates, guess = 0.1) {
    if (cashFlows.length !== dates.length || cashFlows.length < 2) return NaN;
    const paired = cashFlows.map((cf, i) => ({ cf, date: dates[i] }));
    paired.sort((a, b) => a.date - b.date);
    const sortedFlows = paired.map(p => p.cf);
    const sortedDates = paired.map(p => p.date);
    const baseDate = sortedDates[0];
    const dayFrac = j => (sortedDates[j] - baseDate) / (1000 * 60 * 60 * 24) / 365;
    const npv = (rate) => {
        let s = 0;
        for (let j = 0; j < sortedFlows.length; j++) s += sortedFlows[j] / Math.pow(1 + rate, dayFrac(j));
        return s;
    };
    const tolerance = 1e-7, maxIter = 100;
    // 牛顿法：传统现金流（单次变号）收敛快
    let rate = guess;
    for (let i = 0; i < maxIter; i++) {
        let v = 0, d = 0;
        for (let j = 0; j < sortedFlows.length; j++) {
            const f = dayFrac(j);
            const term = Math.pow(1 + rate, f);
            v += sortedFlows[j] / term;
            d -= sortedFlows[j] * f * Math.pow(1 + rate, f - 1);
        }
        if (Math.abs(v) < tolerance) return rate;
        if (Math.abs(d) < tolerance) break;
        rate -= v / d;
    }
    // 牛顿法失败（止盈等非传统现金流存在多次变号）→ 二分法在 [-0.9999, 100] 内求根
    let lo = -0.9999, hi = 100;
    let fLo = npv(lo), fHi = npv(hi);
    if (fLo * fHi > 0) return NaN;
    for (let i = 0; i < 200; i++) {
        const mid = (lo + hi) / 2;
        const fMid = npv(mid);
        if (Math.abs(fMid) < tolerance) return mid;
        if (fLo * fMid < 0) { hi = mid; fHi = fMid; }
        else { lo = mid; fLo = fMid; }
    }
    return (lo + hi) / 2;
}

// 通用指标计算：由对齐后的日期/资产/每日投入序列计算净值曲线与风险收益指标
// 与组合回测口径完全一致（时间加权净值 TWR），供组合回测与策略比较共用
// 净值只反映市场涨跌：每日增长率 = (今日总资产 − 今日新增投入) / 昨日总资产，起点恒为 1.0
function computeMetrics(validDates, validAssets, validInvest) {
    const n = validAssets.length;
    let netValues = [];
    let annualVolatility = NaN, sharpeRatio = NaN, calmarRatio = NaN, maxDrawdown = 0;
    let annualReturnPct = NaN, winRate = NaN, maxDDDuration = NaN;

    if (n >= MIN_TRADE_DAYS) {
        // 时间加权净值（TWR）：扣除当日新增投入后递推，加钱不会抬高净值
        netValues = [1.0];
        for (let i = 1; i < validAssets.length; i++) {
            const yesterdayAsset = validAssets[i-1];
            const todayAsset = validAssets[i];
            const todayInvest = validInvest[i];
            if (yesterdayAsset > 0) netValues.push(netValues[i-1] * ((todayAsset - todayInvest) / yesterdayAsset));
            else netValues.push(1.0);
        }
        const dailyReturns = [];
        for (let i = 1; i < netValues.length; i++) dailyReturns.push((netValues[i] - netValues[i-1]) / netValues[i-1]);
        const totalDays = (validDates[validDates.length-1] - validDates[0]) / (1000*60*60*24);
        const annualReturn = Math.pow(netValues[netValues.length-1], 365/totalDays) - 1;
        const mean = dailyReturns.reduce((a,b)=>a+b,0)/dailyReturns.length;
        const variance = dailyReturns.reduce((a,b)=>a+Math.pow(b-mean,2),0)/dailyReturns.length;
        annualVolatility = Math.sqrt(variance) * Math.sqrt(252);
        let peak = netValues[0];
        for (const v of netValues) { if (v > peak) peak = v; const dd = (v - peak) / peak; if (dd < maxDrawdown) maxDrawdown = dd; }
        maxDrawdown *= 100;
        if (annualVolatility > 0) sharpeRatio = (annualReturn - RISK_FREE_RATE) / annualVolatility;
        if (maxDrawdown !== 0) calmarRatio = annualReturn / Math.abs(maxDrawdown/100);
        annualReturnPct = annualReturn * 100;
        winRate = dailyReturns.length ? dailyReturns.filter(r => r > 0).length / dailyReturns.length * 100 : NaN;
        let peakV = netValues[0], ddFrom = null, maxSpan = 0;
        for (let i = 0; i < netValues.length; i++) {
            if (netValues[i] > peakV) { peakV = netValues[i]; ddFrom = null; }
            else if (netValues[i] < peakV) {
                if (ddFrom === null) ddFrom = i;
                const span = (validDates[i] - validDates[ddFrom]) / 86400000;
                if (span > maxSpan) maxSpan = span;
            }
        }
        maxDDDuration = maxSpan;
    }
    return { netValues, annualVolatility, sharpeRatio, calmarRatio, maxDrawdown, annualReturnPct, winRate, maxDDDuration };
}

// ============ 基金数据持久化（IndexedDB，与基准一致） ============
function serializeFundRow(code, f, order) {
    return {
        code: code,
        dates: f.dates.map(d => formatDate(d)),
        nav: f.nav, div: f.div,
        minDate: f.minDate, maxDate: f.maxDate,
        order: order
    };
}
function rowToFund(row) {
    return {
        dates: row.dates.map(s => new Date(s + 'T00:00:00')),
        nav: row.nav, div: row.div,
        minDate: row.minDate, maxDate: row.maxDate
    };
}
async function saveFundsToDB() {
    const codes = Object.keys(fundsData);
    const rows = codes.map((code, idx) => serializeFundRow(code, fundsData[code], idx));
    await db.funds.clear();
    await db.funds.bulkPut(rows);
}
async function loadFundsFromDB() {
    const rows = await db.funds.toArray();
    // 按导入时记录的 order 排序还原 Excel 工作表顺序；旧缓存无 order 时回退到末尾（保持原代码序）
    rows.sort((a, b) => (a.order == null ? Infinity : a.order) - (b.order == null ? Infinity : b.order));
    fundsData = {};
    rows.forEach(r => { fundsData[r.code] = rowToFund(r); });
    return Object.keys(fundsData).length > 0;
}
// 任何基金变动都清空所有计划 + 策略沙盒（用户决定）
function clearAllPlanData() {
    investmentPlans = [];
    scItems = [];
    renderPlanList();
    renderScItems();
}
async function deleteFund(code) {
    delete fundsData[code];
    await db.funds.delete(code);
    clearAllPlanData();
    refreshFundUI();
}
async function deleteSelectedFunds(codes) {
    codes.forEach(c => delete fundsData[c]);
    await db.funds.bulkDelete(codes);
    clearAllPlanData();
    refreshFundUI();
}
async function clearAllFunds() {
    fundsData = {};
    await db.funds.clear();
    clearAllPlanData();
    refreshFundUI();
}
// UI 同步：基金列表 / 计划卡片显隐 / 本地计数
function refreshFundUI() {
    renderFundList();
    const has = Object.keys(fundsData).length > 0;
    ['planListSection','resultSection'].forEach(id => {
        const el = document.getElementById(id); if (el) el.style.display = has ? 'block' : 'none';
    });
    const hint = document.getElementById('fundStorageHint');
    if (hint) hint.textContent = has ? ('本地已存 ' + Object.keys(fundsData).length + ' 只') : '本地无数据';
}
function renderFundList() {
    const list = document.getElementById('fundList');
    if (!list) return;
    const codes = Object.keys(fundsData);
    if (codes.length === 0) { list.innerHTML = '<p class="text-gray-400 text-sm">暂无本地基金数据（上传 Excel 后自动保存）</p>'; return; }
    list.innerHTML = codes.map(code => {
        const f = fundsData[code];
        return `<div class="flex items-center gap-2 text-sm py-1" data-code="${code}">
            <input type="checkbox" class="fund-check" value="${code}">
            <span class="font-mono text-gray-700">${code}</span>
            <span class="text-gray-500">${f.minDate} ~ ${f.maxDate}</span>
            <span class="text-gray-400 text-xs">${f.nav.length} 条</span>
            <button class="fund-del ml-auto text-red-500 hover:text-red-700 text-xs font-medium" data-code="${code}">删除</button>
        </div>`;
    }).join('');
    list.querySelectorAll('.fund-del').forEach(btn => btn.addEventListener('click', () => {
        if (confirm('删除基金 ' + btn.dataset.code + '？所有相关计划将一并清空。')) deleteFund(btn.dataset.code);
    }));
}
// 基金上传（方案B：合并，同名覆盖/新名追加；写入 IndexedDB 并清空所有计划）
document.getElementById('fileInput').addEventListener('change', function(e) {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = async function(e) {
        const data = new Uint8Array(e.target.result);
        const workbook = XLSX.read(data, {type: 'array', cellDates: true});
        let added = 0;
        workbook.SheetNames.forEach(name => {
            const sheet = workbook.Sheets[name];
            const json = XLSX.utils.sheet_to_json(sheet, {header: 1, raw: true});
            const dates = [], nav = [], div = [];
            for (let i = 1; i < json.length; i++) {
                if (json[i][0] && json[i][1] !== undefined) {
                    const date = parseDateFlexible(json[i][0]);
                    if (!isNaN(date.getTime())) {
                        dates.push(date);
                        nav.push(parseFloat(json[i][1]));
                        div.push(parseFloat(json[i][2]) || 0);
                    }
                }
            }
            if (dates.length > 0) {
                const minDate = formatDate(dates[0]);
                const maxDate = formatDate(dates[dates.length-1]);
                fundsData[name] = {dates, nav, div, minDate, maxDate};
                added++;
            }
        });
        if (added > 0) {
            await saveFundsToDB();
            clearAllPlanData();
            refreshFundUI();
        }
    };
    reader.readAsArrayBuffer(file);
});

// 基金列表工具栏：全选 / 批量删除 / 清空全部
document.getElementById('fundSelectAll').addEventListener('change', e => {
    document.querySelectorAll('.fund-check').forEach(c => c.checked = e.target.checked);
});
document.getElementById('fundBatchDeleteBtn').addEventListener('click', async () => {
    const codes = [...document.querySelectorAll('.fund-check:checked')].map(c => c.value);
    if (!codes.length) { alert('请先勾选要删除的基金'); return; }
    if (!confirm('确认删除选中的 ' + codes.length + ' 只基金？所有相关计划将一并清空。')) return;
    await deleteSelectedFunds(codes);
});
document.getElementById('fundClearAllBtn').addEventListener('click', async () => {
    if (!Object.keys(fundsData).length) return;
    if (!confirm('清空全部本地基金数据？所有计划将一并清空。')) return;
    await clearAllFunds();
});

function addPlan() {
    const funds = Object.keys(fundsData);
    if (funds.length === 0) { alert('请先在「数据管理」中上传基金净值数据'); return; }
    const fund = funds[0];
    const f = fundsData[fund];
    const plan = {
        id: Date.now() + Math.floor(Math.random() * 1000),
        fund,
        type: 'monthly',
        startDate: f.minDate,
        endDate: f.maxDate,
        amount: 1000,
        div: 'reinvest',
        weekday: 1,
        dayOfMonth: 'first',
        stopGain: false,
        stopGainPct: 8,
        stopGainSellRatio: 100
    };
    const existingIndex = investmentPlans.findIndex(p => p.fund === fund);
    if (existingIndex !== -1) {
        if (!confirm(`基金 ${fund} 已有计划，是否覆盖？`)) return;
        investmentPlans[existingIndex] = plan;
    } else investmentPlans.push(plan);
    renderPlanList();
    checkNavGaps();
}
function deletePlan(id) { investmentPlans = investmentPlans.filter(p => p.id !== id); renderPlanList(); checkNavGaps(); }
function clearAllPlans() { if (confirm('清空所有计划？')) { investmentPlans = []; renderPlanList(); checkNavGaps(); } }

// 检测基金净值空档：以基准指数连续交易日（优先）或计划内所有基金日期并集（回退）为参照日历
// （基准日期缓存 benchmarkDateStrs 与 refreshBenchmarkCache 定义于 js/benchmarks.js，因基准脚本先于本文件加载）
function checkNavGaps() {
    const hint = document.getElementById('navGapHint');
    if (!hint) return;
    if (investmentPlans.length === 0) { hint.classList.add('hidden'); hint.innerHTML = ''; return; }

    const fundsInPlan = [...new Set(investmentPlans.map(p => p.fund))];

    // 参照日历：优先用基准指数的连续交易日；无基准时回退到多基金日期并集
    const useBenchmark = benchmarkDateStrs.length > 0;
    const refStrs = useBenchmark
        ? benchmarkDateStrs
        : (() => {
            const s = new Set();
            fundsInPlan.forEach(code => { const f = fundsData[code]; if (f) f.dates.forEach(d => s.add(formatDate(d))); });
            return Array.from(s).sort();
        })();

    const gapFunds = [];
    fundsInPlan.forEach(code => {
        const f = fundsData[code];
        if (!f || f.dates.length === 0) return;
        const firstDs = formatDate(f.dates[0]);          // 基金成立日
        const ownSet = new Set(f.dates.map(d => formatDate(d)));
        let missing = 0;
        for (const ds of refStrs) {
            if (ds < firstDs) continue;                  // 尚未成立，不算空白
            if (!ownSet.has(ds)) missing++;              // 成立后、参照日历有而本基金缺 -> 空白
        }
        if (missing > 0) gapFunds.push({ code, missing });
    });

    // 无基准提示（仅提示，不影响并集回退检测）
    const warnHtml = useBenchmark ? '' :
        '<span class="text-amber-600 text-sm">⚠ 未上传/未选择基准指数，无法检测工作日缺失，净值可能存在空白。请上传基准指数。</span><br>';

    if (gapFunds.length === 0) {
        hint.classList.remove('hidden');
        const okMsg = useBenchmark
            ? '✓ 所有基金净值相对基准指数完整，无需填充。'
            : '✓ 所有基金净值日期一致，无需填充。';
        hint.innerHTML = warnHtml + `<span class="text-green-600 text-sm">${okMsg}</span>`;
        return;
    }
    const names = gapFunds.map(g => `${g.code}(缺失${g.missing}天)`).join('、');
    const gapMsg = useBenchmark
        ? `⚠ 检测到以下基金相对基准指数存在空白净值：${names}。缺失日将按前一交易日净值模拟填充。`
        : `⚠ 检测到以下基金相对于其它基金存在空白净值：${names}。缺失日将按前一交易日净值模拟填充。`;
    hint.classList.remove('hidden');
    hint.innerHTML = warnHtml + `${gapMsg}
        <label class="ml-2 inline-flex items-center gap-1 text-sm font-medium text-amber-700 cursor-pointer">
            <input type="checkbox" id="fillNavCheck" ${fillMissingNav ? 'checked' : ''} onchange="fillMissingNav=this.checked"> 填充空白净值
        </label>`;
}
// 从 fundsData 的 key（"代码_基金名称"）拆分出代码与名称
function fundCodeName(key) {
    const idx = String(key).indexOf('_');
    if (idx > 0) return { code: key.slice(0, idx), name: key.slice(idx + 1) };
    return { code: String(key), name: String(key) };
}

// 单张计划卡片（内联可编辑，data-field 写回 investmentPlans 对应对象）
function planCardHtml(p) {
    const fundOpts = Object.keys(fundsData).map(k => `<option value="${k}" ${k === p.fund ? 'selected' : ''}>${k}</option>`).join('');
    const typeOpts = `
        <option value="single" ${p.type === 'single' ? 'selected' : ''}>单笔</option>
        <option value="weekly" ${p.type === 'weekly' ? 'selected' : ''}>每周定投</option>
        <option value="biweekly" ${p.type === 'biweekly' ? 'selected' : ''}>每双周定投</option>
        <option value="monthly" ${p.type === 'monthly' ? 'selected' : ''}>每月定投</option>`;
    const wdOpts = [1,2,3,4,5].map(d => `<option value="${d}" ${String(p.weekday) === String(d) ? 'selected' : ''}>${wdNames[d]}</option>`).join('');
    const domOpts = ['first','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28'].map(d => {
        const t = d === 'first' ? '每月首个交易日' : (d + ' 号');
        return `<option value="${d}" ${String(p.dayOfMonth) === String(d) ? 'selected' : ''}>${t}</option>`;
    }).join('');
    const divOpts = `<option value="reinvest" ${p.div === 'reinvest' ? 'selected' : ''}>红利再投资</option><option value="cash" ${p.div === 'cash' ? 'selected' : ''}>现金分红</option>`;
    const fd = fundsData[p.fund] || {};
    const isSingle = p.type === 'single';
    const showWeekday = p.type === 'weekly' || p.type === 'biweekly';
    const showDom = p.type === 'monthly';
    const disCls = 'disabled:bg-gray-100 disabled:text-gray-400 disabled:cursor-not-allowed';
    const daySlot = showWeekday
        ? `<label class="block text-xs text-gray-600 mb-1">定投星期</label>
           <select data-field="weekday" class="w-full p-2 border rounded-lg text-sm ${disCls}">${wdOpts}</select>`
        : showDom
        ? `<label class="block text-xs text-gray-600 mb-1">每月几号</label>
           <select data-field="dayOfMonth" class="w-full p-2 border rounded-lg text-sm ${disCls}">${domOpts}</select>`
        : `<label class="block text-xs text-gray-600 mb-1">定投周期</label>
           <select disabled class="w-full p-2 border rounded-lg bg-gray-100 text-gray-400 cursor-not-allowed text-sm"><option>—</option></select>`;
    const stopGainPctDiv = p.stopGain ? `<div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">止盈阈值(%)</label><input type="number" data-field="stopGainPct" value="${p.stopGainPct}" min="0.1" step="0.1" class="w-full p-2 border rounded-lg text-sm"></div>` : '';
    const stopGainSellDiv = p.stopGain ? `<div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">赎回比例(%)</label><input type="number" data-field="stopGainSellRatio" value="${p.stopGainSellRatio}" min="1" max="100" step="1" class="w-full p-2 border rounded-lg text-sm"></div>` : '';
    return `
    <div class="border rounded-lg p-3 bg-gray-50" data-id="${p.id}">
      <div class="grid grid-cols-1 md:grid-cols-6 gap-3 items-start">
        <div class="md:col-span-2"><label class="block text-xs text-gray-600 mb-1">基金</label><select data-field="fund" class="w-full p-2 border rounded-lg text-sm">${fundOpts}</select></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">投资类型</label><select data-field="type" class="w-full p-2 border rounded-lg text-sm">${typeOpts}</select></div>
        <div class="md:col-span-2">${daySlot}</div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">金额(元)</label><input type="number" data-field="amount" value="${p.amount}" min="100" class="w-full p-2 border rounded-lg text-sm"></div>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-6 gap-3 items-start mt-3">
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">开始日期</label><input type="date" data-field="startDate" value="${p.startDate}" min="${fd.minDate || ''}" max="${fd.maxDate || ''}" class="w-full p-2 border rounded-lg text-sm"></div>
        <div class="md:col-span-1 ${isSingle ? 'opacity-50 cursor-not-allowed' : ''}"><label class="block text-xs text-gray-600 mb-1">结束日期</label><input type="date" data-field="endDate" value="${isSingle ? p.startDate : p.endDate}" ${isSingle ? 'disabled' : ''} min="${fd.minDate || ''}" max="${fd.maxDate || ''}" class="w-full p-2 border rounded-lg text-sm ${disCls}"></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">分红方式</label><select data-field="div" class="w-full p-2 border rounded-lg text-sm">${divOpts}</select></div>
        <div class="md:col-span-1 flex items-end"><label class="flex items-center gap-2 text-xs font-medium text-gray-700 cursor-pointer h-9"><input type="checkbox" data-field="stopGain" ${p.stopGain ? 'checked' : ''} class="w-4 h-4"> 目标止盈</label></div>
        ${stopGainPctDiv}
        ${stopGainSellDiv}
      </div>
      <div class="mt-2 text-right"><button data-act="del" class="text-red-500 hover:text-red-700 text-sm font-medium">删除此计划</button></div>
    </div>`;
}

function renderPlanList() {
    const container = document.getElementById('planList');
    if (!container) return;
    if (investmentPlans.length === 0) { container.innerHTML = '<p class="text-gray-500 text-sm">暂无计划，点击右上角「＋ 添加计划」。</p>'; return; }
    container.innerHTML = investmentPlans.map(p => planCardHtml(p)).join('');
    investmentPlans.forEach(p => {
        const card = container.querySelector('[data-id="' + p.id + '"]');
        if (!card) return;
        card.querySelectorAll('[data-field]').forEach(el => {
            el.addEventListener('change', e => {
                const f = e.target.dataset.field;
                let v = e.target.value;
                if (e.target.type === 'number') v = parseFloat(v);
                if (e.target.type === 'checkbox') v = e.target.checked;
                if (f === 'weekday') v = parseInt(e.target.value, 10);
                p[f] = v;
                if (f === 'fund') {
                    const nf = fundsData[v];
                    if (nf) { p.startDate = nf.minDate; p.endDate = nf.maxDate; }
                    renderPlanList();
                } else if (f === 'type') {
                    if (v === 'single') p.endDate = p.startDate;
                    else { const nf = fundsData[p.fund]; if (nf && p.endDate === p.startDate) p.endDate = nf.maxDate; }
                    renderPlanList();
                } else if (f === 'startDate' && p.type === 'single') {
                    p.endDate = v;
                    renderPlanList();
                } else if (f === 'stopGain') {
                    renderPlanList();
                }
                checkNavGaps();
            });
        });
        const del = card.querySelector('[data-act="del"]');
        if (del) del.addEventListener('click', () => deletePlan(p.id));
    });
}

// 核心回测（保持不变）
function runBacktest() {
    if (investmentPlans.length === 0) { alert('请先添加投资计划！'); return; }
    const fundShares = {}; Object.keys(fundsData).forEach(code => fundShares[code] = 0);
    let totalCash = 0;       // 总现金（真实分红 + 止盈赎回到账）
    let totalCashDiv = 0;    // 纯现金分红累计（不含止盈赎回）
    const dailyAsset = [], dailyDates = [], dailyInvest = [], dailyCashDiv = [], dailyTotalCash = [];
    const cashFlows = [], flowDates = [];

    const allDatesSet = new Set();
    Object.values(fundsData).forEach(f => f.dates.forEach(d => allDatesSet.add(formatDate(d))));
    // 并入基准指数连续交易日：使基金相对基准缺失的交易日进入统一日历，由下方前向填充补齐
    if (benchmarkDateStrs.length > 0) benchmarkDateStrs.forEach(d => allDatesSet.add(d));
    const allDateStrs = Array.from(allDatesSet).sort();
    const allDates = allDateStrs.map(str => new Date(str + 'T00:00:00'));
    const earliestInvestDate = new Date(Math.min(...investmentPlans.map(p => new Date(p.startDate).getTime())));

    // 为每个基金构建前向填充净值表：参照并集日期，成立后缺失日用最近一次已知净值填充
    const fundNavMap = {};
    const simNav = {};        // 与 allDateStrs 对齐的前向填充净值数组（未成立为 null）
    const simDiv = {};        // 与 allDateStrs 对齐的每份额分红数组（无分红为 0）
    const simDow = new Array(allDateStrs.length);   // 每日期的星期（0=周日），避免热循环中重复构造 Date
    for (let k = 0; k < allDateStrs.length; k++) simDow[k] = new Date(allDateStrs[k] + 'T00:00:00').getUTCDay();
    // 预缓存时间戳与"日"数值，供定投模拟热循环直接使用（消除内层 new Date 与字符串 split）
    const simDateTs = new Array(allDateStrs.length);
    const simDayOfMonth = new Array(allDateStrs.length);
    for (let k = 0; k < allDateStrs.length; k++) { simDateTs[k] = allDates[k].getTime(); simDayOfMonth[k] = parseInt(allDateStrs[k].split('-')[2], 10); }
    Object.keys(fundsData).forEach(code => {
        const f = fundsData[code];
        const realNav = new Map(f.dates.map((d, i) => [formatDate(d), f.nav[i]]));
        const realDiv = new Map(f.dates.map((d, i) => [formatDate(d), f.div[i]]));
        const m = new Map();
        const navArr = new Array(allDateStrs.length).fill(null);
        const divArr = new Array(allDateStrs.length).fill(0);
        let lastNav = null;            // 基金尚未成立时为 null，不填充
        for (let k = 0; k < allDateStrs.length; k++) {
            const ds = allDateStrs[k];
            if (realNav.has(ds)) lastNav = realNav.get(ds);
            if (lastNav !== null) { m.set(ds, lastNav); navArr[k] = lastNav; }
            if (realDiv.has(ds)) divArr[k] = realDiv.get(ds);
        }
        fundNavMap[code] = m;
        simNav[code] = navArr;
        simDiv[code] = divArr;
    });
    // 预计算 monthly 'first'（每月首个交易日）计划的每月首个交易日集合
    const firstTradingDaySet = new Set();
    for (const plan of investmentPlans) {
        if (plan.type === 'monthly' && plan.dayOfMonth === 'first') {
            const f = fundsData[plan.fund];
            if (!f || f.dates.length === 0) continue;
            const minT = new Date(plan.startDate + 'T00:00:00').getTime();
            const maxT = new Date(plan.endDate + 'T00:00:00').getTime();
            const byMonth = {};
            for (const d of f.dates) {
                const ds = formatDate(d);
                const t = new Date(ds + 'T00:00:00').getTime();
                if (t < minT || t > maxT) continue;
                const m = ds.slice(0, 7);
                if (!byMonth[m] || ds < byMonth[m]) byMonth[m] = ds;
            }
            Object.values(byMonth).forEach(ds => firstTradingDaySet.add(plan.id + '|' + ds));
        }
    }
    const doFill = fillMissingNav;

    // 每基金止盈/本金状态（按单基金独立触发目标止盈）
    const fundRunPrincipal = {}; Object.keys(fundsData).forEach(code => fundRunPrincipal[code] = 0);
    const fundRunMaxPrincipal = {}; Object.keys(fundsData).forEach(code => fundRunMaxPrincipal[code] = 0);
    const fundCostBasis = {}; Object.keys(fundsData).forEach(code => fundCostBasis[code] = 0);
    const fundMaxPrincipal = {}; Object.keys(fundsData).forEach(code => fundMaxPrincipal[code] = 0);
    const fundRedeemed = {}; Object.keys(fundsData).forEach(code => fundRedeemed[code] = 0);
    const fundStopGainEvents = {}; Object.keys(fundsData).forEach(code => fundStopGainEvents[code] = []);

    // 预计算 biweekly 计划的每双周投资日期集合（每两个该 weekday 交易日投一次）
    const biweeklySet = new Set();
    for (const plan of investmentPlans) {
        if (plan.type === 'biweekly') {
            const f = fundsData[plan.fund];
            if (!f || f.dates.length === 0) continue;
            const minT = new Date(plan.startDate + 'T00:00:00').getTime();
            const maxT = new Date(plan.endDate + 'T00:00:00').getTime();
            const wd = plan.weekday != null ? plan.weekday : 1;
            let cnt = 0;
            for (const d of f.dates) {
                const ds = formatDate(d);
                const t = new Date(ds + 'T00:00:00').getTime();
                if (t < minT || t > maxT) continue;
                if (new Date(ds + 'T00:00:00').getUTCDay() === wd) {
                    if (cnt % 2 === 0) biweeklySet.add(plan.id + '|' + ds);
                    cnt++;
                }
            }
        }
    }

    for (let dtIdx = 0; dtIdx < allDates.length; dtIdx++) {
        const currentDt = allDates[dtIdx];
        const dateStr = allDateStrs[dtIdx];
        let dailyInv = 0;
        for (const plan of investmentPlans) {
            const fund = plan.fund;
            const fundData = fundsData[fund];
            const fundDateIdx = fundData.dates.findIndex(d => formatDate(d) === dateStr);
            if (fundDateIdx === -1) continue;
            const divPerShare = fundData.div[fundDateIdx];
            const nav = fundData.nav[fundDateIdx];
            if (fundShares[fund] > 0 && divPerShare > 0) {
                const totalDiv = fundShares[fund] * divPerShare;
                if (plan.div === 'reinvest') fundShares[fund] += totalDiv / nav;
                else { totalCash += totalDiv; totalCashDiv += totalDiv; cashFlows.push(totalDiv); flowDates.push(new Date(currentDt)); }
            }
        }
        // 目标止盈检查（按单基金，在投资前、以截至昨日的本轮峰值本金为分母）
        for (const plan of investmentPlans) {
            if (!plan.stopGain) continue;
            const fund = plan.fund;
            if (fundShares[fund] <= 0) continue;
            const fundData = fundsData[fund];
            let nav;
            if (doFill) { nav = fundNavMap[fund].get(dateStr); if (nav === undefined) continue; }
            else { const idx = fundData.dates.findIndex(d => formatDate(d) === dateStr); if (idx === -1) continue; nav = fundData.nav[idx]; }
            const th = (parseFloat(plan.stopGainPct) || 0) / 100;
            const sellRatio = Math.min(1, Math.max(0, (parseFloat(plan.stopGainSellRatio) || 0) / 100));
            const roundPrincipal = fundRunMaxPrincipal[fund];
            if (roundPrincipal > 0 && sellRatio > 0) {
                const holdMv = fundShares[fund] * nav;
                if ((holdMv - fundCostBasis[fund]) / roundPrincipal >= th) {
                    const sellShares = fundShares[fund] * sellRatio;
                    const proceeds = sellShares * nav;
                    fundShares[fund] -= sellShares;
                    totalCash += proceeds;
                    cashFlows.push(proceeds); flowDates.push(new Date(currentDt));
                    fundCostBasis[fund] *= (1 - sellRatio);
                    fundRedeemed[fund] += proceeds;
                    fundStopGainEvents[fund].push({ dateStr, proceeds, ratio: sellRatio, nav });
                    if (sellRatio >= 1 || fundShares[fund] < 1e-9) {
                        fundRunPrincipal[fund] = 0;
                        fundRunMaxPrincipal[fund] = 0;
                    }
                }
            }
        }
        for (const plan of investmentPlans) {
            const fund = plan.fund;
            const fundData = fundsData[fund];
            let nav;
            if (doFill) {
                nav = fundNavMap[fund].get(dateStr);
                if (nav === undefined) continue;        // 基金尚未成立
            } else {
                const fundDateIdx = fundData.dates.findIndex(d => formatDate(d) === dateStr);
                if (fundDateIdx === -1) continue;
                nav = fundData.nav[fundDateIdx];
            }
            const amt = plan.amount;
            const currentDateStr = dateStr;
            let shouldInvest = false;
            if (plan.type === 'single') { if (currentDateStr === plan.startDate) shouldInvest = true; }
            else if (plan.type === 'weekly') {
                if (currentDateStr >= plan.startDate && currentDateStr <= plan.endDate) {
                    const d = new Date(currentDateStr + 'T00:00:00');
                    if (d.getUTCDay() === (plan.weekday != null ? plan.weekday : 1)) shouldInvest = true;
                }
            } else if (plan.type === 'biweekly') {
                if (currentDateStr >= plan.startDate && currentDateStr <= plan.endDate) {
                    if (biweeklySet.has(plan.id + '|' + currentDateStr)) shouldInvest = true;
                }
            } else if (plan.type === 'monthly') {
                if (currentDateStr >= plan.startDate && currentDateStr <= plan.endDate) {
                    if (plan.dayOfMonth === 'first') {
                        if (firstTradingDaySet.has(plan.id + '|' + currentDateStr)) shouldInvest = true;
                    } else {
                        const dom = plan.dayOfMonth != null ? parseInt(plan.dayOfMonth, 10) : 1;
                        if (parseInt(currentDateStr.split('-')[2]) === dom) shouldInvest = true;
                    }
                }
            }
            if (shouldInvest) {
                fundShares[fund] += amt / nav;
                fundRunPrincipal[fund] += amt;
                fundCostBasis[fund] += amt;
                fundRunMaxPrincipal[fund] = Math.max(fundRunMaxPrincipal[fund], fundRunPrincipal[fund]);
                fundMaxPrincipal[fund] = Math.max(fundMaxPrincipal[fund], fundRunPrincipal[fund]);
                dailyInv += amt;
                cashFlows.push(-amt);
                flowDates.push(new Date(currentDt));
            }
        }
        let mv = 0;
        for (const code of Object.keys(fundShares)) {
            const fundData = fundsData[code];
            let nav;
            if (doFill) nav = fundNavMap[code].get(dateStr);
            else { const idx = fundData.dates.findIndex(d => formatDate(d) === dateStr); if (idx !== -1) nav = fundData.nav[idx]; }
            if (nav !== undefined) mv += fundShares[code] * nav;
        }
        dailyAsset.push(mv + totalCash);
        dailyDates.push(new Date(currentDt));
        dailyInvest.push(dailyInv);
        dailyCashDiv.push(totalCashDiv);
        dailyTotalCash.push(totalCash);
    }

    let marketValue = 0;
    for (const code of Object.keys(fundShares)) {
        const fundData = fundsData[code];
        marketValue += fundShares[code] * fundData.nav[fundData.nav.length-1];
    }
    const cashDiv = totalCashDiv;
    const totalAsset = marketValue + totalCash;
    cashFlows.push(marketValue);
    flowDates.push(dailyDates[dailyDates.length-1]);
    const totalInvest = -cashFlows.reduce((s, v) => s + (v < 0 ? v : 0), 0);
    const totalReturn = totalInvest > 0 ? (totalAsset / totalInvest - 1) * 100 : 0;
    // 止盈聚合（跨基金）
    const stopGainByFund = {};
    const stopGainEvents = [];
    let totalMaxPrincipal = 0, totalRedeemedAll = 0;
    Object.keys(fundMaxPrincipal).forEach(code => {
        totalMaxPrincipal += fundMaxPrincipal[code];
        totalRedeemedAll += fundRedeemed[code];
        if (fundStopGainEvents[code].length) {
            stopGainByFund[code] = { events: fundStopGainEvents[code], totalRedeemed: fundRedeemed[code] };
            fundStopGainEvents[code].forEach(e => stopGainEvents.push({ fund: code, dateStr: e.dateStr, proceeds: e.proceeds, ratio: e.ratio, nav: e.nav }));
        }
    });
    const hasStopGainPlan = investmentPlans.some(p => p.stopGain);
    const netProfit = totalAsset - totalInvest;
    const maxPrincipalReturn = totalMaxPrincipal > 0 ? netProfit / totalMaxPrincipal * 100 : 0;
    const combined = flowDates.map((d, i) => ({ date: d, cf: cashFlows[i] }));
    combined.sort((a, b) => a.date - b.date);
    const xirrVal = xirr(combined.map(c=>c.cf), combined.map(c=>c.date)) * 100;

    // 净值序列起点：最早的计划起始日（earliestInvestDate）。TWR 公式在昨日资产为 0 时自动置 1.0，前导空仓不会压平曲线
    const startIdx = dailyDates.findIndex(d => d >= earliestInvestDate);
    const validDates = dailyDates.slice(startIdx);
    const validAssets = dailyAsset.slice(startIdx);
    const validInvest = dailyInvest.slice(startIdx);
    const validCashDivs = dailyCashDiv.slice(startIdx);
    const validTotalCash = dailyTotalCash.slice(startIdx);

    const _m = computeMetrics(validDates, validAssets, validInvest);
    let annualVolatility = _m.annualVolatility, sharpeRatio = _m.sharpeRatio, calmarRatio = _m.calmarRatio, maxDrawdown = _m.maxDrawdown;
    let annualReturnPct = _m.annualReturnPct, winRate = _m.winRate, maxDDDuration = _m.maxDDDuration, netValues = _m.netValues;
    backtestResult = { dates: validDates, assets: validAssets, netValues, invests: validInvest, cashDivs: validCashDivs, totalCashSeries: validTotalCash,
        simDateStrs: allDateStrs, simNav: simNav, simDiv: simDiv, simDow: simDow, simDateTs: simDateTs, simDayOfMonth: simDayOfMonth, simStartIdx: startIdx,
        stopGainByFund, stopGainEvents, totalMaxPrincipal, totalRedeemedAll, maxPrincipalReturn, hasStopGainPlan };

    const twrHtml = isNaN(annualReturnPct) ? '-' : annualReturnPct.toFixed(2) + '%';
    const winHtml = isNaN(winRate) ? '-' : winRate.toFixed(1) + '%';
    let riskHtml = validDates.length >= MIN_TRADE_DAYS ? `
        <div class="bg-rose-50 p-4 rounded-lg text-center" data-mkey="最大回撤"><div class="text-sm text-slate-500">最大回撤</div><div class="text-2xl font-bold text-rose-600">${maxDrawdown.toFixed(2)}%</div></div>
        <div class="bg-rose-50 p-4 rounded-lg text-center" data-mkey="回撤持续天数"><div class="text-sm text-slate-500">回撤持续天数</div><div class="text-2xl font-bold text-rose-600">${maxDDDuration.toFixed(0)} 天</div></div>
        <div class="bg-rose-50 p-4 rounded-lg text-center" data-mkey="年化波动率"><div class="text-sm text-slate-500">年化波动率</div><div class="text-2xl font-bold text-rose-600">${(annualVolatility*100).toFixed(2)}%</div></div>
        <div class="bg-rose-50 p-4 rounded-lg text-center" data-mkey="夏普/卡玛"><div class="text-sm text-slate-500">夏普/卡玛</div><div class="text-2xl font-bold text-rose-600">${sharpeRatio.toFixed(2)}/${calmarRatio.toFixed(2)}</div></div>
    ` : `<div class="bg-gray-100 p-4 rounded-lg text-center col-span-4" data-mkey="风险指标"><div class="text-sm text-gray-600">风险指标</div><div class="text-xl font-medium text-gray-500">投资时间不足${MIN_TRADE_DAYS}个交易日，以下指标暂不可用</div></div>`;

    const peakPrincipalVal = hasStopGainPlan ? totalMaxPrincipal : totalInvest;
    const peakReturnVal = hasStopGainPlan ? maxPrincipalReturn : totalReturn;
    document.getElementById('metrics').innerHTML = `
        <div class="bg-blue-50 p-4 rounded-lg text-center" data-mkey="总投入本金"><div class="text-sm text-slate-500">总投入本金</div><div class="text-2xl font-bold text-blue-700">${totalInvest.toFixed(2)} 元</div></div>
        <div class="bg-blue-50 p-4 rounded-lg text-center" data-mkey="持仓市值"><div class="text-sm text-slate-500">持仓市值</div><div class="text-2xl font-bold text-blue-700">${marketValue.toFixed(2)} 元</div></div>
        <div class="bg-blue-50 p-4 rounded-lg text-center" data-mkey="累计现金分红"><div class="text-sm text-slate-500">累计现金分红</div><div class="text-2xl font-bold text-blue-700">${cashDiv.toFixed(2)} 元</div></div>
        <div class="bg-blue-50 p-4 rounded-lg text-center" data-mkey="总资产"><div class="text-sm text-slate-500">总资产</div><div class="text-2xl font-bold text-blue-700">${totalAsset.toFixed(2)} 元</div></div>
        <div class="bg-amber-50 p-4 rounded-lg text-center" data-mkey="峰值本金"><div class="text-sm text-slate-500">峰值本金</div><div class="text-2xl font-bold text-amber-700">${peakPrincipalVal.toFixed(2)} 元</div></div>

        <div class="bg-emerald-50 p-4 rounded-lg text-center" data-mkey="累计收益率"><div class="text-sm text-slate-500">累计收益率</div><div class="text-2xl font-bold text-emerald-800">${totalReturn.toFixed(2)}%</div></div>
        <div class="bg-emerald-50 p-4 rounded-lg text-center" data-mkey="XIRR年化"><div class="text-sm text-slate-500">XIRR年化</div><div class="text-2xl font-bold text-emerald-800">${isNaN(xirrVal)?'-':xirrVal.toFixed(2)+'%'}</div></div>
        <div class="bg-emerald-50 p-4 rounded-lg text-center" data-mkey="年化收益率(时间加权)"><div class="text-sm text-slate-500">年化收益率(时间加权)</div><div class="text-2xl font-bold text-emerald-800">${twrHtml}</div></div>
        <div class="bg-emerald-50 p-4 rounded-lg text-center" data-mkey="胜率(正收益日占比)"><div class="text-sm text-slate-500">胜率(正收益日占比)</div><div class="text-2xl font-bold text-emerald-800">${winHtml}</div></div>
        <div class="bg-amber-50 p-4 rounded-lg text-center cursor-help" data-mkey="赎回金额"><div class="text-sm text-slate-500">赎回金额</div><div class="text-2xl font-bold text-amber-700">${totalRedeemedAll.toFixed(2)} 元</div></div>

        ${riskHtml}
        <div class="bg-amber-50 p-4 rounded-lg text-center cursor-help" data-mkey="峰值本金收益率"><div class="text-sm text-slate-500">峰值本金收益率</div><div class="text-2xl font-bold text-amber-700">${peakReturnVal.toFixed(2)}%</div></div>
    `;
    // 止盈浮窗：赎回金额卡显示「止盈明细」；峰值本金收益率卡显示「止盈次数 + 间隔统计」
    const tipEl = document.getElementById('stopGainTip');
    const bindStopGainTip = (mkey, builder) => {
        const card = document.querySelector('#metrics [data-mkey="' + mkey + '"]');
        if (!card) return;
        let tipTimer = null;
        const showTip = () => { clearTimeout(tipTimer); tipEl.innerHTML = builder(); tipEl.classList.remove('hidden'); };
        const hideTip = () => { tipTimer = setTimeout(() => tipEl.classList.add('hidden'), 150); };
        card.addEventListener('mouseenter', showTip);
        card.addEventListener('mouseleave', hideTip);
        tipEl.addEventListener('mouseenter', () => clearTimeout(tipTimer));
        tipEl.addEventListener('mouseleave', hideTip);
    };
    bindStopGainTip('赎回金额', buildStopGainTip);
    bindStopGainTip('峰值本金收益率', buildStopGainSummaryTip);

    if (validDates.length > 0) {
        document.getElementById('chartFilter').style.display = 'block';
        document.getElementById('benchmarkSelectorWrapper').style.display = 'flex';
        const minD = formatDate(validDates[0]), maxD = formatDate(validDates[validDates.length-1]);
        document.getElementById('chartStartDate').value = minD; document.getElementById('chartStartDate').min = minD; document.getElementById('chartStartDate').max = maxD;
        document.getElementById('chartEndDate').value = maxD; document.getElementById('chartEndDate').min = minD; document.getElementById('chartEndDate').max = maxD;
        document.getElementById('chartStartDate').onchange = updateCharts;
        document.getElementById('chartEndDate').onchange = updateCharts;
    } else {
        document.getElementById('benchmarkSelectorWrapper').style.display = 'none';
    }
    updateCharts();
    renderAnalysisTable(); // 确保调用的是异步函数
    renderProfitProbability();
    renderCorrelationMatrix();
}

// 构建止盈浮窗内容（按单基金列出止盈触发明细）
function buildStopGainTip() {
    if (!backtestResult.hasStopGainPlan) return '<div class="text-amber-700 font-medium">无基金启用目标止盈</div>';
    const byFund = backtestResult.stopGainByFund || {};
    const codes = Object.keys(byFund);
    if (codes.length === 0) return '<div class="text-amber-700 font-medium">已启用目标止盈，但回测区间内未触发。</div>';
    let html = '<div class="font-semibold text-amber-800 mb-2">止盈触发明细（按单基金）</div>';
    for (const code of codes) {
        const cn = fundCodeName(code);
        const info = byFund[code];
        html += '<div class="mb-3 pb-2 border-b border-amber-100 last:border-0">';
        html += '<div class="font-medium text-gray-800">' + cn.code + ' ' + (cn.name || '') + '</div>';
        html += '<div class="text-xs text-gray-500">触发 ' + info.events.length + ' 次 · 累计赎回 ' + info.totalRedeemed.toFixed(2) + ' 元</div>';
        info.events.forEach(function(e) {
            html += '<div class="text-xs text-gray-600 mt-1">· ' + e.dateStr + ' 赎回 ' + e.proceeds.toFixed(2) + ' 元（' + (e.ratio*100).toFixed(0) + '%，净值 ' + e.nav.toFixed(4) + '）</div>';
        });
        html += '</div>';
    }
    return html;
}

// 构建峰值本金收益率浮窗：止盈次数 + 间隔统计（按单基金）
function buildStopGainSummaryTip() {
    if (!backtestResult.hasStopGainPlan) return '<div class="text-amber-700 font-medium">无基金启用目标止盈</div>';
    const byFund = backtestResult.stopGainByFund || {};
    const codes = Object.keys(byFund);
    if (codes.length === 0) return '<div class="text-amber-700 font-medium">已启用目标止盈，但回测区间内未触发。</div>';
    let totalCount = 0;
    let html = '<div class="font-semibold text-amber-800 mb-2">止盈次数与间隔统计（按单基金）</div>';
    for (const code of codes) {
        const cn = fundCodeName(code);
        const info = byFund[code];
        const evs = info.events || [];
        const n = evs.length;
        totalCount += n;
        html += '<div class="mb-3 pb-2 border-b border-amber-100 last:border-0">';
        html += '<div class="font-medium text-gray-800">' + cn.code + ' ' + (cn.name || '') + '</div>';
        html += '<div class="text-xs text-gray-500">止盈次数：' + n + ' 次</div>';
        if (n >= 2) {
            const intervals = [];
            for (let i = 1; i < n; i++) {
                const t0 = new Date(evs[i - 1].dateStr + 'T00:00:00').getTime();
                const t1 = new Date(evs[i].dateStr + 'T00:00:00').getTime();
                intervals.push(Math.round((t1 - t0) / 86400000));
            }
            const min = Math.min.apply(null, intervals);
            const max = Math.max.apply(null, intervals);
            const avg = Math.round(intervals.reduce((a, b) => a + b, 0) / intervals.length);
            html += '<div class="text-xs text-gray-500">间隔统计：最短 ' + min + ' 天 · 最长 ' + max + ' 天 · 平均 ' + avg + ' 天</div>';
        } else if (n === 1) {
            html += '<div class="text-xs text-gray-500">间隔统计：仅触发 1 次，无间隔</div>';
        }
        html += '</div>';
    }
    html += '<div class="text-xs text-amber-700 font-medium">合计止盈次数：' + totalCount + ' 次</div>';
    return html;
}

