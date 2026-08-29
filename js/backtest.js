/* backtest.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// 修正 Dietz 法（Modified Dietz）——XIRR 的一阶线性近似，纯除法无需迭代，必然可得结果。
// 仅在 XIRR 迭代失败（无解/多解/发散）时作为降级兜底使用，返回年化收益率（小数，非百分比）。
// 口径：期初价值为 0（本回测所有投入均以现金流形式给出），故
//   收益 = Σcf 的相反数中的净盈亏；加权本金 = Σ(流出额 × 该笔资金在期内的剩余存续占比)。
// 返回 NaN 表示连近似也无法计算（如加权本金为 0 或期间为 0）。
function modifiedDietz(cashFlows, dates) {
    if (!cashFlows || !dates || cashFlows.length !== dates.length || cashFlows.length < 2) return NaN;
    const paired = cashFlows.map((cf, i) => ({ cf, date: dates[i] }));
    paired.sort((a, b) => a.date - b.date);
    const t0 = paired[0].date, tN = paired[paired.length - 1].date;
    const totalDays = (tN - t0) / 86400000;
    if (!(totalDays > 0)) return NaN;

    // 净盈亏 = 全部现金流代数和（流入为正、流出为负；期末市值已作为正流包含在内）
    let net = 0, weighted = 0;
    for (const p of paired) {
        net += p.cf;
        if (p.cf < 0) {
            // 该笔投入在期内的剩余存续时间占比
            const w = 1 - ((p.date - t0) / 86400000) / totalDays;
            weighted += (-p.cf) * w;
        }
    }
    if (!(weighted > 0)) return NaN;

    const periodReturn = net / weighted;          // 区间收益率
    const years = totalDays / 365;
    if (!(years > 0)) return NaN;
    // 年化（几何折算）；区间亏损超过 100% 时无法开方，退回线性年化避免 NaN
    const base = 1 + periodReturn;
    const annual = base > 0 ? Math.pow(base, 1 / years) - 1 : periodReturn / years;
    return isFinite(annual) ? annual : NaN;
}

// 记录最近一次 xirr() 调用是否走了降级近似（供 UI 标注「*近似」）。
// 每次 xirr() 入口重置，调用方在拿到结果后立即读取即可。
let xirrLastApprox = false;
function xirrFallback(cashFlows, dates) {
    const approx = modifiedDietz(cashFlows, dates);
    xirrLastApprox = isFinite(approx);
    return approx;
}

// XIRR
function xirr(cashFlows, dates, guess = 0.1) {
    xirrLastApprox = false;
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
        if (Math.abs(v) < tolerance) return isFinite(rate) ? rate : xirrFallback(cashFlows, dates);
        if (Math.abs(d) < tolerance) break;
        rate -= v / d;
        if (!isFinite(rate)) break;               // 迭代发散（NaN/Infinity）→ 转二分法
    }
    // 牛顿法失败（止盈等非传统现金流存在多次变号）→ 二分法在 [-0.9999, 100] 内求根
    let lo = -0.9999, hi = 100;
    let fLo = npv(lo), fHi = npv(hi);
    // 区间两端同号 → 方程在该区间内无根（无解或根在区间外）→ 降级为修正 Dietz 近似
    if (fLo * fHi > 0) return xirrFallback(cashFlows, dates);
    for (let i = 0; i < 200; i++) {
        const mid = (lo + hi) / 2;
        const fMid = npv(mid);
        if (Math.abs(fMid) < tolerance) return mid;
        if (fLo * fMid < 0) { hi = mid; fHi = fMid; }
        else { lo = mid; fLo = fMid; }
    }
    // 200 次二分仍未达容差：区间已极窄，取中点即可；若异常非有限则降级近似
    const mid = (lo + hi) / 2;
    return isFinite(mid) ? mid : xirrFallback(cashFlows, dates);
}

// 通用指标计算：由对齐后的日期/资产/每日投入序列计算净值曲线与风险收益指标
// 与组合回测口径完全一致（时间加权净值 TWR），供组合回测与策略比较共用
// 净值只反映市场涨跌：每日增长率 = (今日总资产 − 今日新增投入) / 昨日总资产，起点恒为 1.0
function computeMetrics(validDates, validAssets, validInvest) {
    const n = validAssets.length;
    let netValues = [];
    let annualVolatility = NaN, sharpeRatio = NaN, calmarRatio = NaN, maxDrawdown = 0;
    let annualReturnPct = NaN, winRate = NaN, maxDDDuration = NaN;
    let worstPeakIdx = 0, worstTroughIdx = 0;

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
        let peak = netValues[0], peakIdx = 0;
        for (let i = 0; i < netValues.length; i++) {
            if (netValues[i] > peak) { peak = netValues[i]; peakIdx = i; }
            const dd = (netValues[i] - peak) / peak;
            if (dd < maxDrawdown) { maxDrawdown = dd; worstPeakIdx = peakIdx; worstTroughIdx = i; }
        }
        maxDrawdown *= 100;
        if (annualVolatility > 0) sharpeRatio = (annualReturn - RISK_FREE_RATE) / annualVolatility;
        if (maxDrawdown !== 0) calmarRatio = annualReturn / Math.abs(maxDrawdown/100);
        annualReturnPct = annualReturn * 100;
        winRate = dailyReturns.length ? dailyReturns.filter(r => r > 0).length / dailyReturns.length * 100 : NaN;
        // 回撤持续天数 = 最大回撤区间（峰值→谷值）经历的自然日，与后续是否创新高无关
        maxDDDuration = (validDates[worstTroughIdx] - validDates[worstPeakIdx]) / 86400000;
    }
    return { netValues, annualVolatility, sharpeRatio, calmarRatio, maxDrawdown, annualReturnPct, winRate, maxDDDuration,
        maxDDPeak: maxDrawdown < 0 ? formatDate(validDates[worstPeakIdx]) : '',
        maxDDTrough: maxDrawdown < 0 ? formatDate(validDates[worstTroughIdx]) : '',
        ddDurPeak: maxDDDuration > 0 ? formatDate(validDates[worstPeakIdx]) : '',
        ddDurTrough: maxDDDuration > 0 ? formatDate(validDates[worstTroughIdx]) : '' };
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
    // resultSection 在点击“启动回测”前保持隐藏，运行成功后才显示
    const planEl = document.getElementById('planListSection'); if (planEl) planEl.style.display = has ? 'block' : 'none';
    const hint = document.getElementById('fundStorageHint');
    if (hint) hint.textContent = has ? ('本地已存 ' + Object.keys(fundsData).length + ' 只') : '本地无数据';
    updateDataMgmtCollapse();
}
// 数据管理折叠状态：基金与基准都有内容时默认折叠，缺一种时默认展开
async function updateDataMgmtCollapse() {
    const btn = document.getElementById('toggleDataMgmtBtn');
    const collapsible = document.getElementById('dataMgmtCollapsible');
    if (!btn || !collapsible) return;
    if (btn.dataset.userToggled === '1') return; // 用户手动切换过则不再自动覆盖
    let benchCount = 0;
    try { benchCount = await db.benchmarks.count(); } catch (e) { console.error('查询基准数量失败', e); }
    const hasFund = Object.keys(fundsData).length > 0;
    const hasBench = benchCount > 0;
    const collapsed = hasFund && hasBench;
    collapsible.classList.toggle('hidden', collapsed);
    btn.textContent = collapsed ? '▼ 展开' : '▲ 折叠';
}
// 用户手动切换数据管理折叠
document.getElementById('toggleDataMgmtBtn')?.addEventListener('click', () => {
    const btn = document.getElementById('toggleDataMgmtBtn');
    const collapsible = document.getElementById('dataMgmtCollapsible');
    if (!btn || !collapsible) return;
    btn.dataset.userToggled = '1';
    const collapsed = collapsible.classList.toggle('hidden');
    btn.textContent = collapsed ? '▼ 展开' : '▲ 折叠';
});
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
                    if (date && !isNaN(date.getTime())) {
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
        mdDays: 120,
        mdPct: 10,
        mdBench: 'self',       // 最大回撤回撤基准：self=基金自身净值（默认） / portfolio=组合净值 / bench:<id>=导入基准
        mdContinuous: false,   // 最大回撤连续投资：开启后窗口持续计算，满足回撤阈值即每交易日投一笔，不满足即停
        stopGain: false,
        stopGainPct: 8,
        stopGainSellRatio: 100,
        activeRedeems: []   // 主动赎回事件：[{ id, date, mode:'ratio'|'amount', value }]；mode=ratio 按持仓比例%，amount 按金额(元)
    };
    // 同一基金允许多条计划并存（如不同策略/金额），直接新增，不做覆盖
    investmentPlans.push(plan);
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
    // 发现净值不完整时自动勾选「填充空白净值」，确保缺失日默认按前一交易日净值模拟填充
    fillMissingNav = true;
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

// 回撤基准下拉选项 HTML（复用 benchmarks.js 的 getBenchmarkOptions）
function mdBenchOptions(selected) {
    const sel = selected || 'self';
    return getBenchmarkOptions().map(o =>
        `<option value="${o.value}" ${String(o.value) === String(sel) ? 'selected' : ''}>${o.label}</option>`
    ).join('');
}

// 主动赎回配置区 HTML（多事件增删）——参考 strategy.js 的 scActiveRedeemHtml
function planActiveRedeemHtml(p) {
    const list = (p.activeRedeems || []).map((r, ri) => `
      <div class="flex items-center gap-2" data-redeem-idx="${ri}">
        <input type="date" data-ar-field="date" data-idx="${ri}" value="${r.date || ''}" class="w-32 min-w-0 p-1.5 border border-gray-300 rounded text-sm">
        <select data-ar-field="mode" data-idx="${ri}" class="p-1.5 border border-gray-300 rounded text-sm">
          <option value="ratio" ${r.mode === 'ratio' ? 'selected' : ''}>按比例%</option>
          <option value="shares" ${r.mode === 'shares' ? 'selected' : ''}>按份额(份)</option>
          <option value="amount" ${r.mode === 'amount' ? 'selected' : ''}>按金额元</option>
        </select>
        <input type="number" data-ar-field="value" data-idx="${ri}" value="${r.value != null ? r.value : ''}" step="0.01" min="0" class="w-24 p-1.5 border border-gray-300 rounded text-sm">
        <button type="button" data-act="delRedeem" data-idx="${ri}" class="text-red-500 hover:text-red-700 text-sm font-medium">✕</button>
      </div>`).join('');
    return `
      <div class="mt-2 border-t border-gray-200 pt-2">
        <div class="flex items-center gap-2 mb-1">
          <span class="text-xs font-medium text-orange-600 whitespace-nowrap">主动赎回</span>
          <button type="button" data-act="addRedeem" class="text-xs text-blue-600 hover:underline">＋ 添加赎回</button>
          <span class="text-[10px] text-gray-400">指定日期赎回持仓比例/份额/金额（到日自动执行）</span>
        </div>
        <div class="flex flex-col gap-1">${list || '<span class="text-[10px] text-gray-400">无主动赎回</span>'}</div>
      </div>`;
}

// 单张计划卡片（内联可编辑，data-field 写回 investmentPlans 对应对象）
function planCardHtml(p) {
    const fundOpts = Object.keys(fundsData).map(k => `<option value="${k}" ${k === p.fund ? 'selected' : ''}>${k}</option>`).join('');
    const typeOpts = `
        <option value="single" ${p.type === 'single' ? 'selected' : ''}>单笔</option>
        <option value="weekly" ${p.type === 'weekly' ? 'selected' : ''}>每周定投</option>
        <option value="biweekly" ${p.type === 'biweekly' ? 'selected' : ''}>每双周定投</option>
        <option value="monthly" ${p.type === 'monthly' ? 'selected' : ''}>每月定投</option>
        <option value="maxDrawdown" ${p.type === 'maxDrawdown' ? 'selected' : ''}>最大回撤投资</option>`;
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
    const showMd = p.type === 'maxDrawdown';
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
      ${showMd ? `
      <div class="grid grid-cols-1 md:grid-cols-6 gap-3 items-start mt-3">
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">回撤窗口天数</label><input type="number" data-field="mdDays" value="${p.mdDays}" min="5" step="1" class="w-full p-2 border rounded-lg text-sm"></div>
        <div class="md:col-span-1"><label class="block text-xs text-gray-600 mb-1">回撤阈值(%)</label><input type="number" data-field="mdPct" value="${p.mdPct}" min="1" step="0.5" class="w-full p-2 border rounded-lg text-sm"></div>
        <div class="md:col-span-2"><label class="block text-xs text-gray-600 mb-1">回撤基准</label><select data-field="mdBench" class="w-full p-2 border rounded-lg text-sm">${mdBenchOptions(p.mdBench)}</select></div>
        <div class="md:col-span-2 flex items-end"><label class="flex items-center gap-2 text-xs font-medium text-gray-700 cursor-pointer h-9"><input type="checkbox" data-field="mdContinuous" ${p.mdContinuous ? 'checked' : ''} class="w-4 h-4"> 连续投资（逢跌每日加仓至回升）</label></div>
        <div class="md:col-span-6 text-[10px] text-gray-400">${p.mdContinuous ? '窗口持续计算：回撤达阈值即每个交易日投一笔，回撤收窄至阈值以下即停止，可反复触发。' : '监测窗口内，从近 N 日最高净值回撤达阈值时投入一笔金额；创阶段新高后重新计算窗口可再触发。'}回撤基准默认用基金自身净值；选「组合净值」按整体组合表现；选导入基准按该指数回撤判定。</div>
      </div>` : ''}
      ${planActiveRedeemHtml(p)}
      <div class="mt-2 text-left"><button data-act="del" class="text-red-500 hover:text-red-700 text-sm font-medium">删除此计划</button></div>
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
                // 日期字段年份边界控制：非法（5 位及以上年份/越界 1900~当前年+2）拒绝写入，恢复原值
                if (e.target.type === 'date') {
                    const clean = sanitizeDateInput(v);
                    if (clean === null) { e.target.value = p[f] || ''; return; }
                    v = clean;
                }
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
                    // 单笔投资：结束日期跟随开始日期，仅同步输入框显示值，
                    // 不重建整个列表（重建会替换 DOM、丢失焦点，导致方向键无法连续调整日期）。
                    p.endDate = v;
                    const endInput = card.querySelector('[data-field="endDate"]');
                    if (endInput) endInput.value = v;
                } else if (f === 'stopGain') {
                    renderPlanList();
                }
                checkNavGaps();
            });
        });
        // 主动赎回事件绑定（data-ar-field 带 idx 写回 activeRedeems 对应项）
        card.querySelectorAll('[data-ar-field]').forEach(el => {
            el.addEventListener('change', e => {
                const idx = parseInt(e.target.dataset.idx, 10);
                const f = e.target.dataset.arField;
                if (!p.activeRedeems) p.activeRedeems = [];
                const ev = p.activeRedeems[idx];
                if (!ev) return;
                let v = e.target.value;
                if (f === 'value') v = parseFloat(v);
                // 赎回日期年份边界控制：非法拒绝写入并恢复原值
                if (f === 'date') {
                    const clean = sanitizeDateInput(v);
                    if (clean === null) { e.target.value = ev.date || ''; return; }
                    v = clean;
                }
                ev[f] = v;
            });
        });
        const addRedeem = card.querySelector('[data-act="addRedeem"]');
        if (addRedeem) addRedeem.addEventListener('click', () => {
            if (!p.activeRedeems) p.activeRedeems = [];
            p.activeRedeems.push({ id: Date.now() + Math.floor(Math.random() * 1000), date: p.startDate || '', mode: 'ratio', value: 10 });
            renderPlanList();
        });
        card.querySelectorAll('[data-act="delRedeem"]').forEach(btn => {
            btn.addEventListener('click', () => {
                const idx = parseInt(btn.dataset.idx, 10);
                if (p.activeRedeems) p.activeRedeems.splice(idx, 1);
                renderPlanList();
            });
        });
        const del = card.querySelector('[data-act="del"]');
        if (del) del.addEventListener('click', () => deletePlan(p.id));
    });
    refreshRebalanceUI();
}

// 再平衡设置显隐：仅当组合含 2 个及以上不同基金的单笔投资(single)计划时展示
function refreshRebalanceUI() {
    const sec = document.getElementById('rebalanceSection');
    if (!sec) return;
    const singles = investmentPlans.filter(p => p.type === 'single');
    const show = investmentPlans.length >= 2 && singles.length === investmentPlans.length
        && new Set(singles.map(p => p.fund)).size >= 2;
    sec.style.display = show ? 'flex' : 'none';
}

// 核心回测（保持不变）
function runBacktest() {
    if (investmentPlans.length === 0) { alert('请先添加投资计划！'); return; }
    const planShares = {}; investmentPlans.forEach(p => planShares[p.id] = 0);
    let totalCash = 0;       // 总现金（真实分红 + 止盈赎回到账）
    let totalCashDiv = 0;    // 纯现金分红累计（不含止盈赎回）
    const dailyAsset = [], dailyDates = [], dailyInvest = [], dailyCashDiv = [], dailyTotalCash = [], dailyHoldAsset = [];
    const cashFlows = [], flowDates = [];
    // 可用最大资金池（全局共享，跨基金）：留空/0/NaN = 不限制
    const _comboPoolEl = document.getElementById('comboPool');
    const _comboRaw = _comboPoolEl ? parseFloat(_comboPoolEl.value) : NaN;
    const comboPoolCap = (isFinite(_comboRaw) && _comboRaw > 0) ? _comboRaw : Infinity;

    const allDatesSet = new Set();
    Object.values(fundsData).forEach(f => f.dates.forEach(d => allDatesSet.add(formatDate(d))));
    // 并入基准指数连续交易日：使基金相对基准缺失的交易日进入统一日历，由下方前向填充补齐
    if (benchmarkDateStrs.length > 0) benchmarkDateStrs.forEach(d => allDatesSet.add(d));
    const allDateStrs = Array.from(allDatesSet).sort();
    const allDates = allDateStrs.map(str => new Date(str + 'T00:00:00'));
    // earliestInvestDate 必须按"本地零点"解析，与 allDates/dailyDates（均为
    // `new Date(str + 'T00:00:00')` 本地零点）同一基准。若用 new Date(p.startDate)
    // 按 UTC 零点解析，会与本地零点相差 8 小时，使下方 startIdx 用 epoch 比较时
    // 跳过首笔定投日当天，把首笔投入从 invests 切掉——累计净收益会漏算首笔本金。
    const earliestInvestDate = new Date(Math.min(...investmentPlans.map(p => new Date(p.startDate + 'T00:00:00').getTime())));

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

    // 每计划止盈/本金状态（按 plan 独立追踪，同一基金的多计划互不影响）
    const planRunPrincipal = {}; investmentPlans.forEach(p => planRunPrincipal[p.id] = 0);
    const planRunMaxPrincipal = {}; investmentPlans.forEach(p => planRunMaxPrincipal[p.id] = 0);
    const planCostBasis = {}; investmentPlans.forEach(p => planCostBasis[p.id] = 0);
    const planMaxPrincipal = {}; investmentPlans.forEach(p => planMaxPrincipal[p.id] = 0);
    const planRedeemed = {}; investmentPlans.forEach(p => planRedeemed[p.id] = 0);
    const planStopGainEvents = {}; investmentPlans.forEach(p => planStopGainEvents[p.id] = []);
    const planActiveRedeemed = {}; investmentPlans.forEach(p => planActiveRedeemed[p.id] = 0);   // 主动赎回到账累计金额
    const planActiveRedeemEvents = {}; investmentPlans.forEach(p => planActiveRedeemEvents[p.id] = []);   // 主动赎回触发事件
    const planMdEvents = {}; investmentPlans.forEach(p => planMdEvents[p.id] = []);   // 最大回撤投资触发加仓事件
    const planInvestEvents = {}; investmentPlans.forEach(p => planInvestEvents[p.id] = []);   // 普通定投/单笔投资触发事件
    let poolBalance = comboPoolCap;   // 共享资金池当前可用余额：买入扣减、赎回回充
    let minPoolBalance = comboPoolCap;   // 运行期最低余额，用于换算峰值占用

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

    // 每计划净值滑动窗口历史（最大回撤投资用，跨日期持久）
    const planNavHist = {};
    // 每计划本次下跌记录的最大回撤比例（连续投资"创最深才买"用，跨日期持久）
    const planMaxDD = {};
    // 组合净值序列（每日末尾的 mv+totalCash），供 maxDrawdown 选「组合净值」基准时使用；
    // 判定点位于当日投资分支（早于当日组合市值计算），故用截至昨日的组合净值入窗。
    const comboNavHist = [];
    // 导入基准净值映射缓存（按基准 id），maxDrawdown 选 bench:<id> 时懒加载一次
    const benchNavMaps = {};

    // ---- 再平衡准备 ----
    // 适用条件：组合含 2 个及以上单笔投资(single)计划，且全部为 single（其他定投方式不参与）
    const rebalanceSection = document.getElementById('rebalanceSection');
    const rebalanceEnable = rebalanceSection && document.getElementById('rebalanceEnable')
        ? document.getElementById('rebalanceEnable').checked : false;
    const rebalanceFreq = rebalanceSection && document.getElementById('rebalanceFreq')
        ? document.getElementById('rebalanceFreq').value : 'month';
    const rebalanceThreshold = rebalanceSection && document.getElementById('rebalanceThreshold')
        ? (parseFloat(document.getElementById('rebalanceThreshold').value) || 0) : 0;
    const singlePlans = investmentPlans.filter(p => p.type === 'single');
    // 去重后的基金数：再平衡需在 ≥2 种不同基金间调仓，全部同一基金时无意义，不触发
    const distinctFunds = new Set(singlePlans.map(p => p.fund)).size;
    const canRebalance = rebalanceEnable && singlePlans.length >= 2 && singlePlans.length === investmentPlans.length && distinctFunds >= 2;
    // 目标权重 = 各计划初始单笔金额占比（不新增字段，动态推导）
    const totalSingle = singlePlans.reduce((s, p) => s + (parseFloat(p.amount) || 0), 0);
    const targetWeight = {};
    singlePlans.forEach(p => {
        targetWeight[p.id] = totalSingle > 0 ? (parseFloat(p.amount) || 0) / totalSingle : 1 / singlePlans.length;
    });
    const planRebalanceEvents = {}; singlePlans.forEach(p => planRebalanceEvents[p.id] = []);
    const rebalanceExecDates = [];     // 实际执行再平衡的日期（供结果摘要）
    let rebalanceTotalTurnover = 0;    // 累计调仓规模（卖出+买入）
    let prevRebalanceMonthKey = '';    // 用于按月份切换判定触发日

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
            if (planShares[plan.id] > 0 && divPerShare > 0) {
                const totalDiv = planShares[plan.id] * divPerShare;
                if (plan.div === 'reinvest') planShares[plan.id] += totalDiv / nav;
                else { totalCash += totalDiv; totalCashDiv += totalDiv; cashFlows.push(totalDiv); flowDates.push(new Date(currentDt)); }
            }
        }
        // 目标止盈检查（按 plan 独立，在投资前、以截至昨日的本轮峰值本金为分母）
        for (const plan of investmentPlans) {
            if (!plan.stopGain) continue;
            const fund = plan.fund;
            if (planShares[plan.id] <= 0) continue;
            const fundData = fundsData[fund];
            let nav;
            if (doFill) { nav = fundNavMap[fund].get(dateStr); if (nav === undefined) continue; }
            else { const idx = fundData.dates.findIndex(d => formatDate(d) === dateStr); if (idx === -1) continue; nav = fundData.nav[idx]; }
            const th = (parseFloat(plan.stopGainPct) || 0) / 100;
            const sellRatio = Math.min(1, Math.max(0, (parseFloat(plan.stopGainSellRatio) || 0) / 100));
            const roundPrincipal = planRunMaxPrincipal[plan.id];
            if (roundPrincipal > 0 && sellRatio > 0) {
                const holdMv = planShares[plan.id] * nav;
                if ((holdMv - planCostBasis[plan.id]) / roundPrincipal >= th) {
                    const sellShares = Math.min(planShares[plan.id], planShares[plan.id] * sellRatio);
                    const proceeds = sellShares * nav;
                    planShares[plan.id] -= sellShares;
                    totalCash += proceeds;
                    cashFlows.push(proceeds); flowDates.push(new Date(currentDt));
                    planCostBasis[plan.id] *= (1 - sellRatio);
                    planRedeemed[plan.id] += proceeds;
                    poolBalance += proceeds;   // 赎回回充资金池，可再投
                    planStopGainEvents[plan.id].push({ dateStr, proceeds, ratio: sellRatio, nav });
                    if (sellRatio >= 1 || planShares[plan.id] < 1e-9) {
                        planRunPrincipal[plan.id] = 0;
                        planRunMaxPrincipal[plan.id] = 0;
                    }
                }
            }
        }
        // 主动赎回（在买入前执行）：命中条目配置的赎回日期时，按比例(持仓%)/份额(份)/金额(元)赎回
        for (const plan of investmentPlans) {
            if (!(plan.activeRedeems && plan.activeRedeems.length)) continue;
            if (planShares[plan.id] <= 0) continue;
            const fundData = fundsData[plan.fund];
            let nav;
            if (doFill) { nav = fundNavMap[plan.fund].get(dateStr); if (nav === undefined) continue; }
            else { const idx = fundData.dates.findIndex(d => formatDate(d) === dateStr); if (idx === -1) continue; nav = fundData.nav[idx]; }
            for (const ev of plan.activeRedeems) {
                if (!ev || ev.date !== dateStr) continue;
                let sellShares;
                if (ev.mode === 'amount') {
                    sellShares = Math.min(planShares[plan.id], (parseFloat(ev.value) || 0) / nav);   // 赎回指定金额对应份额，超持仓全赎
                } else if (ev.mode === 'shares') {
                    sellShares = Math.min(planShares[plan.id], parseFloat(ev.value) || 0);   // 赎回指定份额（份），超持仓全赎
                } else {
                    // 按比例：以「当前持仓份额」为基数（不受之前赎回影响，可直接按比例赎回）
                    const ratio = Math.min(1, Math.max(0, (parseFloat(ev.value) || 0) / 100));
                    sellShares = planShares[plan.id] * ratio;
                }
                if (sellShares <= 0) continue;
                const proceeds = sellShares * nav;
                const holdShares = planShares[plan.id];   // 赎回时点持有份额（回测后回显）
                const beforeShares = planShares[plan.id];
                planShares[plan.id] -= sellShares; totalCash += proceeds; planRedeemed[plan.id] += proceeds; planActiveRedeemed[plan.id] += proceeds;
                cashFlows.push(proceeds); flowDates.push(new Date(currentDt));
                poolBalance += proceeds;   // 主动赎回到账回充资金池，可再投
                if (beforeShares > 0) planCostBasis[plan.id] *= planShares[plan.id] / beforeShares;   // 按份额比例调减成本
                planActiveRedeemEvents[plan.id].push({ dateStr, proceeds, nav, mode: ev.mode, holdShares });
            }
        }
        // ---- 再平衡（组合内部调仓）：仅 canRebalance 且到达触发日时执行 ----
        // 卖出超配回充现金 totalCash，再买入低配（受 totalCash 约束），不触碰外部 poolBalance，
        // 也不写入外部 cashFlows（内部资金转移，XIRR 期末市值自动反映）。
        if (canRebalance) {
            const mKey = dateStr.slice(0, 7);          // 'yyyy-mm'
            let isTrigger = false;
            if (mKey !== prevRebalanceMonthKey) {
                const mon = parseInt(dateStr.slice(5, 7), 10); // 1-12
                if (rebalanceFreq === 'quarter') isTrigger = (mon - 1) % 3 === 0;
                else if (rebalanceFreq === 'half') isTrigger = (mon - 1) % 6 === 0;
                else if (rebalanceFreq === 'year') isTrigger = mon === 1;
                else isTrigger = true;                    // month：每月首个切换日触发
            }
            if (mKey !== prevRebalanceMonthKey) prevRebalanceMonthKey = mKey;
            if (isTrigger) {
                // 当日各计划净值可得性
                const reNav = {};
                let allNavOk = true;
                for (const plan of singlePlans) {
                    let nav;
                    if (doFill) nav = fundNavMap[plan.fund].get(dateStr);
                    else { const idx = fundsData[plan.fund].dates.findIndex(d => formatDate(d) === dateStr); if (idx !== -1) nav = fundsData[plan.fund].nav[idx]; }
                    if (nav === undefined) { allNavOk = false; break; }
                    reNav[plan.id] = nav;
                }
                if (allNavOk) {
                    let totalMv = 0;
                    for (const plan of singlePlans) totalMv += planShares[plan.id] * reNav[plan.id];
                    if (totalMv > 0) {
                        const th = rebalanceThreshold / 100;
                        const sells = [], buys = [];
                        for (const plan of singlePlans) {
                            const curW = (planShares[plan.id] * reNav[plan.id]) / totalMv;
                            const tW = targetWeight[plan.id];
                            if (th > 0 && Math.abs(curW - tW) <= th) continue;  // 未超阈值不动
                            if (curW > tW) sells.push({ plan, nav: reNav[plan.id], targetMv: tW * totalMv });
                            else if (curW < tW) buys.push({ plan, nav: reNav[plan.id], targetMv: tW * totalMv });
                        }
                        // 先统一卖出超配：回充 totalCash
                        for (const s of sells) {
                            const curMv = planShares[s.plan.id] * s.nav;
                            let sellMv = curMv - s.targetMv;
                            if (sellMv <= 0) continue;
                            let sellShares = Math.min(planShares[s.plan.id], sellMv / s.nav);
                            if (sellShares <= 0) continue;
                            const proceeds = sellShares * s.nav;
                            const beforeShares = planShares[s.plan.id];
                            planShares[s.plan.id] -= sellShares;
                            totalCash += proceeds;
                            if (beforeShares > 0) planCostBasis[s.plan.id] *= planShares[s.plan.id] / beforeShares;
                            rebalanceTotalTurnover += proceeds;
                            planRebalanceEvents[s.plan.id].push({ dateStr, direction: 'sell', proceeds, nav: s.nav });
                        }
                        // 再统一买入低配：消耗 totalCash（现金充足时才买，资金不足买尽可能多）
                        for (const b of buys) {
                            const curMv = planShares[b.plan.id] * b.nav;
                            let buyMv = b.targetMv - curMv;
                            if (buyMv <= 0) continue;
                            if (buyMv > totalCash) buyMv = totalCash;
                            if (buyMv <= 0) continue;
                            const buyShares = buyMv / b.nav;
                            planShares[b.plan.id] += buyShares;
                            totalCash -= buyMv;
                            planCostBasis[b.plan.id] += buyMv;
                            rebalanceTotalTurnover += buyMv;
                            planRebalanceEvents[b.plan.id].push({ dateStr, direction: 'buy', amount: buyMv, nav: b.nav });
                        }
                        if (sells.length || buys.length) rebalanceExecDates.push(dateStr);
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
            } else if (plan.type === 'maxDrawdown') {
                if (currentDateStr >= plan.startDate && currentDateStr <= plan.endDate) {
                    // 统一口径（与策略对比一致）：回撤窗口仅在该基金真实交易日推进与判定。
                    // 基金不交易（仅并集日历/基准有值）的日子不推窗、不判定，避免窗口密度差异导致首笔触发不一致；
                    // 并额外跳过周末（周六/周日）——基金数据里季末/年末可能出现与前日一致的周末净值点，
                    // 若据此推窗会在非交易日误触发回撤买入，故与策略对比一致排除。
                    const mdRealTradingIdx = fundData.dates.findIndex(d => formatDate(d) === currentDateStr);
                    const mdDow = mdRealTradingIdx !== -1 ? fundData.dates[mdRealTradingIdx].getUTCDay() : -1;
                    if (mdRealTradingIdx !== -1 && mdDow !== 6 && mdDow !== 0) {
                        const mdDays = Math.max(5, parseInt(plan.mdDays, 10) || 120);
                        const mdPctTh = (parseFloat(plan.mdPct) || 10) / 100;
                        // 回撤基准序列当日值：self=基金自身净值；portfolio=昨日组合净值；bench:<id>=导入基准净值
                        let mdVal;
                        const mb = plan.mdBench || 'self';
                        if (mb === 'portfolio') {
                            mdVal = comboNavHist.length ? comboNavHist[comboNavHist.length - 1] : null;
                        } else if (mb && mb.indexOf('bench:') === 0) {
                            const bid = parseInt(mb.slice(6), 10);
                            if (!benchNavMaps[bid]) benchNavMaps[bid] = getBenchmarkNavMap(bid);
                            const m = benchNavMaps[bid];
                            mdVal = m ? (m.get(currentDateStr) ?? null) : null;
                        } else {
                            mdVal = nav;   // self：基金自身净值（现状）
                        }
                        const hist = (planNavHist[plan.id] = planNavHist[plan.id] || []);
                        let dd = 0;   // 当日回撤比例（窗口高点 - 当日值）/ 窗口高点
                        if (mdVal != null && mdVal > 0) {   // 基准当日不可得或无效时跳过推窗，避免误触发
                            hist.push(mdVal);
                            if (hist.length > mdDays) hist.shift();
                        }
                        let high = hist.length ? hist[0] : 0;
                        for (let w = 1; w < hist.length; w++) if (hist[w] > high) high = hist[w];
                        if (high > 0 && mdVal != null && mdVal > 0) dd = (high - mdVal) / high;
                        const hit = high > 0 && dd >= mdPctTh;
                        if (plan.mdContinuous) {
                            // 连续投资：本轮次回撤超过阈值后，每次创出本轮次新的最深回撤（dd 严格大于本轮记录）才投一笔；
                            // 横盘（dd 未创新高）或反弹（dd 缩小）不投。首笔（本轮无记录）hit 即建仓。
                            // 创出阶段新高（回撤归零 dd<=0）即本轮次结束，重置本轮最深回撤，下一轮下跌重新计数。
                            if (mdVal != null && mdVal > 0 && dd <= 0) planMaxDD[plan.id] = null;
                            const maxDD = planMaxDD[plan.id];
                            if (hit && (maxDD == null || dd > maxDD)) shouldInvest = true;
                        } else if (hit) {
                            shouldInvest = true;
                            hist.length = 0;   // 单笔模式：重置窗口，创阶段新高后重新计算可再触发
                        }
                        // 当日序列值有效时，滚动更新本次记录的最大回撤比例（供次日比较是否创最深）
                        if (mdVal != null && mdVal > 0) {
                            const curMaxDD = planMaxDD[plan.id];
                            planMaxDD[plan.id] = Math.max(curMaxDD != null ? curMaxDD : 0, dd);
                        }
                    }
                }
            }
            if (shouldInvest && poolBalance >= amt) {
                planShares[plan.id] += amt / nav;
                planRunPrincipal[plan.id] += amt;
                planCostBasis[plan.id] += amt;
                planRunMaxPrincipal[plan.id] = Math.max(planRunMaxPrincipal[plan.id], planRunPrincipal[plan.id]);
                planMaxPrincipal[plan.id] = Math.max(planMaxPrincipal[plan.id], planRunPrincipal[plan.id]);
                dailyInv += amt;
                cashFlows.push(-amt);
                flowDates.push(new Date(currentDt));
                poolBalance -= amt;
                if (poolBalance < minPoolBalance) minPoolBalance = poolBalance;   // 更新运行期最低余额
                if (plan.type === 'maxDrawdown') planMdEvents[plan.id].push({ fund: plan.fund, dateStr: currentDateStr, nav: nav, amt: amt });
                else if (plan.type === 'single' || plan.type === 'weekly' || plan.type === 'biweekly' || plan.type === 'monthly') {
                    planInvestEvents[plan.id].push({ type: plan.type, fund: plan.fund, dateStr: currentDateStr, nav: nav, amt: amt });
                }
            }
        }
        let mv = 0;
        for (const plan of investmentPlans) {
            const code = plan.fund;
            const fundData = fundsData[code];
            let nav;
            if (doFill) nav = fundNavMap[code].get(dateStr);
            else { const idx = fundData.dates.findIndex(d => formatDate(d) === dateStr); if (idx !== -1) nav = fundData.nav[idx]; }
            if (nav !== undefined) mv += planShares[plan.id] * nav;
        }
        dailyAsset.push(mv + totalCash);
        comboNavHist.push(mv + totalCash);   // 组合净值入窗（供 maxDrawdown 组合净值基准）
        dailyHoldAsset.push(mv);
        dailyDates.push(new Date(currentDt));
        dailyInvest.push(dailyInv);
        dailyCashDiv.push(totalCashDiv);
        dailyTotalCash.push(totalCash);
    }

    let marketValue = 0;
    for (const plan of investmentPlans) {
        const code = plan.fund;
        const fundData = fundsData[code];
        marketValue += planShares[plan.id] * fundData.nav[fundData.nav.length-1];
    }
    const cashDiv = totalCashDiv;
    const totalAsset = marketValue + totalCash;
    cashFlows.push(marketValue);
    flowDates.push(dailyDates[dailyDates.length-1]);
    const totalInvest = -cashFlows.reduce((s, v) => s + (v < 0 ? v : 0), 0);
    const totalReturn = totalInvest > 0 ? (totalAsset / totalInvest - 1) * 100 : 0;
    // 止盈聚合：按 plan 维度统计后，按 plan.fund 汇总回填（保持按基金展示结构）
    const stopGainByFund = {};
    const stopGainEvents = [];
    let totalMaxPrincipal = 0, totalRedeemedAll = 0;
    investmentPlans.forEach(plan => {
        totalMaxPrincipal += planMaxPrincipal[plan.id];
        totalRedeemedAll += planRedeemed[plan.id];
        if (planStopGainEvents[plan.id].length) {
            const code = plan.fund;
            if (!stopGainByFund[code]) stopGainByFund[code] = { events: [], totalRedeemed: 0 };
            planStopGainEvents[plan.id].forEach(e => {
                stopGainByFund[code].events.push(e);
                stopGainByFund[code].totalRedeemed += e.proceeds;
                stopGainEvents.push({ fund: code, dateStr: e.dateStr, proceeds: e.proceeds, ratio: e.ratio, nav: e.nav });
            });
        }
    });
    // 主动赎回聚合：按 plan 维度统计后，按 plan.fund 汇总回填（供赎回金额浮窗展示明细）
    const activeRedeemByFund = {};
    const activeRedeemEvents = [];
    let totalActiveRedeemed = 0;
    investmentPlans.forEach(plan => {
        totalActiveRedeemed += planActiveRedeemed[plan.id];
        if (planActiveRedeemEvents[plan.id].length) {
            const code = plan.fund;
            if (!activeRedeemByFund[code]) activeRedeemByFund[code] = { events: [], totalRedeemed: 0 };
            planActiveRedeemEvents[plan.id].forEach(e => {
                activeRedeemByFund[code].events.push(e);
                activeRedeemByFund[code].totalRedeemed += e.proceeds;
                activeRedeemEvents.push({ fund: code, dateStr: e.dateStr, proceeds: e.proceeds, nav: e.nav, mode: e.mode });
            });
        }
    });
    const hasActiveRedeemPlan = investmentPlans.some(p => p.activeRedeems && p.activeRedeems.length);
    // 最大回撤投资聚合：按 plan 汇总触发加仓事件（用于组合总资产曲线三角标注）
    const mdEvents = [];
    investmentPlans.forEach(plan => {
        if (planMdEvents[plan.id] && planMdEvents[plan.id].length) {
            planMdEvents[plan.id].forEach(e => mdEvents.push(e));
        }
    });
    // 普通定投/单笔投资聚合：按 plan 汇总触发事件（用于组合总资产曲线圆点标注）
    const investEvents = [];
    investmentPlans.forEach(plan => {
        if (planInvestEvents[plan.id] && planInvestEvents[plan.id].length) {
            planInvestEvents[plan.id].forEach(e => investEvents.push(e));
        }
    });
    const hasStopGainPlan = investmentPlans.some(p => p.stopGain);
    const netProfit = totalAsset - totalInvest;
    const maxPrincipalReturn = totalMaxPrincipal > 0 ? netProfit / totalMaxPrincipal * 100 : 0;
    const combined = flowDates.map((d, i) => ({ date: d, cf: cashFlows[i] }));
    combined.sort((a, b) => a.date - b.date);
    const xirrVal = xirr(combined.map(c=>c.cf), combined.map(c=>c.date)) * 100;
    // XIRR 迭代失败时已降级为修正 Dietz 近似，标注以示区分
    const xirrIsApprox = xirrLastApprox;

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
    backtestResult = { dates: validDates, assets: validAssets, holdAssets: dailyHoldAsset.slice(startIdx), netValues, invests: validInvest, cashDivs: validCashDivs, totalCashSeries: validTotalCash,
        simDateStrs: allDateStrs, simNav: simNav, simDiv: simDiv, simDow: simDow, simDateTs: simDateTs, simDayOfMonth: simDayOfMonth, simStartIdx: startIdx,
        stopGainByFund, stopGainEvents, totalMaxPrincipal, totalRedeemedAll, maxPrincipalReturn, hasStopGainPlan, mdEvents, investEvents,
        activeRedeemByFund, activeRedeemEvents, totalActiveRedeemed, hasActiveRedeemPlan,
        maxDDPeak: _m.maxDDPeak, maxDDTrough: _m.maxDDTrough, ddDurPeak: _m.ddDurPeak, ddDurTrough: _m.ddDurTrough,
        rebalance: { canRebalance, execDates: rebalanceExecDates, totalTurnover: rebalanceTotalTurnover, planEvents: planRebalanceEvents, freq: rebalanceFreq, threshold: rebalanceThreshold } };

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

        <div class="bg-emerald-50 p-4 rounded-lg text-center" data-mkey="累计收益率(资金加权)"><div class="text-sm text-slate-500">累计收益率(资金加权)</div><div class="text-2xl font-bold text-emerald-800">${totalReturn.toFixed(2)}%</div></div>
        <div class="bg-[rgba(16,185,129,0.12)] p-4 rounded-lg text-center" data-mkey="XIRR年化(资金加权)"><div class="text-sm text-slate-500">XIRR年化(资金加权)${xirrIsApprox?'<span class="text-amber-600" title="XIRR 迭代未收敛（现金流多次变号或无解），已降级为修正 Dietz 近似值">*近似</span>':''}</div><div class="text-2xl font-bold text-emerald-800">${isNaN(xirrVal)?'-':xirrVal.toFixed(2)+'%'}</div></div>
        <div class="bg-emerald-50 p-4 rounded-lg text-center" data-mkey="年化收益率(时间加权净值)"><div class="text-sm text-slate-500">年化收益率(时间加权净值)</div><div class="text-2xl font-bold text-emerald-800">${twrHtml}</div></div>
        <div class="bg-emerald-50 p-4 rounded-lg text-center" data-mkey="胜率(正收益日占比)"><div class="text-sm text-slate-500">胜率(正收益日占比)</div><div class="text-2xl font-bold text-emerald-800">${winHtml}</div></div>
        <div class="bg-amber-50 p-4 rounded-lg text-center cursor-help" data-mkey="赎回金额"><div class="text-sm text-slate-500">赎回金额</div><div class="text-2xl font-bold text-amber-700">${totalRedeemedAll.toFixed(2)} 元</div></div>

        ${riskHtml}
        <div class="bg-amber-50 p-4 rounded-lg text-center cursor-help" data-mkey="峰值本金收益率"><div class="text-sm text-slate-500">峰值本金收益率</div><div class="text-2xl font-bold text-amber-700">${peakReturnVal.toFixed(2)}%</div></div>
    `;
    // 再平衡摘要（仅在满足再平衡条件时展示）
    const rebalanceSummaryEl = document.getElementById('rebalanceSummary');
    if (rebalanceSummaryEl) {
        const rbl = backtestResult.rebalance;
        if (rbl && rbl.canRebalance) {
            const freqLabel = ({ month: '月', quarter: '季', half: '半年', year: '年' })[rbl.freq] || '月';
            const execN = (rbl.execDates || []).length;
            const lastDate = execN > 0 ? rbl.execDates[execN - 1] : '-';
            const turnover = rbl.totalTurnover || 0;
            const thInfo = rbl.threshold > 0 ? `，偏离阈值 ${rbl.threshold}%` : '，完全调回';
            rebalanceSummaryEl.innerHTML = `<div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm text-gray-600 px-4 py-3 bg-indigo-50 rounded-lg border border-indigo-100">
                <span class="font-medium text-indigo-700 cursor-help" title="悬停查看每次再平衡明细" data-rebalance-label>再平衡：</span>
                <span>每${freqLabel}执行</span>
                <span>执行 ${execN} 次</span>
                <span>最后触发：${lastDate}</span>
                <span>累计调仓：${turnover.toFixed(2)} 元${thInfo}</span>
                <span class="text-xs text-gray-400">目标权重 = 各计划初始单笔金额占比；再平衡为组合内部调仓，不影响 XIRR 本金口径</span>
            </div>`;
            // 绑定再平衡明细浮窗：悬停"再平衡："显示每次执行的买卖明细
            const rbTipEl = document.createElement('div');
            rbTipEl.className = 'hidden absolute z-30 left-0 bottom-full mb-1 w-96 max-h-80 overflow-auto bg-white border border-indigo-300 rounded-lg shadow-xl p-3 text-sm';
            rebalanceSummaryEl.appendChild(rbTipEl);
            const rbLabel = rebalanceSummaryEl.querySelector('[data-rebalance-label]');
            if (rbTipEl && rbLabel) {
                const buildRebalanceDetail = function () {
                    const _r = backtestResult.rebalance;
                    const byDate = new Map();
                    if (_r && _r.planEvents) {
                        for (const pid in _r.planEvents) {
                            const plan = investmentPlans.find(p => String(p.id) === String(pid));
                            (_r.planEvents[pid] || []).forEach(ev => {
                                if (!byDate.has(ev.dateStr)) byDate.set(ev.dateStr, []);
                                byDate.get(ev.dateStr).push(Object.assign({ fund: plan ? plan.fund : '' }, ev));
                            });
                        }
                    }
                    let html = '';
                    byDate.forEach((evs, ds) => {
                        html += '<div class="mb-1 font-medium text-indigo-700">' + ds + '</div>';
                        evs.forEach(e => {
                            const cn = fundCodeName(e.fund);
                            const amt = e.direction === 'sell' ? e.proceeds : e.amount;
                            html += '<div class="text-gray-600 pl-2">' + (e.direction === 'sell' ? '卖出 ' : '买入 ') + cn.code + ' ' + amt.toFixed(2) + '元 @' + e.nav.toFixed(4) + '</div>';
                        });
                    });
                    return html || '<div class="text-gray-500">无再平衡明细</div>';
                };
                let rbTimer = null;
                const rbShow = () => { clearTimeout(rbTimer); rbTipEl.innerHTML = buildRebalanceDetail(); rbTipEl.classList.remove('hidden'); };
                const rbHide = () => { rbTimer = setTimeout(() => rbTipEl.classList.add('hidden'), 150); };
                rbLabel.addEventListener('mouseenter', rbShow);
                rbLabel.addEventListener('mouseleave', rbHide);
                rbTipEl.addEventListener('mouseenter', () => clearTimeout(rbTimer));
                rbTipEl.addEventListener('mouseleave', rbHide);
            }
        } else {
            rebalanceSummaryEl.innerHTML = '';
        }
    }
    // 资金池剩余/上限：显示在「可用最大资金池(元)」输入框左侧
    const comboPoolStatEl = document.getElementById('comboPoolStat');
    if (comboPoolStatEl) {
        if (isFinite(comboPoolCap)) {
            const comboUsed = comboPoolCap - minPoolBalance;
            comboPoolStatEl.textContent = `资金池峰值占用 / 上限：${comboUsed.toFixed(0)} / ${comboPoolCap.toFixed(0)} 元`;
            comboPoolStatEl.title = '所有投资共享此资金池；峰值占用 = 上限 − 运行过程中的最低余额，反映这笔资金被同时占用的最高水位';
            comboPoolStatEl.classList.remove('hidden');
        } else {
            comboPoolStatEl.textContent = '';
            comboPoolStatEl.classList.add('hidden');
        }
    }
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
    // 回测成功后显示结果板块（点击启动回测前保持隐藏）
    const rs = document.getElementById('resultSection'); if (rs) rs.style.display = 'block';
}

// 构建止盈/主动赎回浮窗内容（按单基金列出触发明细）
function buildStopGainTip() {
    const hasSg = backtestResult.hasStopGainPlan;
    const sgByFund = backtestResult.stopGainByFund || {};
    const sgCodes = hasSg ? Object.keys(sgByFund) : [];
    const hasAr = backtestResult.hasActiveRedeemPlan;
    const arByFund = backtestResult.activeRedeemByFund || {};
    const arCodes = hasAr ? Object.keys(arByFund) : [];
    if (sgCodes.length === 0 && arCodes.length === 0) {
        if (!hasSg && !hasAr) return '<div class="text-amber-700 font-medium">未启用目标止盈，也无主动赎回配置</div>';
        return '<div class="text-amber-700 font-medium">已配置赎回，但回测区间内未触发。</div>';
    }
    let html = '';
    if (sgCodes.length > 0) {
        html += '<div class="font-semibold text-amber-800 mb-2">止盈触发明细（按单基金）</div>';
        for (const code of sgCodes) {
            const cn = fundCodeName(code);
            const info = sgByFund[code];
            html += '<div class="mb-3 pb-2 border-b border-amber-100">';
            html += '<div class="font-medium text-gray-800">' + cn.code + ' ' + (cn.name || '') + '</div>';
            html += '<div class="text-xs text-gray-500">触发 ' + info.events.length + ' 次 · 累计赎回 ' + info.totalRedeemed.toFixed(2) + ' 元</div>';
            info.events.forEach(function(e) {
                html += '<div class="text-xs text-gray-600 mt-1">· ' + e.dateStr + ' 赎回 ' + e.proceeds.toFixed(2) + ' 元（' + (e.ratio*100).toFixed(0) + '%，净值 ' + e.nav.toFixed(4) + '）</div>';
            });
            html += '</div>';
        }
    }
    if (arCodes.length > 0) {
        if (html) html += '<div class="my-2 border-t border-amber-200"></div>';
        html += '<div class="font-semibold text-orange-700 mb-2">主动赎回明细（按单基金）</div>';
        for (const code of arCodes) {
            const cn = fundCodeName(code);
            const info = arByFund[code];
            html += '<div class="mb-3 pb-2 border-b border-amber-100 last:border-0">';
            html += '<div class="font-medium text-gray-800">' + cn.code + ' ' + (cn.name || '') + '</div>';
            html += '<div class="text-xs text-gray-500">触发 ' + info.events.length + ' 次 · 累计赎回 ' + info.totalRedeemed.toFixed(2) + ' 元</div>';
            info.events.forEach(function(e) {
                const modeTxt = e.mode === 'amount' ? '按金额' : '按比例';
                html += '<div class="text-xs text-gray-600 mt-1">· ' + e.dateStr + ' 赎回 ' + e.proceeds.toFixed(2) + ' 元（' + modeTxt + '，净值 ' + e.nav.toFixed(4) + '）</div>';
            });
            html += '</div>';
        }
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

