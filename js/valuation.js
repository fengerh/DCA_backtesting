/* valuation.js —— 指数估值比较模块
 * 复用「基准指数」数据（db.benchmarks / benchmarkListCache，每条 data = [{date, nav}]）。
 * 功能：
 *   ① 单指数：点位图 + 自定义窗口（年/交易日）滚动百分位图 + 高低阈值线
 *   ② 双指数：点位 / N日均值 比值图 + 比值滚动百分位图 + 高低阈值线
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> valuation -> main
 */

// 估值图表实例（重绘前 destroy，避免 canvas 叠加）
let valPointChart = null, valRatioChart = null;

// 交易日折算：A股近似一年 252 个交易日
const VALUATION_TRADING_DAYS_PER_YEAR = 252;

// 从 benchmarkListCache 读取一条基准
function getBenchmarkById(id) {
    if (id == null) return null;
    return benchmarkListCache.find(b => String(b.id) === String(id)) || null;
}

// 渲染估值下拉（单指数 + 双指数 A/B），无基准时显示提示
function refreshValuationLists() {
    const noDataEl = document.getElementById('valuationNoData');
    const single = document.getElementById('valSingleIndex');
    const ratioA = document.getElementById('valRatioA');
    const ratioB = document.getElementById('valRatioB');
    if (!single) return;

    const benchmarks = benchmarkListCache || [];
    const hasData = benchmarks.length > 0;
    if (noDataEl) noDataEl.classList.toggle('hidden', hasData);
    single.disabled = !hasData;
    ratioA.disabled = !hasData;
    ratioB.disabled = !hasData;

    if (!hasData) {
        single.innerHTML = '<option value="">-- 无基准 --</option>';
        ratioA.innerHTML = '<option value="">-- 无基准 --</option>';
        ratioB.innerHTML = '<option value="">-- 无基准 --</option>';
        return;
    }

    const optsHtml = benchmarks.map(b =>
        `<option value="${b.id}">${b.name} (${b.data[0]?.date?.slice(0,7) || ''} ~ ${b.data[b.data.length-1]?.date?.slice(0,7) || ''})</option>`).join('');
    single.innerHTML = optsHtml;
    ratioA.innerHTML = optsHtml;
    ratioB.innerHTML = optsHtml;
    // 双指数默认：B 取第二个（若存在），避免 A/B 相同
    if (benchmarks.length > 1) ratioB.value = benchmarks[1].id;
    // 切换指数时自动填充共同交易日范围，并重置展示日期以同步匹配新的交集
    ratioA.onchange = syncRatioRangeOnIndexChange;
    ratioB.onchange = syncRatioRangeOnIndexChange;
    if (benchmarks.length >= 2) autoFillRatioDates();
    // 单指数：切换指数时自动填充点位时间区间为全部数据
    single.onchange = autoFillSingleRange;
    autoFillSingleRange();
}

// 自动填充单指数点位时间区间：基准指数变化时，开始/结束日期取该指数的最大日期范围
function autoFillSingleRange() {
    const startEl = document.getElementById('valSingleStart');
    const endEl = document.getElementById('valSingleEnd');
    if (!startEl || !endEl) return;
    const id = document.getElementById('valSingleIndex').value;
    const bm = getBenchmarkById(id);
    if (!bm || !bm.data || !bm.data.length) return;
    const sorted = bm.data.slice().sort((a, b) => a.date.localeCompare(b.date));
    const first = sorted[0].date, last = sorted[sorted.length - 1].date;
    // 始终按当前选中基准指数的全部日期范围刷新（取该指数的最大可用区间）
    startEl.value = first;
    endEl.value = last;
    // 点位与展示日期输入都设置 min/max 上限，防止年份溢出数据范围
    [startEl, endEl].forEach(el => { el.min = first; el.max = last; });
    // 同步填充独立展示区间默认为该指数的默认区间（指数最大日期范围），切换指数时一并刷新
    const showStartEl = document.getElementById('valSingleShowStart');
    const showEndEl = document.getElementById('valSingleShowEnd');
    if (showStartEl) { showStartEl.value = first; showStartEl.min = first; showStartEl.max = last; }
    if (showEndEl) { showEndEl.value = last; showEndEl.min = first; showEndEl.max = last; }
}

// 滚动百分位序列：对已排序 dates/navs，取每个 index 以 i 为终点、长度<=window 的窗口，
// 返回 { startIdx, percentile: 当日点位在窗口内超过百分之多少的历史样本 }
function rollingPercentile(navs, windowN) {
    const n = navs.length;
    // 维护有序窗口（二分插入），随索引右移动态进出
    const sorted = [];   // 当前窗口已排序值
    const out = new Array(n);
    let windowStart = 0;
    for (let i = 0; i < n; i++) {
        const val = navs[i];
        // 插入当前值（在计算 percentil 前先插入，窗口含当日）
        let lo = 0, hi = sorted.length;
        while (lo < hi) { const mid = (lo + hi) >> 1; if (sorted[mid] < val) lo = mid + 1; else hi = mid; }
        sorted.splice(lo, 0, val);
        // 移除超出窗口（含当日，窗口大小上限为 windowN）的最早值
        if (i - windowStart + 1 > windowN) {
            const evict = navs[windowStart++];
            let elo = 0, ehi = sorted.length;
            while (elo < ehi) { const emid = (elo + ehi) >> 1; if (sorted[emid] < evict) elo = emid + 1; else ehi = emid; }
            sorted.splice(elo, 1);
        }
        // 百分位 = 窗口内小于 val 的样本占比
        // 窗口未满（数据起始处不足所选滚动年度）时留白（null），避免用累计窗口造成前期大幅波动
        const less = lo; // lo 即 val 应插入的位置 = 严格小于 val 的元素个数
        out[i] = (sorted.length < windowN) ? null : (less / sorted.length) * 100;
    }
    return { startIdx: windowStart, percentile: out };
}

// 滚动 N 日均值：第 i 日为 [max(0,i-n+1), i] 内样本均值；不足 n 时按累计样本均值
function rollingMean(navs, n) {
    const m = Math.max(1, n);
    const out = new Array(navs.length);
    let sum = 0;
    for (let i = 0; i < navs.length; i++) {
        sum += navs[i];
        if (i >= m) sum -= navs[i - m];
        const cnt = Math.min(i + 1, m);
        out[i] = sum / cnt;
    }
    return out;
}

// 生成单指数估值图（点位左轴 + 百分位右轴 合并一张图）
function renderSingleValuation() {
    const single = document.getElementById('valSingleIndex');
    const hint = document.getElementById('valSingleHint');
    if (!single) return;
    const id = single.value;
    const bm = getBenchmarkById(id);
    if (!bm) { if (hint) hint.textContent = '请先选择基准指数'; return; }

    const data = bm.data.slice().sort((a, b) => a.date.localeCompare(b.date));
    // 点位时间区间：空 = 全部数据
    const start = document.getElementById('valSingleStart').value;
    const end = document.getElementById('valSingleEnd').value;
    // 年份边界控制：非法（5 位及以上年份/越界）则中止渲染，避免异常日期进入图表
    if (start && sanitizeDateInput(start) === null) return;
    if (end && sanitizeDateInput(end) === null) return;
    if (start && end && start > end) { if (hint) hint.textContent = '开始日期晚于结束日期，请检查'; return; }
    // 独立展示区间：仅控制图表显示范围，不影响百分位计算；空 = 随点位区间
    // 防溢出：将展示日期钳制到该指数实际数据范围内，年份不会超出可用区间
    const dataFirst = data[0].date, dataLast = data[data.length - 1].date;
    let showStart = document.getElementById('valSingleShowStart').value;
    let showEnd = document.getElementById('valSingleShowEnd').value;
    // 年份边界控制：非法则中止渲染
    if (showStart && sanitizeDateInput(showStart) === null) return;
    if (showEnd && sanitizeDateInput(showEnd) === null) return;
    if (showStart && (showStart < dataFirst)) showStart = dataFirst;
    if (showStart && (showStart > dataLast)) showStart = dataLast;
    if (showEnd && (showEnd < dataFirst)) showEnd = dataFirst;
    if (showEnd && (showEnd > dataLast)) showEnd = dataLast;
    const dispStart = showStart || start;
    const dispEnd = showEnd || end;
    if (dispStart && dispEnd && dispStart > dispEnd) { if (hint) hint.textContent = '展示开始日期晚于展示结束日期，请检查'; return; }
    const hi = parseFloat(document.getElementById('valSingleHi').value) || 80;
    const lo = parseFloat(document.getElementById('valSingleLo').value) || 20;
    // 点位百分位均值 N：>1 时对点位做 N 日均线后再算百分位（减少毛刺），点位曲线本身不变
    const meanN = parseInt(document.getElementById('valSingleMeanN').value, 10) || 0;

    const dates = data.map(d => d.date);
    const navs = data.map(d => d.nav);

    // 三个滚动年度（百分位窗口）：读有效值（空/<=0 跳过）；全为 0/空 则不画百分位线
    const rollIds = ['valSingleRollYears1', 'valSingleRollYears2', 'valSingleRollYears3'];
    const rollYears = [];
    for (const rid of rollIds) {
        const el = document.getElementById(rid);
        if (!el) continue;
        const v = parseFloat(el.value);
        if (!isNaN(v) && v > 0) rollYears.push(v);
    }

    // 百分位基于指数完整历史（含展示区间之前数据），保证"当前点位相对前 N 年"正确。
    // 均值 N 启用时，百分位对 N 日均线序列计算（减少毛刺）；点位曲线仍用原始 navs。
    // 对每个滚动年度先算全序列百分位（O(n log w)），再按展示区间过滤。
    const pctBase = meanN > 1 ? rollingMean(navs, meanN) : navs;
    const pctDates = [], pointVals = [], pctSeries = [];
    for (let k = 0; k < rollYears.length; k++) {
        const windowN_k = Math.max(1, rollYears[k]) * VALUATION_TRADING_DAYS_PER_YEAR;
        pctSeries.push(rollingPercentile(pctBase, windowN_k).percentile);
    }
    for (let i = 0; i < navs.length; i++) {
        if (dispStart && dates[i] < dispStart) continue;
        if (dispEnd && dates[i] > dispEnd) continue;
        pctDates.push(dates[i]);
        pointVals.push(navs[i]);
    }
    // 百分位序列按同一过滤后的索引截取
    for (let k = 0; k < rollYears.length; k++) {
        const src = pctSeries[k];
        const filtered = [];
        for (let i = 0; i < navs.length; i++) {
            if (dispStart && dates[i] < dispStart) continue;
            if (dispEnd && dates[i] > dispEnd) continue;
            filtered.push(src[i]);
        }
        pctSeries[k] = filtered;
    }

    if (hint) hint.textContent = `点位区间 ${start || '全部'} ~ ${end || '全部'}，展示 ${dispStart || '全部'} ~ ${dispEnd || '全部'}，百分位滚动 ${rollYears.join('/')} 年${rollYears.length === 0 ? '（未填写，不画百分位线）' : ''}${meanN > 1 ? `（按 ${meanN} 日均值算）` : ''}，阈值高 ${hi}% / 低 ${lo}%（历史不足相应年度按累计值）`;

    const ctx = document.getElementById('valPointChart').getContext('2d');
    if (valPointChart) valPointChart.destroy();
    const SINGLE_ROLL_COLORS = ['rgba(16,185,129,0.5)', 'rgba(139,92,246,0.5)', 'rgba(245,158,11,0.5)'];
    const datasets = [
        { label: `${bm.name} 点位`, data: pointVals, yAxisID: 'y', borderColor: '#6366F1',
          backgroundColor: 'rgba(99,102,241,0.08)', fill: true, tension: 0.1, pointRadius: 0, borderWidth: 2 }
    ];
    for (let k = 0; k < rollYears.length; k++) {
        datasets.push({ label: `百分位 ${rollYears[k]} 年 (%)${meanN > 1 ? ` / ${meanN}日均值` : ''}`, data: pctSeries[k], yAxisID: 'y1',
            borderColor: SINGLE_ROLL_COLORS[k % SINGLE_ROLL_COLORS.length],
            backgroundColor: 'transparent', fill: false, tension: 0.1, pointRadius: 0, borderWidth: 2 });
    }
    if (hi > 0) datasets.push({ label: `高估线 ${hi}%`, data: pctDates.map(() => hi), yAxisID: 'y1', borderColor: '#EF4444', borderDash: [6, 3], pointRadius: 0, borderWidth: 1.5, fill: false });
    if (lo > 0) datasets.push({ label: `低估线 ${lo}%`, data: pctDates.map(() => lo), yAxisID: 'y1', borderColor: '#3B82F6', borderDash: [6, 3], pointRadius: 0, borderWidth: 1.5, fill: false });
    valPointChart = new Chart(ctx, {
        type: 'line',
        data: { labels: pctDates, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { labels: { boxWidth: 12 } }, tooltip: { mode: 'index', intersect: false } },
            scales: {
                x: { ticks: { maxTicksLimit: 12 } },
                y: { position: 'left', title: { display: true, text: '点位' } },
                y1: { position: 'right', min: 0, max: 100, grid: { drawOnChartArea: false }, title: { display: true, text: '百分位 (%)' } }
            }
        }
    });
}

// 双指数共同交易日范围（交集）：返回 { start, end } 日期字符串；无交集返回 null
function commonDateRange(bmA, bmB) {
    const datesA = bmA.data.map(d => d.date).sort();
    const datesB = bmB.data.map(d => d.date).sort();
    const setB = new Set(datesB);
    const common = datesA.filter(date => setB.has(date));
    if (common.length === 0) return null;
    return { start: common[0], end: common[common.length - 1] };
}

// 自动填充双指数比值日期范围：取两指数共同交易日范围（较晚开始、较早结束）
function autoFillRatioDates() {
    const hint = document.getElementById('valRatioHint');
    const idA = document.getElementById('valRatioA').value;
    const idB = document.getElementById('valRatioB').value;
    if (!idA || !idB) { if (hint) hint.textContent = '请先选择指数 A 与指数 B'; return; }
    if (String(idA) === String(idB)) { if (hint) hint.textContent = '指数 A 与 B 不能相同'; return; }
    const bmA = getBenchmarkById(idA), bmB = getBenchmarkById(idB);
    if (!bmA || !bmB) return;
    const range = commonDateRange(bmA, bmB);
    if (!range) { if (hint) hint.textContent = '两个指数无共同交易日'; return; }
    document.getElementById('valRatioStart').value = range.start;
    document.getElementById('valRatioEnd').value = range.end;
    // 点位与展示日期输入都设置 min/max 上限，防止年份溢出共同交易日范围
    const startEl = document.getElementById('valRatioStart');
    const endEl = document.getElementById('valRatioEnd');
    if (startEl) { startEl.min = range.start; startEl.max = range.end; }
    if (endEl) { endEl.min = range.start; endEl.max = range.end; }
    // 展示日期默认跟随点位区间（仅当展示日期为空时设置，避免覆盖用户手动修改的值）
    const showStartEl = document.getElementById('valRatioShowStart');
    const showEndEl = document.getElementById('valRatioShowEnd');
    if (showStartEl) { if (!showStartEl.value) showStartEl.value = range.start; showStartEl.min = range.start; showStartEl.max = range.end; }
    if (showEndEl) { if (!showEndEl.value) showEndEl.value = range.end; showEndEl.min = range.start; showEndEl.max = range.end; }
    if (hint) hint.textContent = `共同交易日范围：${range.start} ~ ${range.end}（可手动调整）`;
}

// 切换双指数 A/B 时：先清空展示日期，再自动填充新的共同交易日交集，
// 使展示日期与点位日期一起同步匹配新的交集
function syncRatioRangeOnIndexChange() {
    const showStartEl = document.getElementById('valRatioShowStart');
    const showEndEl = document.getElementById('valRatioShowEnd');
    if (showStartEl) showStartEl.value = '';
    if (showEndEl) showEndEl.value = '';
    autoFillRatioDates();
}

// 双指数比值：交集对齐后，在 [start,end] 日期范围内逐日计算原始比值（A/B）
// 主曲线与百分位各自的 N 日均值平滑由调用方（renderRatioValuation）独立处理，此处始终返回原始比值
function buildRatioSeries(bmA, bmB, start, end) {
    const mapA = new Map(bmA.data.map(d => [d.date, d.nav]));
    const mapB = new Map(bmB.data.map(d => [d.date, d.nav]));
    // 共同日期（交集），并按起止日期裁剪
    const common = Array.from(mapA.keys()).filter(date => mapB.has(date) && date >= start && date <= end).sort();
    if (common.length === 0) return null;
    const ratioByDate = [];
    for (const date of common) ratioByDate.push({ date, r: mapA.get(date) / mapB.get(date) });
    return ratioByDate;
}

// 根据比值口径模式切换 N 日输入框的可用状态：点位比值 → 置灰禁用，N日均值比值 → 启用
function updateRatioModeInput() {
    const modeEl = document.getElementById('valRatioMode');
    const nEl = document.getElementById('valRatioN');
    if (!modeEl || !nEl) return;
    nEl.disabled = modeEl.value !== 'ma';
}

// 生成双指数比值图（比值左轴 + 比值百分位右轴 合并一张图）
function renderRatioValuation() {
    const hint = document.getElementById('valRatioHint');
    const idA = document.getElementById('valRatioA').value;
    const idB = document.getElementById('valRatioB').value;
    if (!idA || !idB) { if (hint) hint.textContent = '请先选择指数 A 与指数 B'; return; }
    if (String(idA) === String(idB)) { if (hint) hint.textContent = '指数 A 与 B 不能相同'; return; }
    const bmA = getBenchmarkById(idA), bmB = getBenchmarkById(idB);
    if (!bmA || !bmB) { if (hint) hint.textContent = '基准数据缺失'; return; }

    const mode = document.getElementById('valRatioMode').value;
    const n = parseInt(document.getElementById('valRatioN').value, 10) || 20;
    const hi = parseFloat(document.getElementById('valRatioHi').value) || 80;
    const lo = parseFloat(document.getElementById('valRatioLo').value) || 20;

    // 两个滚动窗口（年）：读有效值（空/<=0 跳过）；全为 0/空 则不画百分位线
    const rollIds = ['valRatioRollYears1', 'valRatioRollYears2'];
    const rollYears = [];
    for (const rid of rollIds) {
        const el = document.getElementById(rid);
        if (!el) continue;
        const v = parseFloat(el.value);
        if (!isNaN(v) && v > 0) rollYears.push(v);
    }

    // 点位区间：用户选择或默认交集；未填则自动填充
    const range = commonDateRange(bmA, bmB);
    let start = document.getElementById('valRatioStart').value;
    let end = document.getElementById('valRatioEnd').value;
    if ((!start || !end) && range) { start = range.start; end = range.end;
        document.getElementById('valRatioStart').value = start;
        document.getElementById('valRatioEnd').value = end;
    }
    // 年份边界控制：非法（5 位及以上年份/越界）则中止渲染
    if (start && sanitizeDateInput(start) === null) return;
    if (end && sanitizeDateInput(end) === null) return;
    if (start > end) { if (hint) hint.textContent = '点位开始日期晚于点位结束日期，请检查'; return; }

    // 独立展示区间：仅控制图表显示范围，不影响百分位计算；
    // 展示日期首次（为空时）自动带入两指数交集范围，用户手动修改后保留
    const showStartEl = document.getElementById('valRatioShowStart');
    const showEndEl = document.getElementById('valRatioShowEnd');
    if (showStartEl && !showStartEl.value && range) showStartEl.value = range.start;
    if (showEndEl && !showEndEl.value && range) showEndEl.value = range.end;
    // 防溢出：将展示日期钳制到两指数共同交易日范围内，年份不会超出可用区间
    const rangeFirst = range ? range.start : null, rangeLast = range ? range.end : null;
    let showStart = showStartEl ? showStartEl.value : '';
    let showEnd = showEndEl ? showEndEl.value : '';
    // 年份边界控制：非法则中止渲染
    if (showStart && sanitizeDateInput(showStart) === null) return;
    if (showEnd && sanitizeDateInput(showEnd) === null) return;
    if (showStart && rangeFirst && showStart < rangeFirst) showStart = rangeFirst;
    if (showStart && rangeLast && showStart > rangeLast) showStart = rangeLast;
    if (showEnd && rangeFirst && showEnd < rangeFirst) showEnd = rangeFirst;
    if (showEnd && rangeLast && showEnd > rangeLast) showEnd = rangeLast;
    const dispStart = showStart || start;
    const dispEnd = showEnd || end;
    if (dispStart && dispEnd && dispStart > dispEnd) { if (hint) hint.textContent = '展示开始日期晚于展示结束日期，请检查'; return; }

    const series = buildRatioSeries(bmA, bmB, start, end);
    if (!series || series.length === 0) { if (hint) hint.textContent = '点位区间内无共同交易日，无法计算比值'; return; }

    const dates = series.map(s => s.date);
    const ratiosRaw = series.map(s => s.r);

    // 主曲线序列：按口径取逐日原始比值或 N 日均值比值（保留下拉切换）
    const ratioValsMain = mode === 'ma' ? rollingMean(ratiosRaw, n) : ratiosRaw;

    // 百分位基础序列：完全跟随主曲线口径（展示啥，就按什么算）——点位比值用原始比值，N日均值比值用同一 N 日均值序列
    const pctBase = ratioValsMain;

    // 对每个滚动窗口先算全序列百分位（保证"当前比值相对前 N 年"正确），再按展示区间过滤
    const pctSeries = [];
    for (let k = 0; k < rollYears.length; k++) {
        const windowN_k = Math.max(1, rollYears[k]) * VALUATION_TRADING_DAYS_PER_YEAR;
        pctSeries.push(rollingPercentile(pctBase, windowN_k).percentile);
    }
    const pctDates = [], ratioVals = [];
    for (let i = 0; i < ratiosRaw.length; i++) {
        if (dispStart && dates[i] < dispStart) continue;
        if (dispEnd && dates[i] > dispEnd) continue;
        pctDates.push(dates[i]);
        ratioVals.push(ratioValsMain[i]);
    }
    const pctVals = pctSeries.map(src => {
        const filtered = [];
        for (let i = 0; i < ratiosRaw.length; i++) {
            if (dispStart && dates[i] < dispStart) continue;
            if (dispEnd && dates[i] > dispEnd) continue;
            filtered.push(src[i]);
        }
        return filtered;
    });

    if (hint) hint.textContent = `点位区间 ${start || '全部'} ~ ${end || '全部'}，展示 ${dispStart || '全部'} ~ ${dispEnd || '全部'}，口径：${mode === 'ma' ? n + '日均值' : '点位'}比值（百分位同口径），滚动 ${rollYears.join('/')} 年${rollYears.length === 0 ? '（未填写，不画百分位线）' : ''}，阈值高 ${hi}% / 低 ${lo}%（历史不足相应年度按累计值）`;

    const ctx = document.getElementById('valRatioChart').getContext('2d');
    if (valRatioChart) valRatioChart.destroy();
    const RATIO_ROLL_COLORS = ['rgba(16,185,129,0.5)', 'rgba(139,92,246,0.5)', 'rgba(245,158,11,0.5)'];
    const ratioLabel = `${bmA.name} / ${bmB.name}${mode === 'ma' ? ` (${n}日均值)` : ''}`;
    const datasets = [
        { label: ratioLabel, data: ratioVals, yAxisID: 'y', borderColor: '#6366F1',
          backgroundColor: 'rgba(99,102,241,0.08)', fill: true, tension: 0.1, pointRadius: 0, borderWidth: 2 }
    ];
    for (let k = 0; k < rollYears.length; k++) {
        datasets.push({ label: `比值百分位 ${rollYears[k]} 年 (%)`, data: pctVals[k], yAxisID: 'y1',
            borderColor: RATIO_ROLL_COLORS[k % RATIO_ROLL_COLORS.length],
            backgroundColor: 'transparent', fill: false, tension: 0.1, pointRadius: 0, borderWidth: 2 });
    }
    if (hi > 0) datasets.push({ label: `高估线 ${hi}%`, data: pctDates.map(() => hi), yAxisID: 'y1', borderColor: '#EF4444', borderDash: [6, 3], pointRadius: 0, borderWidth: 1.5, fill: false });
    if (lo > 0) datasets.push({ label: `低估线 ${lo}%`, data: pctDates.map(() => lo), yAxisID: 'y1', borderColor: '#3B82F6', borderDash: [6, 3], pointRadius: 0, borderWidth: 1.5, fill: false });
    valRatioChart = new Chart(ctx, {
        type: 'line',
        data: { labels: pctDates, datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { labels: { boxWidth: 12 } }, tooltip: { mode: 'index', intersect: false } },
            scales: {
                x: { ticks: { maxTicksLimit: 12 } },
                y: { position: 'left', title: { display: true, text: '比值' } },
                y1: { position: 'right', min: 0, max: 100, grid: { drawOnChartArea: false }, title: { display: true, text: '百分位 (%)' } }
            }
        }
    });
}

// 初始化：基准列表就绪后填充下拉（由 loadBenchmarkList 之后或首次进入 valuation 模式调用）
function initValuation() {
    refreshValuationLists();
}
