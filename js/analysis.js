/* analysis.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// ============ 盈利概率 & 相关性分析 ============

// 在 [winStartIdx, winEndIdx] 闭区间（对齐 simDateStrs）内重放全部投资计划，
// 返回该窗口的资金加权持有收益与 XIRR 年化；区间内无扣款则返回 null。
// 采用“以窗口起点为锚点”的解释：即“假设在历史任意时点开始同一套定投计划并持有 holdDays”。
function simulateDcaWindow(winStartIdx, winEndIdx) {
    const dateStrs = backtestResult.simDateStrs;
    const simNav = backtestResult.simNav;
    const simDiv = backtestResult.simDiv;
    const simDow = backtestResult.simDow;
    const simDateTs = backtestResult.simDateTs;
    const simDayOfMonth = backtestResult.simDayOfMonth;
    if (!dateStrs || !simNav || !simDateTs) return null;
    const shares = {};
    let totalCash = 0;
    const cashFlows = [], flowDates = [];
    let invested = 0;
    for (const plan of investmentPlans) {
        const fund = plan.fund;
        const navArr = simNav[fund];
        const divArr = simDiv[fund];
        if (!navArr) continue;
        if (shares[fund] === undefined) shares[fund] = 0;
        let biweeklyInvestKs = null;
        if (plan.type === 'biweekly') {
            biweeklyInvestKs = new Set();
            let cnt = 0;
            for (let kk = winStartIdx; kk <= winEndIdx; kk++) {
                if (simDow[kk] === (plan.weekday != null ? plan.weekday : 1) && navArr[kk] != null && navArr[kk] !== undefined) {
                    if (cnt % 2 === 0) biweeklyInvestKs.add(kk);
                    cnt++;
                }
            }
        }
        for (let k = winStartIdx; k <= winEndIdx; k++) {
            const nav = navArr[k];
            if (nav === null || nav === undefined) continue;   // 基金尚未成立
            const ds = dateStrs[k];
            // 分红处理（基于已持有份额）
            if (shares[fund] > 0) {
                const divPerShare = divArr[k] || 0;
                if (divPerShare > 0) {
                    const totalDiv = shares[fund] * divPerShare;
                    if (plan.div === 'reinvest') shares[fund] += totalDiv / nav;
                    else { totalCash += totalDiv; cashFlows.push(totalDiv); flowDates.push(simDateTs[k]); }
                }
            }
            // 扣款判定（以窗口起点为锚）
            let shouldInvest = false;
            if (plan.type === 'single') { if (k === winStartIdx) shouldInvest = true; }
            else if (plan.type === 'weekly') { if (k >= winStartIdx && simDow[k] === 1) shouldInvest = true; }
            else if (plan.type === 'monthly') { if (k >= winStartIdx && simDayOfMonth[k] === 1) shouldInvest = true; }
            else if (plan.type === 'biweekly') { if (biweeklyInvestKs && biweeklyInvestKs.has(k)) shouldInvest = true; }
            if (shouldInvest) {
                shares[fund] += plan.amount / nav;
                invested += plan.amount;
                cashFlows.push(-plan.amount);
                flowDates.push(simDateTs[k]);
            }
        }
    }
    if (invested <= 0) return null;
    let mv = 0;
    for (const fund of Object.keys(shares)) {
        const endNav = simNav[fund][winEndIdx];
        if (endNav !== null && endNav !== undefined) mv += shares[fund] * endNav;
    }
    mv += totalCash;
    const holdingReturn = mv / invested - 1;
    cashFlows.push(mv);
    flowDates.push(simDateTs[winEndIdx]);
    const x = xirr(cashFlows, flowDates);
    const xirrAnnual = isNaN(x) ? NaN : x;
    return { holdingReturn, xirrAnnual };
}

// 给定持有期（交易日），按指定口径计算盈利概率、平均持有收益与平均年化收益
// mode: 'dca' = 定投模拟（资金加权，推荐）；'twr' = 组合净值（一次性买入，时间加权）
function calcProfit(holdDays, mode) {
    mode = mode || 'dca';
    const nv = backtestResult.netValues || [];
    const n = nv.length;
    if (n <= holdDays) return null;
    if (mode === 'twr') {
        let wins = 0, sum = 0, cnt = 0;
        for (let i = 0; i + holdDays < n; i++) {
            const ret = nv[i + holdDays] / nv[i] - 1;
            if (isFinite(ret)) { sum += ret; cnt++; if (ret > 0) wins++; }
        }
        if (cnt === 0) return null;
        const avgReturn = sum / cnt;
        // 时间加权年化：(1+平均持有收益)^(252/holdDays) - 1
        const avgAnnual = Math.pow(1 + avgReturn, 252 / holdDays) - 1;
        return { winRate: wins / cnt * 100, avgReturn: avgReturn * 100, avgAnnual: avgAnnual * 100, samples: cnt, annualSamples: cnt };
    }
    // 定投模拟口径
    const startIdx = backtestResult.simStartIdx || 0;
    let wins = 0, sum = 0, sumAnnual = 0, cnt = 0, annualCnt = 0;
    for (let i = 0; i + holdDays < n; i++) {
        const w = simulateDcaWindow(startIdx + i, startIdx + i + holdDays);
        if (!w) continue;
        sum += w.holdingReturn; cnt++;
        if (w.holdingReturn > 0) wins++;
        if (isFinite(w.xirrAnnual)) { sumAnnual += w.xirrAnnual; annualCnt++; }
    }
    if (cnt === 0) return null;
    return {
        winRate: wins / cnt * 100,
        avgReturn: sum / cnt * 100,
        avgAnnual: annualCnt > 0 ? sumAnnual / annualCnt * 100 : NaN,
        samples: cnt,
        annualSamples: annualCnt
    };
}

function setProfitMode(mode) {
    if (mode !== 'dca' && mode !== 'twr') return;
    profitMode = mode;
    document.getElementById('ppModeDca').className = 'px-3 py-1.5 text-sm ' + (mode === 'dca' ? 'bg-blue-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-100');
    document.getElementById('ppModeTwr').className = 'px-3 py-1.5 text-sm ' + (mode === 'twr' ? 'bg-blue-600 text-white' : 'bg-white text-gray-700 hover:bg-gray-100');
    renderProfitProbability();
}

// 批量计算盈利概率（异步分块 + 进度），彻底避免切换口径时主线程阻塞
// periods: [{label, days}]，mode: 'dca'（定投模拟）| 'twr'（组合净值）
// 返回 results: { [days]: {winRate, avgReturn, avgAnnual, samples, annualSamples} | null }
let ppComputeToken = 0;
function computeProfitProbabilityAsync(periods, mode, onProgress, onDone) {
    mode = mode || 'dca';
    const nv = backtestResult.netValues || [];
    const n = nv.length;
    const result = {};
    periods.forEach(p => { result[p.days] = null; });
    const validPeriods = periods.filter(p => n > p.days);
    if (validPeriods.length === 0) { onDone(result); return; }

    // 时间加权（组合净值）口径：O(n) 极快，同步即可
    if (mode === 'twr') {
        validPeriods.forEach(p => {
            const holdDays = p.days;
            let wins = 0, sum = 0, cnt = 0;
            for (let i = 0; i + holdDays < n; i++) {
                const ret = nv[i + holdDays] / nv[i] - 1;
                if (isFinite(ret)) { sum += ret; cnt++; if (ret > 0) wins++; }
            }
            if (cnt > 0) {
                const avgReturn = sum / cnt;
                result[p.days] = {
                    winRate: wins / cnt * 100,
                    avgReturn: avgReturn * 100,
                    avgAnnual: (Math.pow(1 + avgReturn, 252 / holdDays) - 1) * 100,
                    samples: cnt, annualSamples: cnt
                };
            }
        });
        onDone(result);
        return;
    }

    // 定投模拟（资金加权）口径：
    // 关键优化——对每个历史起点只重放一次（覆盖全部持有期），避免不同持有期重复重放（优化项 2）；
    // 内层使用预缓存时间戳与日份数组，不再 new Date（优化项 1）；分块异步执行并显示进度（优化项 4）。
    const simNav = backtestResult.simNav, simDiv = backtestResult.simDiv;
    const simDow = backtestResult.simDow, simDateTs = backtestResult.simDateTs, simDay = backtestResult.simDayOfMonth;
    const startIdx = backtestResult.simStartIdx || 0;
    if (!simNav || !simDateTs) { onDone(result); return; }
    const simLen = startIdx + n; // 等于 allDateStrs.length
    const maxHold = Math.max.apply(null, validPeriods.map(p => p.days));
    const minHold = Math.min.apply(null, validPeriods.map(p => p.days));
    const offsets = validPeriods.map(p => p.days).sort((a, b) => a - b);
    const acc = {};
    validPeriods.forEach(p => { acc[p.days] = { wins: 0, sum: 0, sumAnnual: 0, cnt: 0, annualCnt: 0 }; });

    const limit = n - minHold; // 起点循环上界（不含）
    let i = 0;
    const BATCH = 150;

    function processStart(S) {
        const shares = {};
        for (const plan of investmentPlans) { if (simNav[plan.fund]) shares[plan.fund] = 0; }
        let totalCash = 0, invested = 0;
        const cashFlows = [], flowDates = [];
        const baseK = startIdx + S;
        const Ek = Math.min(baseK + maxHold, simLen - 1);
        let offIdx = 0;
        const biweeklyKsMap = {};
        for (const plan of investmentPlans) {
            if (plan.type === 'biweekly') {
                const navArr = simNav[plan.fund];
                const set = new Set();
                let cnt = 0;
                for (let kk = baseK; kk <= Ek; kk++) {
                    if (simDow[kk] === (plan.weekday != null ? plan.weekday : 1) && navArr[kk] != null && navArr[kk] !== undefined) {
                        if (cnt % 2 === 0) set.add(kk);
                        cnt++;
                    }
                }
                biweeklyKsMap[plan.id] = set;
            }
        }
        for (let k = baseK; k <= Ek; k++) {
            const dOff = k - baseK;
            const dow = simDow[k];
            const day = simDay[k];
            for (const plan of investmentPlans) {
                const fund = plan.fund, navArr = simNav[fund];
                if (!navArr) continue;
                const nav = navArr[k];
                if (nav === null || nav === undefined) continue;
                const divArr = simDiv[fund];
                if (shares[fund] > 0) {
                    const divPerShare = divArr ? (divArr[k] || 0) : 0;
                    if (divPerShare > 0) {
                        const totalDiv = shares[fund] * divPerShare;
                        if (plan.div === 'reinvest') shares[fund] += totalDiv / nav;
                        else { totalCash += totalDiv; cashFlows.push(totalDiv); flowDates.push(simDateTs[k]); }
                    }
                }
                let shouldInvest = false;
                if (plan.type === 'single') { if (k === baseK) shouldInvest = true; }
                else if (plan.type === 'weekly') { if (k >= baseK && dow === 1) shouldInvest = true; }
                else if (plan.type === 'monthly') { if (k >= baseK && day === 1) shouldInvest = true; }
                else if (plan.type === 'biweekly') { if (biweeklyKsMap[plan.id] && biweeklyKsMap[plan.id].has(k)) shouldInvest = true; }
                if (shouldInvest) {
                    shares[fund] += plan.amount / nav;
                    invested += plan.amount;
                    cashFlows.push(-plan.amount);
                    flowDates.push(simDateTs[k]);
                }
            }
            while (offIdx < offsets.length && dOff >= offsets[offIdx]) {
                const off = offsets[offIdx];
                let mv = 0;
                for (const fund of Object.keys(shares)) {
                    const endNav = simNav[fund][k];
                    if (endNav !== null && endNav !== undefined) mv += shares[fund] * endNav;
                }
                mv += totalCash;
                if (invested > 0) {
                    const holdingReturn = mv / invested - 1;
                    if (isFinite(holdingReturn)) {
                        const a = acc[off];
                        a.sum += holdingReturn; a.cnt++;
                        if (holdingReturn > 0) a.wins++;
                        cashFlows.push(mv); flowDates.push(simDateTs[k]);
                        const x = xirr(cashFlows, flowDates);
                        cashFlows.pop(); flowDates.pop();
                        if (isFinite(x)) { a.sumAnnual += x; a.annualCnt++; }
                    }
                }
                offIdx++;
            }
        }
    }

    function step() {
        const end = Math.min(i + BATCH, limit);
        for (; i < end; i++) processStart(i);
        if (onProgress) onProgress(limit > 0 ? i / limit : 1);
        if (i < limit) setTimeout(step, 0);
        else {
            validPeriods.forEach(p => {
                const a = acc[p.days];
                if (a.cnt > 0) {
                    result[p.days] = {
                        winRate: a.wins / a.cnt * 100,
                        avgReturn: a.sum / a.cnt * 100,
                        avgAnnual: a.annualCnt > 0 ? a.sumAnnual / a.annualCnt * 100 : NaN,
                        samples: a.cnt, annualSamples: a.annualCnt
                    };
                }
            });
            onDone(result);
        }
    }
    step();
}

function renderProfitProbability() {
    const sec = document.getElementById('profitProbabilitySection');
    const tbl = document.getElementById('profitProbabilityTable');
    const nv = backtestResult.netValues || [];
    if (!nv || nv.length === 0) { sec.style.display = 'none'; return; }
    const basePeriods = [
        { label: '满6月', days: 126 },
        { label: '满1年', days: 252 },
        { label: '满2年', days: 504 },
        { label: '满3年', days: 756 }
    ];
    const custom = customPeriods.map(y => ({ label: y + '年', days: Math.max(1, Math.round(y * 252)) }));
    const all = basePeriods.concat(custom);

    // 先显示加载态（含进度），再异步计算，避免同步计算冻结 UI（优化项 4）
    const token = ++ppComputeToken;
    tbl.innerHTML = '<div class="p-8 text-center text-gray-500">盈利概率计算中… <span id="ppProg" class="font-medium text-blue-600"></span></div>';
    sec.style.display = 'block';

    computeProfitProbabilityAsync(all, profitMode, (prog) => {
        if (token !== ppComputeToken) return;
        const el = document.getElementById('ppProg');
        if (el) el.textContent = Math.round(prog * 100) + '%';
    }, (results) => {
        if (token !== ppComputeToken) return; // 已有更新的计算，丢弃旧结果
        let rows = '', any = false;
        all.forEach(p => {
            const r = results[p.days];
            if (!r) {
                rows += `<tr><td class="px-4 py-3 border-b font-medium">${p.label}</td><td class="px-4 py-3 border-b text-center text-gray-400" colspan="3">数据不足</td></tr>`;
                return;
            }
            any = true;
            const retColor = r.avgReturn >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold';
            const retSign = r.avgReturn >= 0 ? '+' : '';
            const annColor = (!isFinite(r.avgAnnual)) ? 'text-gray-400'
                : (r.avgAnnual >= 0 ? 'text-red-600 font-semibold' : 'text-green-600 font-semibold');
            const annText = isFinite(r.avgAnnual) ? (r.avgAnnual >= 0 ? '+' : '') + r.avgAnnual.toFixed(2) + '%' : '—';
            const annNote = (!isFinite(r.avgAnnual) && r.annualSamples === 0) ? ' <span class="text-xs text-gray-400">(年化数据不足)</span>' : '';
            const wr = r.winRate;
            rows += `<tr>
                <td class="px-4 py-3 border-b font-medium text-center whitespace-nowrap">${p.label}</td>
                <td class="px-4 py-3 border-b text-center whitespace-nowrap ${retColor}">${retSign}${r.avgReturn.toFixed(2)}%</td>
                <td class="px-4 py-3 border-b text-center whitespace-nowrap ${annColor}">${annText}${annNote}</td>
                <td class="px-4 py-3 border-b">
                    <div class="flex items-center gap-2">
                        <div class="flex-1 bg-gray-100 rounded-full h-3 overflow-hidden">
                            <div class="h-3 rounded-full" style="width:${wr.toFixed(1)}%;background:linear-gradient(90deg,#3b82f6,#2563eb);"></div>
                        </div>
                        <span class="text-sm font-semibold text-blue-700 w-14 text-right">${wr.toFixed(1)}%</span>
                    </div>
                    <div class="text-xs text-gray-400 mt-1">样本数 ${r.samples}</div>
                </td>
            </tr>`;
        });
        if (!any) { sec.style.display = 'none'; return; }
        const modeLabel = profitMode === 'dca' ? '定投模拟（资金加权，按计划规则在历史任意时点起投）' : '组合净值（一次性买入，时间加权）';
        const ppDefaultWidths = ['14%', '14%', '16%'];
        const ppHeaders = ['持有时长', '平均收益', '年化收益率'];
        let ppHeadHtml = '';
        ppHeaders.forEach((h, i) => {
            const w = colWidths[i] ? colWidths[i] + 'px' : ppDefaultWidths[i];
            ppHeadHtml += `<th class="px-4 py-2 border-b text-center whitespace-nowrap" style="width:${w}">${h}</th>`;
        });
        ppHeadHtml += '<th class="px-4 py-2 border-b text-center">盈利概率</th>';
        tbl.innerHTML = '<table class="min-w-full bg-white border border-gray-200 rounded-lg text-sm" style="table-layout:fixed;width:100%"><thead class="bg-gray-100"><tr>' +
            ppHeadHtml + '</tr></thead><tbody>' +
            rows + '</tbody></table>' +
            `<p class="text-xs text-gray-400 mt-3">当前口径：${modeLabel}。盈利概率、平均收益与年化收益率均为历史业绩数据测算，不代表未来收益。</p>`;
        enableColumnResize(tbl.querySelector('table'));
        sec.style.display = 'block';
    });
}

// 前三列手动拖拽调宽（拖表头右边界）。colWidths 为 null 时用默认百分比，否则用已保存的像素宽。
let colWidths = [null, null, null];
function enableColumnResize(table) {
    if (!table) return;
    const ths = table.querySelectorAll('thead th');
    ths.forEach((th, idx) => {
        if (idx >= 3) return; // 仅前三列可拖拽
        th.style.position = 'relative';
        let handle = th.querySelector('.col-resize-handle');
        if (!handle) {
            handle = document.createElement('span');
            handle.className = 'col-resize-handle';
            handle.style.cssText = 'position:absolute;right:0;top:0;height:100%;width:7px;cursor:col-resize;user-select:none;z-index:5;';
            th.appendChild(handle);
        }
        handle.onmousedown = function (e) {
            e.preventDefault();
            e.stopPropagation();
            const startX = e.clientX;
            const startW = th.offsetWidth;
            th.style.width = startW + 'px'; // 锁定为像素，便于平滑拖拽
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            function onMove(ev) {
                const newW = Math.max(48, startW + (ev.clientX - startX));
                th.style.width = newW + 'px';
            }
            function onUp() {
                document.removeEventListener('mousemove', onMove);
                document.removeEventListener('mouseup', onUp);
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
                colWidths[idx] = th.offsetWidth; // 记存，重算后保持
            }
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup', onUp);
        };
    });
}

function addCustomPeriod() {
    const v = parseFloat(document.getElementById('customHoldYears').value);
    if (!(v > 0)) { alert('请输入大于 0 的持有年数'); return; }
    if (!customPeriods.includes(v)) customPeriods.push(v);
    renderProfitProbability();
}
function clearCustomPeriods() {
    customPeriods = [];
    renderProfitProbability();
}

// 基金简称截断（超长加省略号，全称通过 title 显示）
function shortName(name, max) {
    max = max || 8;
    if (!name) return '';
    return name.length > max ? name.slice(0, max) + '…' : name;
}

// 相关系数 -> 颜色（正相关红系、负相关绿系、0 附近白）
function corrColor(c) {
    if (c >= 0) {
        const t = c;
        const r = Math.round(255 - t * 39);
        const g = Math.round(255 - t * 207);
        const b = Math.round(255 - t * 216);
        return { bg: `rgb(${r},${g},${b})`, fg: t > 0.55 ? '#fff' : '#333' };
    } else {
        const t = -c;
        const r = Math.round(255 - t * 229);
        const g = Math.round(255 - t * 103);
        const b = Math.round(255 - t * 175);
        return { bg: `rgb(${r},${g},${b})`, fg: t > 0.55 ? '#fff' : '#333' };
    }
}

function pearson(a, b) {
    const n = a.length;
    let sa = 0, sb = 0;
    for (let i = 0; i < n; i++) { sa += a[i]; sb += b[i]; }
    const ma = sa / n, mb = sb / n;
    let cov = 0, va = 0, vb = 0;
    for (let i = 0; i < n; i++) {
        const da = a[i] - ma, db = b[i] - mb;
        cov += da * db; va += da * da; vb += db * db;
    }
    if (va === 0 || vb === 0) return NaN;
    return cov / Math.sqrt(va * vb);
}

function renderCorrelationMatrix() {
    const sec = document.getElementById('correlationSection');
    const tbl = document.getElementById('correlationTable');
    const legend = document.getElementById('correlationLegend');
    const codes = [...new Set(investmentPlans.map(p => p.fund))];
    if (codes.length === 0) { sec.style.display = 'none'; return; }

    // 每只基金：日期 -> 日收益率
    const retMaps = {};
    codes.forEach(code => {
        const f = fundsData[code];
        if (!f) return;
        const m = new Map();
        for (let i = 1; i < f.dates.length; i++) {
            const nav0 = f.nav[i - 1], nav1 = f.nav[i];
            if (nav0 > 0 && nav1 > 0) m.set(formatDate(f.dates[i]), nav1 / nav0 - 1);
        }
        retMaps[code] = m;
    });

    // 共同交易日交集
    let common = null;
    codes.forEach(code => {
        const m = retMaps[code];
        if (!m) return;
        const keys = new Set(m.keys());
        common = common === null ? keys : new Set([...common].filter(k => keys.has(k)));
    });
    if (!common || common.size < 3) { sec.style.display = 'none'; return; }

    const commonArr = Array.from(common).sort();
    const series = {};
    codes.forEach(code => {
        const m = retMaps[code];
        series[code] = commonArr.map(d => m.get(d));
    });

    const circle = ['①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨', '⑩'];
    const labels = codes.map(code => fundCodeName(code));

    let headerCells = '<th class="px-3 py-2 border-b text-center"></th>' +
        codes.map((code, i) => `<th class="px-3 py-2 border-b text-center" title="${labels[i].name}">${circle[i] || (i + 1)}<div class="text-xs font-normal text-gray-500">${shortName(labels[i].name)}</div></th>`).join('');

    let body = '';
    codes.forEach((codeA, i) => {
        let cells = `<td class="px-3 py-2 border-b text-center font-medium" title="${labels[i].name}">${circle[i] || (i + 1)} <span class="text-xs text-gray-500">${shortName(labels[i].name)}</span></td>`;
        codes.forEach((codeB, j) => {
            const c = (i === j) ? 1 : pearson(series[codeA], series[codeB]);
            if (isNaN(c)) { cells += '<td class="px-3 py-2 border-b text-center text-gray-400">-</td>'; return; }
            const col = corrColor(c);
            const txt = (c >= 0 ? '+' : '') + c.toFixed(2);
            cells += `<td class="px-3 py-2 border-b text-center font-medium" style="background:${col.bg};color:${col.fg};">${txt}</td>`;
        });
        body += `<tr>${cells}</tr>`;
    });

    tbl.innerHTML = '<table class="min-w-full bg-white border border-gray-200 rounded-lg text-sm"><thead class="bg-gray-100"><tr>' +
        headerCells + '</tr></thead><tbody>' + body + '</tbody></table>';
    legend.innerHTML = '<div class="flex items-center gap-3"><span class="text-sm font-medium text-gray-600">+1.00</span>' +
        '<div class="flex-1 h-4 rounded" style="background:linear-gradient(90deg, rgb(216,48,39), #ffffff, rgb(26,152,80));"></div>' +
        '<span class="text-sm font-medium text-gray-600">-1.00</span></div>' +
        '<div class="text-xs text-gray-400 mt-1">颜色越红代表正相关越强，越绿代表负相关越强。</div>';
    sec.style.display = 'block';
}

// 更新图表（基准对齐至组合起点）
async function updateCharts() {
    const startDate = new Date(document.getElementById('chartStartDate').value);
    const endDate = new Date(document.getElementById('chartEndDate').value);
    const filtered = backtestResult.dates.map((d,i)=>({date:d, asset:backtestResult.assets[i], nv:backtestResult.netValues[i]}))
        .filter(item => item.date >= startDate && item.date <= endDate);
    const chartDates = filtered.map(d => formatDate(d.date));
    const chartAssets = filtered.map(d => d.asset);
    const chartNetValues = filtered.map(d => d.nv);

    // 止盈触发标注：按日期聚合各基金止盈事件，与图表日期对齐
    const stopGainByDate = new Map();
    if (backtestResult.stopGainEvents && backtestResult.stopGainEvents.length) {
        backtestResult.stopGainEvents.forEach(function(e) {
            if (!stopGainByDate.has(e.dateStr)) stopGainByDate.set(e.dateStr, []);
            stopGainByDate.get(e.dateStr).push(e);
        });
    }
    const hasStopGain = stopGainByDate.size > 0;
    const netStopGainData = chartDates.map((ds, i) => stopGainByDate.has(ds) ? chartNetValues[i] : null);
    const assetStopGainData = chartDates.map((ds, i) => stopGainByDate.has(ds) ? chartAssets[i] : null);

    let benchmarkDataset = null;
    if (currentBenchmarkId && filtered.length > 0) {
        const benchmark = await db.benchmarks.get(currentBenchmarkId);
        if (benchmark && benchmark.data.length > 0) {
            const bmMap = new Map(benchmark.data.map(d => [d.date, d.nav]));
            
            // 构建原始基准净值序列（按图表日期）
            const rawBenchValues = [];
            for (let d of filtered.map(f=>f.date)) {
                const dateStr = formatDate(d);
                const nav = bmMap.get(dateStr);
                rawBenchValues.push(nav !== undefined ? nav : null);
            }
            
            // 前向填充缺失值
            for (let i = 1; i < rawBenchValues.length; i++) {
                if (rawBenchValues[i] === null) rawBenchValues[i] = rawBenchValues[i-1];
            }
            
            // 基准对齐到组合净值起点：寻找首个“组合净值有效 且 基准净值有效”的位置
            let alignIdx = -1, alignBenchNav = null, alignNetValue = null;
            for (let i = 0; i < rawBenchValues.length; i++) {
                if (rawBenchValues[i] !== null && chartNetValues[i] != null && chartNetValues[i] > 0) {
                    alignIdx = i; alignBenchNav = rawBenchValues[i]; alignNetValue = chartNetValues[i]; break;
                }
            }
            
            if (alignIdx !== -1 && alignBenchNav > 0) {
                // 在组合净值起点处，把基准缩放为与组合净值相等（基准去靠拢组合净值起点）
                const scale = alignNetValue / alignBenchNav;
                
                // 生成缩放后的基准序列（缺失值填 null，绘图时 Chart.js 会忽略）
                const scaledBenchValues = rawBenchValues.map(v => v !== null ? v * scale : null);
                
                benchmarkDataset = {
                    label: benchmark.name + ' (比较基准)',
                    data: scaledBenchValues,
                    borderColor: '#f97316',
                    backgroundColor: 'transparent',
                    borderDash: [5,5],
                    borderWidth: 2,
                    pointRadius: 0,
                    tension: 0.1,
                    spanGaps: false  // 不连接缺失段
                };
            }
        }
    }


    // 组合净值数据
    const startNetValue = chartNetValues.length > 0 ? chartNetValues[0] : 1.0;
    
    // 准备数据集（显式绑定 yAxisID）
    const netDatasets = [{
        label: '组合净值',
        data: chartNetValues,
        borderColor: '#10b981',
        backgroundColor: 'rgba(16,185,129,0.1)',
        fill: true,
        tension: 0.1,
        pointRadius: 0,
        pointHoverRadius: 6,
        yAxisID: 'y'
    }];
    
    if (benchmarkDataset) {
        benchmarkDataset.yAxisID = 'y';
        netDatasets.push(benchmarkDataset);
    }

    if (hasStopGain) {
        netDatasets.push({
            label: '止盈触发',
            data: netStopGainData,
            showLine: false,
            pointStyle: 'triangle',
            pointRadius: 7,
            pointHoverRadius: 9,
            backgroundColor: '#ef4444',
            borderColor: '#ef4444',
            yAxisID: 'y',
            isStopGainMarker: true
        });
    }

    // 计算所有数据的最小/最大值（用于右侧轴对齐）
    const allNumericValues = chartNetValues.concat(
        benchmarkDataset ? benchmarkDataset.data.filter(v => v !== null && !isNaN(v)) : []
    );
    const minVal = Math.min(...allNumericValues);
    const maxVal = Math.max(...allNumericValues);
    const padding = (maxVal - minVal) * 0.05; // 5% 边距

    // 销毁旧图表
    const netCtx = document.getElementById('netValueChart').getContext('2d');
    if (netValueChart) netValueChart.destroy();

    netValueChart = new Chart(netCtx, {
        type: 'line',
        data: { labels: chartDates, datasets: netDatasets },
        options: {
            responsive: true,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                tooltip: { 
                    mode: 'index',
                    callbacks: {
                        label: function(context) {
                            if (context.dataset.isStopGainMarker && context.parsed.y != null) {
                                const evs = stopGainByDate.get(chartDates[context.dataIndex]) || [];
                                let s = '止盈触发：';
                                evs.forEach(function(e, idx) {
                                    const cn = fundCodeName(e.fund);
                                    s += (idx > 0 ? '; ' : '') + cn.code + ' 赎回' + e.proceeds.toFixed(2) + '元(' + (e.ratio*100).toFixed(0) + '%)';
                                });
                                return s;
                            }
                            return context.dataset.label + ': ' + context.parsed.y.toFixed(4);// 显示精度到0.0001
                        }
                    }
                },
                legend: { labels: { usePointStyle: true } }
            },
            scales: {
                x: { bounds: 'data', offset: false, ticks: { maxTicksLimit: 15 } },
                y: {
                    beginAtZero: false,
                    position: 'left',
                    title: { 
                        display: true, 
                        text: `净值 (起始日 ${startNetValue.toFixed(4)})` 
                    },
                    min: minVal - padding,
                    max: maxVal + padding,
                    ticks: {
                        callback: function(value) {
                            return value.toFixed(4);
                        }
                    }
                },
                y1: {
                    position: 'right',
                    title: { display: true, text: '相对起始日涨跌 (%)' },
                    grid: { drawOnChartArea: false },
                    min: minVal - padding,
                    max: maxVal + padding,
                    ticks: {
                        callback: function(value) {
                            if (!startNetValue || startNetValue === 0) return '';
                            const percent = ((value - startNetValue) / startNetValue) * 100;
                            return percent.toFixed(1) + '%';
                        }
                    }
                }
            }
        }
    });

    const assetCtx = document.getElementById('assetChart').getContext('2d');
    if (assetChart) assetChart.destroy();
    const assetDatasets = [{
        label: '总资产', data: chartAssets, borderColor: '#3b82f6',
        backgroundColor: 'rgba(59,130,246,0.1)', fill: true, tension: 0.1, pointRadius: 0
    }];
    if (hasStopGain) {
        assetDatasets.push({
            label: '止盈触发',
            data: assetStopGainData,
            showLine: false,
            pointStyle: 'triangle',
            pointRadius: 7,
            pointHoverRadius: 9,
            backgroundColor: '#ef4444',
            borderColor: '#ef4444',
            isStopGainMarker: true
        });
    }
    assetChart = new Chart(assetCtx, {
        type: 'line', data: { labels: chartDates, datasets: assetDatasets },
        options: {
            responsive: true, interaction: { mode: 'index' },
            plugins: {
                tooltip: {
                    mode: 'index',
                    callbacks: {
                        label: function(context) {
                            if (context.dataset.isStopGainMarker && context.parsed.y != null) {
                                const evs = stopGainByDate.get(chartDates[context.dataIndex]) || [];
                                let s = '止盈触发：';
                                evs.forEach(function(e, idx) {
                                    const cn = fundCodeName(e.fund);
                                    s += (idx > 0 ? '; ' : '') + cn.code + ' 赎回' + e.proceeds.toFixed(2) + '元(' + (e.ratio*100).toFixed(0) + '%)';
                                });
                                return s;
                            }
                            return context.dataset.label + ': ' + context.parsed.y.toFixed(2) + ' 元';
                        }
                    }
                }
            },
            scales: { x: { bounds: 'data', offset: false, ticks: { maxTicksLimit: 15 } } }
        }
    });

    renderAnalysisTable();
    renderPeriodMetrics();
}

// 渲染「选定区间投资表现」指标卡（复用与整体相同的算法口径，仅作用于所选日期窗口）
function renderPeriodMetrics() {
    const section = document.getElementById('periodMetricsSection');
    const el = document.getElementById('periodMetrics');
    const labelEl = document.getElementById('periodRangeLabel');
    if (!el) return;
    if (!backtestResult.dates || backtestResult.dates.length === 0) { if (section) section.style.display = 'none'; return; }
    if (section) section.style.display = 'block';
    const startStr = document.getElementById('chartStartDate').value;
    const endStr = document.getElementById('chartEndDate').value;
    if (!startStr || !endStr) return;
    const startDate = new Date(startStr), endDate = new Date(endStr);
    const dates = backtestResult.dates;
    let i0 = -1, i1 = -1;
    for (let i = 0; i < dates.length; i++) {
        if (i0 === -1 && dates[i] >= startDate) i0 = i;
        if (dates[i] <= endDate) i1 = i;
    }
    if (i0 === -1) i0 = 0;
    if (i1 === -1) i1 = dates.length - 1;
    if (i1 < i0) i1 = i0;
    const nvs = backtestResult.netValues.slice(i0, i1 + 1);
    const assets = backtestResult.assets.slice(i0, i1 + 1);
    const invests = (backtestResult.invests || []).slice(i0, i1 + 1);
    const cashDivs = backtestResult.cashDivs || [];
    const wDates = dates.slice(i0, i1 + 1);
    const n = nvs.length;
    const windowDays = (dates[i1] - dates[i0]) / 86400000;

    // 快照类（区间口径）
    const intervalPrincipal = invests.reduce((a, b) => a + (b || 0), 0);
    const mvEnd = assets[n - 1] - (cashDivs[i1] || 0);
    const cashDivInterval = (cashDivs[i1] || 0) - (i0 > 0 ? (cashDivs[i0 - 1] || 0) : 0);
    const totalAssetEnd = assets[n - 1];

    // 收益类
    let cumReturn = NaN, xirrValInterval = NaN, annualReturnTwr = NaN, winRate = NaN;
    if (n >= 2 && nvs[0] > 0) {
        cumReturn = nvs[n - 1] / nvs[0] - 1;
        if (windowDays > 0) {
            annualReturnTwr = Math.pow(nvs[n - 1] / nvs[0], 365 / windowDays) - 1;
            // 区间 XIRR：首日资产为初始投入，区间内后续每笔定投为追加投入，末日资产为回收
            const flows = [ -assets[0] ], flowDates = [ dates[i0] ];
            for (let j = i0 + 1; j <= i1; j++) { flows.push(-invests[j - i0]); flowDates.push(dates[j]); }
            flows.push(assets[n - 1]); flowDates.push(dates[i1]);
            xirrValInterval = xirr(flows, flowDates) * 100;
        }
        const dailyReturns = [];
        for (let i = 1; i < n; i++) dailyReturns.push((nvs[i] - nvs[i - 1]) / nvs[i - 1]);
        winRate = dailyReturns.length ? dailyReturns.filter(r => r > 0).length / dailyReturns.length * 100 : NaN;
    }
    const twrHtml = isNaN(annualReturnTwr) ? '-' : (annualReturnTwr * 100).toFixed(2) + '%';
    const winHtml = isNaN(winRate) ? '-' : winRate.toFixed(1) + '%';
    const cumHtml = isNaN(cumReturn) ? '-' : (cumReturn * 100).toFixed(2) + '%';

    // 风险类
    let riskHtml;
    if (n >= MIN_TRADE_DAYS) {
        const dailyReturns = [];
        for (let i = 1; i < n; i++) dailyReturns.push((nvs[i] - nvs[i - 1]) / nvs[i - 1]);
        const mean = dailyReturns.reduce((a, b) => a + b, 0) / dailyReturns.length;
        const variance = dailyReturns.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / dailyReturns.length;
        const annualVolatility = Math.sqrt(variance) * Math.sqrt(252);
        let peak = nvs[0], maxDrawdown = 0;
        for (const v of nvs) { if (v > peak) peak = v; const dd = (v - peak) / peak; if (dd < maxDrawdown) maxDrawdown = dd; }
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
        riskHtml = `
            <div class="bg-red-50 p-4 rounded-lg text-center" data-mkey="最大回撤"><div class="text-sm text-slate-500">最大回撤</div><div class="text-2xl font-bold text-red-700">${maxDrawdown.toFixed(2)}%</div></div>
            <div class="bg-red-50 p-4 rounded-lg text-center" data-mkey="回撤持续天数"><div class="text-sm text-slate-500">回撤持续天数</div><div class="text-2xl font-bold text-red-700">${maxSpan.toFixed(0)} 天</div></div>
            <div class="bg-red-50 p-4 rounded-lg text-center" data-mkey="年化波动率"><div class="text-sm text-slate-500">年化波动率</div><div class="text-2xl font-bold text-red-700">${(annualVolatility * 100).toFixed(2)}%</div></div>
            <div class="bg-red-50 p-4 rounded-lg text-center" data-mkey="夏普/卡玛"><div class="text-sm text-slate-500">夏普/卡玛</div><div class="text-2xl font-bold text-red-700">${isNaN(sharpeRatio) ? '-' : sharpeRatio.toFixed(2)}/${isNaN(calmarRatio) ? '-' : calmarRatio.toFixed(2)}</div></div>
        `;
    } else {
        riskHtml = `<div class="bg-gray-100 p-4 rounded-lg text-center col-span-4" data-mkey="风险指标"><div class="text-sm text-gray-600">风险指标</div><div class="text-xl font-medium text-gray-500">区间交易日不足${MIN_TRADE_DAYS}个，以下指标暂不可用</div></div>`;
    }

    el.innerHTML = `
        <div class="bg-sky-50 p-4 rounded-lg text-center" data-mkey="区间投入本金"><div class="text-sm text-slate-500">区间投入本金</div><div class="text-2xl font-bold text-sky-700">${intervalPrincipal.toFixed(2)} 元</div></div>
        <div class="bg-sky-50 p-4 rounded-lg text-center" data-mkey="区间期末市值"><div class="text-sm text-slate-500">区间期末市值</div><div class="text-2xl font-bold text-sky-700">${mvEnd.toFixed(2)} 元</div></div>
        <div class="bg-sky-50 p-4 rounded-lg text-center" data-mkey="区间现金分红"><div class="text-sm text-slate-500">区间现金分红</div><div class="text-2xl font-bold text-sky-700">${cashDivInterval.toFixed(2)} 元</div></div>
        <div class="bg-sky-50 p-4 rounded-lg text-center" data-mkey="区间期末总资产"><div class="text-sm text-slate-500">区间期末总资产</div><div class="text-2xl font-bold text-sky-700">${totalAssetEnd.toFixed(2)} 元</div></div>

        <div class="bg-green-50 p-4 rounded-lg text-center" data-mkey="区间累计收益率"><div class="text-sm text-slate-500">区间累计收益率</div><div class="text-2xl font-bold text-green-700">${cumHtml}</div></div>
        <div class="bg-green-50 p-4 rounded-lg text-center" data-mkey="区间XIRR年化"><div class="text-sm text-slate-500">区间XIRR年化</div><div class="text-2xl font-bold text-green-700">${isNaN(xirrValInterval) ? '-' : xirrValInterval.toFixed(2) + '%'}</div></div>
        <div class="bg-green-50 p-4 rounded-lg text-center" data-mkey="区间年化收益率(时间加权)"><div class="text-sm text-slate-500">区间年化收益率(时间加权)</div><div class="text-2xl font-bold text-green-700">${twrHtml}</div></div>
        <div class="bg-green-50 p-4 rounded-lg text-center" data-mkey="区间胜率(正收益日占比)"><div class="text-sm text-slate-500">区间胜率(正收益日占比)</div><div class="text-2xl font-bold text-green-700">${winHtml}</div></div>

        ${riskHtml}
    `;
    if (labelEl) labelEl.textContent = `（${startStr} ~ ${endStr}，共 ${n} 个交易日）`;
}

// ============ 指标卡悬停解释 ============
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
    // 注入浮层样式
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

    ['metrics', 'periodMetrics'].forEach(id => {
        const box = document.getElementById(id);
        if (!box) return;
        box.addEventListener('mouseover', e => {
            const cell = e.target.closest('[data-mkey]');
            if (!cell) { tip.style.display = 'none'; return; }
            const key = cell.dataset.mkey;
            const txt = METRIC_TIPS[key];
            if (!txt) { tip.style.display = 'none'; return; }
            tip.innerHTML = '<b>' + key + '</b><br>' + txt;
            tip.style.display = 'block';
        });
        box.addEventListener('mousemove', e => {
            if (tip.style.display === 'block') {
                let x = e.clientX + 14, y = e.clientY + 14;
                const r = tip.getBoundingClientRect();
                if (x + r.width > window.innerWidth) x = e.clientX - r.width - 14;
                if (y + r.height > window.innerHeight) y = e.clientY - r.height - 14;
                tip.style.left = x + 'px';
                tip.style.top = y + 'px';
            }
        });
        box.addEventListener('mouseout', e => {
            if (!e.relatedTarget || !e.relatedTarget.closest('[data-mkey]')) tip.style.display = 'none';
        });
    });
}

// 绑定事件（放在页面加载完成后）
document.addEventListener('DOMContentLoaded', function() {
    const clearBtn = document.getElementById('clearAllBenchmarksBtn');
    if (clearBtn) {
        clearBtn.addEventListener('click', clearAllBenchmarks);
    }
    initMetricTooltip();
});

// 渲染关键节点分析表格（修正版：四个固定节点）
async function renderAnalysisTable() {
    const validDates = backtestResult.dates;
    const netValues = backtestResult.netValues;
    if (validDates.length === 0 || netValues.length === 0) return;

    // 1. 获取四个关键日期
    const firstInvestmentDate = validDates[0]; // 第一笔投资日
    const latestPortfolioDate = validDates[validDates.length-1]; // 组合最新日
    
    const chartStartInput = document.getElementById('chartStartDate');
    const chartEndInput = document.getElementById('chartEndDate');
    const chartStartDate = chartStartInput?.value ? new Date(chartStartInput.value) : null;
    const chartEndDate = chartEndInput?.value ? new Date(chartEndInput.value) : null;
    
    // 确保图表日期在有效范围内
    const adjustedChartStartDate = chartStartDate && chartStartDate >= validDates[0] && chartStartDate <= validDates[validDates.length-1] 
        ? chartStartDate : validDates[0];
    const adjustedChartEndDate = chartEndDate && chartEndDate >= validDates[0] && chartEndDate <= validDates[validDates.length-1] 
        ? chartEndDate : validDates[validDates.length-1];

    // 2. 获取选中的基准数据
    let benchmarkData = null;
    if (currentBenchmarkId) {
        const bm = await db.benchmarks.get(currentBenchmarkId);
        benchmarkData = bm ? bm.data : null;
    }

    // 辅助函数：在 validDates 中找到与targetDate最接近的索引
    const getIndexForDate = (targetDate) => {
        if (!targetDate) return -1;
        // 找到小于等于targetDate的最大日期索引
        for (let i = validDates.length - 1; i >= 0; i--) {
            if (validDates[i] <= targetDate) return i;
        }
        return 0; // 如果所有日期都大于targetDate，返回第一个
    };

    // 3. 构建四个节点
    const nodeConfigs = [
        { 
            name: '第一笔投资', 
            date: firstInvestmentDate,
            getIndex: () => 0 // 总是第一个索引
        },
        { 
            name: '图表开始', 
            date: adjustedChartStartDate,
            getIndex: () => getIndexForDate(adjustedChartStartDate)
        },
        { 
            name: '图表结束', 
            date: adjustedChartEndDate,
            getIndex: () => getIndexForDate(adjustedChartEndDate)
        },
        { 
            name: '组合最新', 
            date: latestPortfolioDate,
            getIndex: () => validDates.length - 1 // 总是最后一个索引
        }
    ];

    // 4. 计算节点索引，处理重叠
    const nodes = [];
    const seenIndices = new Set();
    
    for (const config of nodeConfigs) {
        const idx = config.getIndex();
        if (idx >= 0 && idx < validDates.length) {
            if (seenIndices.has(idx)) {
                // 如果索引已存在，合并节点名称
                const existingNode = nodes.find(n => n.idx === idx);
                if (existingNode) {
                    existingNode.names.push(config.name);
                }
            } else {
                nodes.push({
                    idx: idx,
                    date: validDates[idx],
                    names: [config.name]
                });
                seenIndices.add(idx);
            }
        }
    }

    // 按时间顺序排序节点
    nodes.sort((a, b) => a.idx - b.idx);

    if (nodes.length === 0) return;

    // 5. 计算基准归一化参数
    let bmMap = null;
    let firstValidBenchNav = null;
    let firstValidBenchIdx = -1;
    
    if (benchmarkData) {
        bmMap = new Map(benchmarkData.map(d => [d.date, d.nav]));
        
        // 找到第一个有效的基准净值（在validDates范围内）
        for (let i = 0; i < validDates.length; i++) {
            const dateStr = formatDate(validDates[i]);
            const nav = bmMap.get(dateStr);
            if (nav !== undefined) {
                firstValidBenchNav = nav;
                firstValidBenchIdx = i;
                break;
            }
        }
    }

    // 投资最早日期的组合净值作为累计收益基准
    const baseIdx = nodes[0].idx;
    const baseComboNav = netValues[baseIdx];

    // 计算基准在 baseIdx 处的对齐净值
    let baseBenchNav = null;
    if (bmMap && firstValidBenchNav !== null && firstValidBenchIdx >= 0) {
        const scale = baseComboNav / firstValidBenchNav;
        const baseDateStr = formatDate(validDates[baseIdx]);
        const rawNav = bmMap.get(baseDateStr);
        // 如果baseIdx处有基准数据，直接使用；否则通过前向填充获取
        if (rawNav !== undefined) {
            baseBenchNav = rawNav * scale;
        } else {
            // 前向填充：找到小于等于baseIdx的最近基准数据
            for (let i = baseIdx; i >= 0; i--) {
                const dateStr = formatDate(validDates[i]);
                const nav = bmMap.get(dateStr);
                if (nav !== undefined) {
                    baseBenchNav = nav * scale;
                    break;
                }
            }
        }
    }

    // 6. 逐行计算数据
    const rows = [];
    for (let i = 0; i < nodes.length; i++) {
        const node = nodes[i];
        const idx = node.idx;
        const dateStr = formatDate(node.date);
        const comboNav = netValues[idx];

        // 基准净值（对齐后）
        let benchNav = null;
        if (bmMap && firstValidBenchNav !== null && firstValidBenchIdx >= 0) {
            const scale = baseComboNav / firstValidBenchNav;
            const rawNav = bmMap.get(dateStr);
            if (rawNav !== undefined) {
                benchNav = rawNav * scale;
            } else {
                // 前向填充
                for (let j = idx; j >= 0; j--) {
                    const fillDateStr = formatDate(validDates[j]);
                    const fillNav = bmMap.get(fillDateStr);
                    if (fillNav !== undefined) {
                        benchNav = fillNav * scale;
                        break;
                    }
                }
            }
        }

        // 累计收益（相对于第一个节点）
        const comboAccReturn = baseComboNav > 0 ? (comboNav / baseComboNav - 1) * 100 : 0;
        const benchAccReturn = (benchNav !== null && baseBenchNav !== null && baseBenchNav > 0) 
            ? (benchNav / baseBenchNav - 1) * 100 : null;
        const excessReturn = benchAccReturn !== null ? comboAccReturn - benchAccReturn : null;

        // 阶段收益（相对于上一节点）
        let stageCombo = null, stageBench = null;
        if (i > 0) {
            const prevNode = nodes[i-1];
            const prevIdx = prevNode.idx;
            const prevCombo = netValues[prevIdx];
            stageCombo = prevCombo > 0 ? (comboNav / prevCombo - 1) * 100 : null;

            if (benchmarkData && benchNav !== null) {
                const prevDateStr = formatDate(validDates[prevIdx]);
                const prevRawNav = bmMap.get(prevDateStr);
                let prevBenchNav = null;
                if (prevRawNav !== undefined) {
                    prevBenchNav = prevRawNav * (baseComboNav / firstValidBenchNav);
                } else {
                    // 前向填充
                    for (let j = prevIdx; j >= 0; j--) {
                        const fillDateStr = formatDate(validDates[j]);
                        const fillNav = bmMap.get(fillDateStr);
                        if (fillNav !== undefined) {
                            prevBenchNav = fillNav * (baseComboNav / firstValidBenchNav);
                            break;
                        }
                    }
                }
                stageBench = (prevBenchNav && prevBenchNav > 0) ? (benchNav / prevBenchNav - 1) * 100 : null;
            }
        }

        rows.push({
            name: node.names.join('/'), // 如果有重叠，用斜杠分隔
            date: dateStr,
            comboNav: comboNav.toFixed(4),
            benchNav: benchNav !== null ? benchNav.toFixed(4) : '-',
            comboAcc: comboAccReturn.toFixed(2) + '%',
            benchAcc: benchAccReturn !== null ? benchAccReturn.toFixed(2) + '%' : '-',
            excess: excessReturn !== null ? excessReturn.toFixed(2) + '%' : '-',
            stageCombo: stageCombo !== null ? stageCombo.toFixed(2) + '%' : '-',
            stageBench: stageBench !== null ? stageBench.toFixed(2) + '%' : '-'
        });
    }

    // 7. 渲染表格
    const tbody = document.getElementById('analysisTableBody');
    if (!tbody) return;
    
    tbody.innerHTML = rows.map(r => `
        <tr class="hover:bg-gray-50">
            <td class="px-4 py-2 border-b text-center font-medium">${r.name}</td>
            <td class="px-4 py-2 border-b text-center font-mono">${r.date}</td>
            <td class="px-4 py-2 border-b text-center">${r.comboNav}</td>
            <td class="px-4 py-2 border-b text-center">${r.benchNav}</td>
            <td class="px-4 py-2 border-b text-center ${parseFloat(r.comboAcc) >= 0 ? 'text-green-600' : 'text-red-600'}">${r.comboAcc}</td>
            <td class="px-4 py-2 border-b text-center ${r.benchAcc !== '-' && parseFloat(r.benchAcc) >= 0 ? 'text-green-600' : 'text-red-600'}">${r.benchAcc}</td>
            <td class="px-4 py-2 border-b text-center ${r.excess !== '-' && parseFloat(r.excess) >= 0 ? 'text-green-600' : 'text-red-600'}">${r.excess}</td>
            <td class="px-4 py-2 border-b text-center ${r.stageCombo !== '-' && parseFloat(r.stageCombo) >= 0 ? 'text-green-600' : 'text-red-600'}">${r.stageCombo}</td>
            <td class="px-4 py-2 border-b text-center ${r.stageBench !== '-' && parseFloat(r.stageBench) >= 0 ? 'text-green-600' : 'text-red-600'}">${r.stageBench}</td>
        </tr>
    `).join('');
}

