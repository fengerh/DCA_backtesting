// 复刻 定投测算（净值）.html 的目标止盈定投现金流，并输出供 Excel 核对
const fs = require('fs');

// ---------- 参数（请按你当时的设置修改） ----------
const PARAMS = {
  fundContains: '008114',     // 基金筛选
  strategy: '5',              // 目标止盈定投
  freq: 'monthly',
  dayOfMonth: '1',            // 每月1号
  baseAmount: 1000,
  stopGainPct: 8,             // 止盈阈值 8%
  stopGainSellRatio: 100,     // 赎回比例 100%
  startDate: '2019-12-10',
  endDate: '2026-07-07',
  div: 'reinvest'             // 红利再投资
};

// ---------- 工具函数（与代码一致） ----------
function formatDate(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}
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

// ---------- 读取基金数据 ----------
const raw = JSON.parse(fs.readFileSync('回测项目_2026-07-09.json', 'utf8'));
const fundsData = raw.fundsData;
const fkey = Object.keys(fundsData).find(k => k.includes(PARAMS.fundContains));
if (!fkey) { console.error('未找到基金:', PARAMS.fundContains); process.exit(1); }
const fund = fundsData[fkey];
console.log('基金:', fkey, ' 交易日数:', fund.dates.length);

const startTs = new Date(PARAMS.startDate + 'T00:00:00').getTime();
const endTs = new Date(PARAMS.endDate + 'T00:00:00').getTime();

const dates = [], dateStrs = [], navs = [], divs = [];
for (let i = 0; i < fund.dates.length; i++) {
  const ts = new Date(fund.dates[i] + 'T00:00:00').getTime();
  if (ts >= startTs && ts <= endTs) {
    dates.push(new Date(fund.dates[i] + 'T00:00:00'));
    dateStrs.push(fund.dates[i]);
    navs.push(fund.nav[i]);
    divs.push(fund.div[i] || 0);
  }
}
const N = dates.length;
console.log('区间交易日数:', N);

const dow = new Array(N), dom = new Array(N);
for (let k = 0; k < N; k++) { dow[k] = dates[k].getUTCDay(); dom[k] = parseInt(dateStrs[k].split('-')[2], 10); }

const reinvest = PARAMS.div === 'reinvest';
const strat = PARAMS.strategy;
const isInvest = new Array(N).fill(false);
if (strat === '1') isInvest[0] = true;
else {
  const freq = PARAMS.freq;
  if (freq === 'weekly' || freq === 'biweekly') {
    const wd = parseInt(PARAMS.weekday || '1');
    let firstIdx = -1;
    for (let k = 0; k < N; k++) { if (dow[k] === wd) { firstIdx = k; break; } }
    if (firstIdx >= 0) for (let k = firstIdx; k < N; k++) {
      if (dow[k] === wd) {
        if (freq === 'biweekly') { const wd2 = Math.round((dates[k] - dates[firstIdx]) / (7 * 86400000)); if (wd2 % 2 !== 0) continue; }
        isInvest[k] = true;
      }
    }
  } else {
    if (PARAMS.dayOfMonth === 'first') {
      let lastMonth = -1;
      for (let k = 0; k < N; k++) { const ym = dateStrs[k].slice(0, 7); if (ym !== lastMonth) { isInvest[k] = true; lastMonth = ym; } }
    } else {
      const domN = parseInt(PARAMS.dayOfMonth);
      for (let k = 0; k < N; k++) if (dom[k] === domN) isInvest[k] = true;
    }
  }
}
const investIdx = [];
for (let k = 0; k < N; k++) if (isInvest[k]) investIdx.push(k);
console.log('定投次数:', investIdx.length);

let shares = 0, totalCash = 0, totalInvested = 0, costBasis = 0, stopGainCount = 0, investCount = 0, totalRedeemed = 0, totalDividendCash = 0;
let runPrincipal = 0, maxPrincipal = 0, runMaxPrincipal = 0;
const cashFlows = [], flowDates = [];

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
    if (strat === '1') amt = PARAMS.baseAmount;
    else if (strat === '2') amt = PARAMS.baseAmount;
    else if (strat === '3') { investCount++; amt = investCount * PARAMS.baseAmount - shares * nav; }
    else if (strat === '4') amt = PARAMS.baseAmount;
    else if (strat === '5') amt = PARAMS.baseAmount;
    else if (strat === '6') amt = PARAMS.baseAmount;
  }
  if (strat === '5') {
    const th = parseFloat(PARAMS.stopGainPct) / 100;
    const sellRatio = Math.min(1, Math.max(0, parseFloat(PARAMS.stopGainSellRatio) / 100));
    const roundPrincipal = runMaxPrincipal;
    if (roundPrincipal > 0 && sellRatio > 0) {
      const holdMv = shares * nav;
      if ((holdMv - costBasis) / roundPrincipal >= th) {
        const sellShares = shares * sellRatio;
        const proceeds = sellShares * nav;
        shares -= sellShares; totalCash += proceeds; totalRedeemed += proceeds;
        cashFlows.push(proceeds); flowDates.push(date);
        costBasis *= (1 - sellRatio);
        stopGainCount++;
        if (sellRatio >= 1 || shares < 1e-9) { runPrincipal = 0; runMaxPrincipal = 0; }
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
  runMaxPrincipal = Math.max(runMaxPrincipal, runPrincipal);
  maxPrincipal = Math.max(maxPrincipal, runPrincipal);
}

const finalDate = dates[N - 1];
cashFlows.push(shares * navs[N - 1]); flowDates.push(finalDate);

const xirrVal = xirr(cashFlows, flowDates) * 100;
const finalAsset = shares * navs[N - 1] + totalCash;

// ---------- 输出摘要 ----------
console.log('\n===== 摘要 =====');
console.log('总投入本金 totalInvested =', totalInvested.toFixed(0));
console.log('期末总资产 finalAsset   =', finalAsset.toFixed(0));
console.log('累积赎回 totalRedeemed  =', totalRedeemed.toFixed(0));
console.log('累计现金分红            =', totalDividendCash.toFixed(0));
console.log('止盈次数 stopGainCount  =', stopGainCount);
console.log('最大投入本金 maxPrincipal =', maxPrincipal.toFixed(0));
console.log('期末剩余份额市值        =', (shares * navs[N-1]).toFixed(0));
console.log('代码 xirr(牛顿guess=0.1) =', (isNaN(xirrVal)?'NaN':xirrVal.toFixed(4)+'%'));

// ---------- 扫描所有实根（检测伪根） ----------
function npvRate(rate) {
  const paired = cashFlows.map((cf, i) => ({ cf, date: flowDates[i] }));
  paired.sort((a, b) => a.date - b.date);
  const sf = paired.map(p => p.cf), sd = paired.map(p => p.date);
  const bd = sd[0];
  let s = 0;
  for (let j = 0; j < sf.length; j++) s += sf[j] / Math.pow(1 + rate, (sd[j] - bd) / 86400000 / 365);
  return s;
}
const roots = [];
let prevR = -0.9999, prevV = npvRate(prevR);
for (let r = -0.9999 + 0.0001; r <= 100; r += 0.0001) {
  const v = npvRate(r);
  if (prevV === 0) { /* */ }
  if (prevV * v < 0) {
    // 细化
    let lo = prevR, hi = r;
    for (let it = 0; it < 60; it++) { const mid = (lo+hi)/2; if (npvRate(lo)*npvRate(mid) <= 0) hi = mid; else lo = mid; }
    roots.push((lo+hi)/2);
  }
  prevR = r; prevV = v;
}
console.log('\n===== 所有实根 (NPV=0 的 r) =====');
if (roots.length === 0) console.log('无实根 (区间内 NPV 不变号)');
else roots.forEach((r, i) => console.log(`根${i+1}: ${(r*100).toFixed(4)}%`));

// ---------- 输出现金流表（供 Excel 粘贴） ----------
console.log('\n===== 现金流表 (日期<TAB>金额) 复制粘贴到 Excel 两列 =====');
// 按日期排序便于核对
const paired = cashFlows.map((cf, i) => ({ d: flowDates[i], cf }));
paired.sort((a, b) => a.d - b.d);
let out = '';
paired.forEach(p => { out += formatDate(p.d) + '\t' + p.cf.toFixed(2) + '\n'; });
console.log(out);
fs.writeFileSync('cashflow_008114_stopgain8.txt', out);
console.log('(已同时保存到 cashflow_008114_stopgain8.txt)');
