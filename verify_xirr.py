# 复刻 定投测算（净值）.html 的目标止盈定投现金流，并输出供 Excel 核对
import json, datetime

# ---------- 参数（请按你当时的设置修改） ----------
PARAMS = {
    'fundContains': '008114',   # 基金筛选
    'strategy': '5',            # 目标止盈定投
    'freq': 'monthly',
    'dayOfMonth': 'first',      # 每月首个交易日
    'baseAmount': 1000,
    'stopGainPct': 8,           # 止盈阈值 8%
    'stopGainSellRatio': 100,   # 赎回比例 100%
    'startDate': '2019-12-10',
    'endDate': '2026-07-07',
    'div': 'reinvest'           # 红利再投资
}

def parse(d): return datetime.datetime.strptime(d, '%Y-%m-%d')

# ---------- xirr（与 backtest.js 一致：dayFrac=(date-base)/365天，牛顿初值0.1，失败则二分） ----------
def xirr(cash_flows, flow_dates, guess=0.1):
    if len(cash_flows) != len(flow_dates) or len(cash_flows) < 2:
        return float('nan')
    paired = sorted(zip(flow_dates, cash_flows), key=lambda p: p[0])
    sdates = [p[0] for p in paired]
    sflows = [p[1] for p in paired]
    base = sdates[0]
    def dayfrac(j): return (sdates[j] - base).days / 365.0
    def npv(rate):
        return sum(sflows[j] / (1 + rate) ** dayfrac(j) for j in range(len(sflows)))
    tol = 1e-7
    rate = guess
    for _ in range(100):
        v = 0.0; d = 0.0
        for j in range(len(sflows)):
            f = dayfrac(j); term = (1 + rate) ** f
            v += sflows[j] / term
            d -= sflows[j] * f * (1 + rate) ** (f - 1)
        if abs(v) < tol: return rate
        if abs(d) < tol: break
        rate -= v / d
    lo, hi = -0.9999, 100.0
    fLo, fHi = npv(lo), npv(hi)
    if fLo * fHi > 0: return float('nan')
    for _ in range(200):
        mid = (lo + hi) / 2; fMid = npv(mid)
        if abs(fMid) < tol: return mid
        if fLo * fMid < 0: hi = mid
        else: lo = mid
    return (lo + hi) / 2

# ---------- 读取基金数据 ----------
raw = json.load(open('回测项目_2026-07-09.json', encoding='utf-8'))
funds = raw['fundsData']
fkey = [k for k in funds if PARAMS['fundContains'] in k][0]
fund = funds[fkey]
print('基金:', fkey, ' 交易日数:', len(fund['dates']))

st = parse(PARAMS['startDate']); en = parse(PARAMS['endDate'])
dates, date_strs, navs, divs = [], [], [], []
for i in range(len(fund['dates'])):
    d = parse(fund['dates'][i])
    if st <= d <= en:
        dates.append(d); date_strs.append(fund['dates'][i])
        navs.append(fund['nav'][i]); divs.append(fund['div'][i] or 0)
N = len(dates)
print('区间交易日数:', N)

dow = [d.weekday() for d in dates]            # Mon=0..Sun=6
dom = [int(s.split('-')[2]) for s in date_strs]

reinvest = PARAMS['div'] == 'reinvest'
strat = PARAMS['strategy']
is_invest = [False] * N
if strat == '1':
    is_invest[0] = True
else:
    freq = PARAMS['freq']
    if freq in ('weekly', 'biweekly'):
        wd = int(PARAMS.get('weekday', 1)) - 1   # 转成 Mon=0
        first = -1
        for k in range(N):
            if dow[k] == wd: first = k; break
        if first >= 0:
            for k in range(first, N):
                if dow[k] == wd:
                    if freq == 'biweekly':
                        if round((dates[k] - dates[first]).days / 7) % 2 != 0: continue
                    is_invest[k] = True
    else:
        if PARAMS['dayOfMonth'] == 'first':
            last = None
            for k in range(N):
                ym = date_strs[k][:7]
                if ym != last: is_invest[k] = True; last = ym
        else:
            domN = int(PARAMS['dayOfMonth'])
            for k in range(N):
                if dom[k] == domN: is_invest[k] = True

invest_cnt = sum(is_invest)
print('定投次数:', invest_cnt)

shares = 0.0; total_cash = 0.0; total_invested = 0.0; cost_basis = 0.0
stop_gain = 0; total_redeemed = 0.0; total_div_cash = 0.0
run_principal = 0.0; max_principal = 0.0; run_max_principal = 0.0
cash_flows, flow_dates = [], []

for k in range(N):
    nav = navs[k]; date = dates[k]
    if shares > 0 and divs[k] > 0:
        total_div = shares * divs[k]
        if reinvest: shares += total_div / nav
        else:
            total_cash += total_div; total_div_cash += total_div
            cash_flows.append(total_div); flow_dates.append(date)
    amt = 0.0
    if is_invest[k]:
        if strat in ('1', '2', '4', '5', '6'): amt = PARAMS['baseAmount']
        elif strat == '3':
            invest_cnt += 1; amt = invest_cnt * PARAMS['baseAmount'] - shares * nav
    if strat == '5':
        th = float(PARAMS['stopGainPct']) / 100
        sell_ratio = min(1.0, max(0.0, float(PARAMS['stopGainSellRatio']) / 100))
        round_principal = run_max_principal
        if round_principal > 0 and sell_ratio > 0:
            hold_mv = shares * nav
            if (hold_mv - cost_basis) / round_principal >= th:
                sell_shares = shares * sell_ratio
                proceeds = sell_shares * nav
                shares -= sell_shares; total_cash += proceeds; total_redeemed += proceeds
                cash_flows.append(proceeds); flow_dates.append(date)
                cost_basis *= (1 - sell_ratio)
                stop_gain += 1
                if sell_ratio >= 1 or shares < 1e-9:
                    run_principal = 0.0; run_max_principal = 0.0
    if amt != 0 and is_invest[k]:
        if amt > 0:
            shares += amt / nav; total_invested += amt; cost_basis += amt; run_principal += amt
            cash_flows.append(-amt); flow_dates.append(date)
        else:
            sell_shares = (-amt) / nav
            if sell_shares > shares: sell_shares = shares
            if sell_shares > 0:
                proceeds = sell_shares * nav
                shares -= sell_shares; total_cash += proceeds; total_redeemed += proceeds
                cash_flows.append(proceeds); flow_dates.append(date)
                denom = shares + sell_shares
                if denom > 0: cost_basis *= shares / denom
    run_max_principal = max(run_max_principal, run_principal)
    max_principal = max(max_principal, run_principal)

final_date = dates[N - 1]
cash_flows.append(shares * navs[N - 1]); flow_dates.append(final_date)

xirr_val = xirr(cash_flows, flow_dates)
final_asset = shares * navs[N - 1] + total_cash

print('\n===== 摘要 =====')
print('总投入本金 totalInvested     =', round(total_invested))
print('期末总资产 finalAsset        =', round(final_asset))
print('累积赎回 totalRedeemed       =', round(total_redeemed))
print('累计现金分红                 =', round(total_div_cash))
print('止盈次数 stopGainCount       =', stop_gain)
print('最大投入本金 maxPrincipal    =', round(max_principal))
print('期末剩余份额市值             =', round(shares * navs[N - 1]))
print('代码 xirr(牛顿 guess=0.1)    =', 'NaN' if xirr_val != xirr_val else f'{xirr_val*100:.4f}%')

# ---------- 扫描所有实根 ----------
def npv_rate(rate):
    paired = sorted(zip(flow_dates, cash_flows), key=lambda p: p[0])
    sd = [p[0] for p in paired]; sf = [p[1] for p in paired]
    bd = sd[0]
    return sum(sf[j] / (1 + rate) ** ((sd[j] - bd).days / 365.0) for j in range(len(sf)))

roots = []
prev_r = -0.9999; prev_v = npv_rate(prev_r)
r = -0.9999
while r <= 100.0:
    r += 0.0005
    v = npv_rate(r)
    if prev_v * v < 0:
        lo, hi = prev_r, r
        for _ in range(60):
            mid = (lo + hi) / 2
            if npv_rate(lo) * npv_rate(mid) <= 0: hi = mid
            else: lo = mid
        roots.append((lo + hi) / 2)
    prev_r, prev_v = r, v

print('\n===== 所有实根 (NPV=0 的 r) =====')
if not roots: print('无实根（区间内 NPV 不变号）')
else:
    for i, rt in enumerate(roots): print(f'根{i+1}: {rt*100:.4f}%')

# ---------- 现金流表（供 Excel 粘贴，日期<TAB>金额） ----------
print('\n===== 现金流表 (日期<TAB>金额) 复制粘贴到 Excel 两列 =====')
paired = sorted(zip(flow_dates, cash_flows), key=lambda p: p[0])
out = ''
for d, cf in paired:
    out += d.strftime('%Y-%m-%d') + '\t' + f'{cf:.2f}\n'
print(out)
open('cashflow_008114_stopgain8.txt', 'w', encoding='utf-8').write(out)
print('(已同时保存到 cashflow_008114_stopgain8.txt)')
