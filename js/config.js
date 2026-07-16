/* config.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// ---------- 全局变量 ----------
let fundsData = {};
let investmentPlans = [];
let fillMissingNav = false;   // 是否对空白净值做前向填充（按前一交易日净值模拟）
let customPeriods = [];       // 盈利概率自定义持有期（年）
let profitMode = 'dca';       // 盈利概率口径：'dca'=定投模拟(资金加权) / 'twr'=组合净值(一次性买入)
let assetChart = null;
let netValueChart = null;
//let compositeWeights = {}; // 临时存储各基准权重 { benchmarkId: weight }
const wdNames = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
const RISK_FREE_RATE = 0.025;
const MIN_TRADE_DAYS = 30;

let backtestResult = { dates: [], assets: [], netValues: [] };

// IndexedDB
const db = new Dexie('BenchmarkDB');
db.version(1).stores({ benchmarks: '++id, name' });
db.version(2).stores({ benchmarks: '++id, name', funds: 'code' });  // 新增 funds 表：本地基金持久化
let currentBenchmarkId = null;
let compositeWeights = {}; // 临时存储各基准权重 { benchmarkId: weight }

