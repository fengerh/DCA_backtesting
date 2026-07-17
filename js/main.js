/* main.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// 工具栏事件绑定
document.getElementById('exportProjectBtn').addEventListener('click', exportProject);
document.getElementById('importProjectBtn').addEventListener('click', function () { document.getElementById('importProjectFile').click(); });
document.getElementById('importProjectFile').addEventListener('change', function (e) { const f = e.target.files[0]; if (f) importProject(f); e.target.value = ''; });
document.getElementById('reportHtmlBtn').addEventListener('click', exportReportHTML);

// 初始化默认模式：进入页面即只显示「模拟组合回测」，隐藏「定投策略比较」
setMode('combo');

// 页面加载即从 IndexedDB 还原本地基金数据（自动保存，无需重新上传）
(async function initFunds() {
    try {
        const ok = await loadFundsFromDB();
        if (ok) { refreshFundUI(); }
    } catch (e) { console.error('加载本地基金失败', e); }
})();
