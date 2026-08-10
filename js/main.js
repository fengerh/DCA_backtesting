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

// 初始化 x 轴对齐方式：默认按日期（同时同步按钮激活态与图表变量，避免受旧默认/缓存影响）
if (typeof setScXMode === 'function') setScXMode('date');

// 初始化比值口径输入框状态（点位比值默认置灰 N 日输入框）
if (typeof updateRatioModeInput === 'function') updateRatioModeInput();

// 页面加载即从 IndexedDB 还原本地基金数据（自动保存，无需重新上传）
(async function initFunds() {
    try {
        const ok = await loadFundsFromDB();
        if (ok) { refreshFundUI(); }
        // 基金异步加载完成后，重新应用当前模式：确保组合回测卡片可见，
        // 并在有基金数据且无计划时自动生成一条默认计划（首屏 setMode 时基金尚未加载，hasFundData 为 false）
        if (currentMode) setMode(currentMode);
        if (typeof updateDataMgmtCollapse === 'function') updateDataMgmtCollapse();
    } catch (e) { console.error('加载本地基金失败', e); }
})();
