/* benchmarks.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// 基准指数日期缓存：选中基准的连续交易日（排序后的日期字符串数组），作为"工作日检测"参照日历
let benchmarkDateStrs = [];

// 刷新基准日期缓存（基准上传/切换/删除后调用）；刷新后重算净值空档提示。
// 基准天然连续且排除节假日，比纯周一到周五更准确；无基准时缓存为空，回退到多基金并集检测。
// 定义在本文件（基准脚本先于 backtest.js 加载），供 loadBenchmarkList 初始化时调用。
// 注意：checkNavGaps 定义于 backtest.js（后加载），故先 await 一拍再调用，确保回测脚本已就绪。
async function refreshBenchmarkCache() {
    if (currentBenchmarkId) {
        const bm = await db.benchmarks.get(currentBenchmarkId);
        benchmarkDateStrs = (bm && bm.data && bm.data.length)
            ? bm.data.map(d => d.date).sort()
            : [];
    } else {
        benchmarkDateStrs = [];
    }
    await Promise.resolve();   // 让出微任务，确保后续脚本（backtest.js）加载完成后再刷新提示
    checkNavGaps();
}


// 基准列表渲染（含日期范围）
async function loadBenchmarkList() {
    compositeWeights = {}; // 新增：每次刷新基准列表时清空临时权重
    const benchmarks = await db.benchmarks.toArray();
    benchmarks.sort((a, b) => (a.order || 0) - (b.order || 0));
    // 未选中任何基准时，默认选中第一个（写入顺序首条），使"基准指数工作日检测"立即生效
    if (!currentBenchmarkId && benchmarks.length > 0) currentBenchmarkId = benchmarks[0].id;
    const container = document.getElementById('benchmarkList');
    const select = document.getElementById('benchmarkSelect');
    
    if (benchmarks.length === 0) {
        container.innerHTML = '<p class="text-gray-400 text-sm text-center py-2">暂无基准，请上传 Excel 文件</p>';
    } else {
        container.innerHTML = benchmarks.map(b => {
            const firstDate = b.data[0]?.date || '?';
            const lastDate = b.data[b.data.length-1]?.date || '?';
            return `
            <div class="flex items-center justify-between bg-white p-2 rounded border">
                <div class="flex items-center gap-2">
                    <input type="radio" name="activeBenchmark" value="${b.id}" ${currentBenchmarkId == b.id ? 'checked' : ''} class="benchmark-radio">
                    <div>
                        <span class="text-sm font-medium">${b.name}</span>
                        <span class="text-xs text-gray-500 ml-2">${firstDate} ~ ${lastDate}</span>
                        <span class="text-xs text-gray-400 ml-1">(${b.data.length}条)</span>
                    </div>
                </div>
                <button class="text-red-500 hover:text-red-700 text-sm delete-benchmark" data-id="${b.id}">删除</button>
            </div>
        `}).join('');
    }

    select.innerHTML = '<option value="">-- 无 --</option>';
    benchmarks.forEach(b => {
        const option = document.createElement('option');
        option.value = b.id;
        option.textContent = `${b.name} (${b.data[0]?.date?.slice(0,7) || ''})`;
        if (currentBenchmarkId == b.id) option.selected = true;
        select.appendChild(option);
    });

            document.querySelectorAll('.benchmark-radio').forEach(radio => {
                radio.addEventListener('change', (e) => {
                    currentBenchmarkId = parseInt(e.target.value);
                    select.value = currentBenchmarkId;
                    refreshBenchmarkCache();
                    if (backtestResult.dates.length) updateCharts();
                });
            });
    document.querySelectorAll('.delete-benchmark').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            const id = parseInt(e.target.dataset.id);
            if (confirm('确定删除该基准吗？')) {
                await db.benchmarks.delete(id);
                if (currentBenchmarkId === id) { currentBenchmarkId = null; select.value = ''; }
                await loadBenchmarkList();
                if (backtestResult.dates.length) updateCharts();
            }
        });
    });
    select.onchange = () => {
        currentBenchmarkId = select.value ? parseInt(select.value) : null;
        document.querySelectorAll('.benchmark-radio').forEach(r => r.checked = r.value == select.value);
        refreshBenchmarkCache();
        if (backtestResult.dates.length) updateCharts(); // 这会触发图表和表格的更新
    };
    refreshBenchmarkCache();   // 基准列表变化（初始化/上传/删除）后刷新参照日历缓存
}

// 渲染合成基准的权重输入界面
async function renderCompositePanel() {
    const benchmarks = await db.benchmarks.toArray();
    benchmarks.sort((a, b) => (a.order || 0) - (b.order || 0));
    const container = document.getElementById('compositeWeightsContainer');
    if (benchmarks.length === 0) {
        container.innerHTML = '<p class="text-gray-400 text-sm">暂无基准数据，请先上传基准</p>';
        return;
    }
    let html = '';
    benchmarks.forEach(b => {
        const weight = compositeWeights[b.id] || 0;
        html += `
            <div class="flex items-center justify-between bg-white p-2 rounded border">
                <span class="text-sm font-medium">${b.name}</span>
                <div class="flex items-center gap-2">
                    <input type="number" min="0" max="100" step="0.1" value="${weight}" 
                        class="composite-weight-input w-20 p-1 border rounded text-sm" data-id="${b.id}">
                    <span class="text-xs text-gray-500">%</span>
                </div>
            </div>
        `;
    });
    container.innerHTML = html;
    updateTotalWeight();
    
    document.querySelectorAll('.composite-weight-input').forEach(input => {
        input.addEventListener('input', (e) => {
            const id = parseInt(e.target.dataset.id);
            compositeWeights[id] = parseFloat(e.target.value) || 0;
            updateTotalWeight();
        });
    });
}

// 更新总权重显示及生成按钮状态
function updateTotalWeight() {
    const total = Object.values(compositeWeights).reduce((sum, w) => sum + w, 0);
    document.getElementById('totalWeightDisplay').textContent = total.toFixed(1);
    const btn = document.getElementById('generateCompositeBtn');
    btn.disabled = (Math.abs(total - 100) > 0.01);
}

// 平均分配权重
function setEqualWeights() {
    const ids = Object.keys(compositeWeights);
    if (ids.length === 0) return;
    const equal = 100 / ids.length;
    ids.forEach(id => compositeWeights[id] = equal);
    renderCompositePanel();
}

// 生成合成基准
async function generateCompositeBenchmark() {
    const benchmarks = await db.benchmarks.toArray();
    benchmarks.sort((a, b) => (a.order || 0) - (b.order || 0));
    const activeIds = Object.keys(compositeWeights).filter(id => compositeWeights[id] > 0);
    if (activeIds.length === 0) {
        alert('至少需要一个基准权重 > 0');
        return;
    }
    
    const selectedBenchmarks = benchmarks.filter(b => activeIds.includes(String(b.id)));
    const allDatesSet = new Set();
    selectedBenchmarks.forEach(b => b.data.forEach(d => allDatesSet.add(d.date)));
    const allDates = Array.from(allDatesSet).sort();
    
    if (allDates.length === 0) {
        alert('没有可用的日期数据');
        return;
    }
    
    // 第一步：为每个基准计算归一化净值（以第一个共同日期为基准1.0）
    const normalizedData = [];
    
    for (const bm of selectedBenchmarks) {
        const bmMap = new Map(bm.data.map(d => [d.date, d.nav]));
        
        // 找到该基准在合成期间的第一个有效净值
        let baseNav = null;
        for (const date of allDates) {
            const nav = bmMap.get(date);
            if (nav !== undefined) {
                baseNav = nav;
                break;
            }
        }
        
        if (baseNav === null) continue;
        
        // 计算该基准的归一化净值序列
        const normalizedSeries = [];
        for (const date of allDates) {
            let nav = bmMap.get(date);
            if (nav === undefined) {
                // 前向填充
                const bmData = bm.data;
                for (let i = bmData.length - 1; i >= 0; i--) {
                    if (bmData[i].date <= date) {
                        nav = bmData[i].nav;
                        break;
                    }
                }
            }
            if (nav !== undefined) {
                normalizedSeries.push({
                    date: date,
                    normalizedNav: nav / baseNav  // 归一化到1.0
                });
            }
        }
        
        if (normalizedSeries.length > 0) {
            normalizedData.push({
                id: bm.id,
                name: bm.name,
                weight: compositeWeights[bm.id] / 100,  // 转换为小数权重
                series: normalizedSeries
            });
        }
    }
    
    if (normalizedData.length === 0) {
        alert('未能生成有效合成数据');
        return;
    }
    
    // 第二步：按日期加权合成
    const compositeData = [];
    for (const date of allDates) {
        let compositeNav = 0;
        let hasData = false;
        
        for (const bm of normalizedData) {
            const dayData = bm.series.find(d => d.date === date);
            if (dayData) {
                compositeNav += dayData.normalizedNav * bm.weight;
                hasData = true;
            }
        }
        
        if (hasData && compositeNav > 0) {
            compositeData.push({ date, nav: compositeNav });
        }
    }
    
    if (compositeData.length === 0) {
        alert('未能生成有效合成数据');
        return;
    }
    
    // 构建带有权重的名称
    const nameParts = [];
    for (const bm of selectedBenchmarks) {
        const weight = compositeWeights[bm.id];
        if (weight > 0) {
            const formattedWeight = Number.isInteger(weight) ? weight.toFixed(0) : weight.toFixed(1);
            nameParts.push(`${bm.name}*${formattedWeight}%`);
        }
    }
    const name = '合成_' + nameParts.join('+');

    await db.benchmarks.add({ name, data: compositeData });
    compositeWeights = {};
    document.getElementById('compositePanel').classList.add('hidden');
    await loadBenchmarkList();
    alert(`合成基准 "${name}" 已生成，共 ${compositeData.length} 条记录`);
}

// 清空所有基准数据
async function clearAllBenchmarks() {
    if (confirm('确定要清空所有已保存的基准吗？此操作不可恢复。')) {
        await db.benchmarks.clear();
        currentBenchmarkId = null;
        document.getElementById('benchmarkSelect').value = '';
        await loadBenchmarkList();
        if (backtestResult.dates.length) updateCharts();
        alert('已清空所有基准数据');
    }
}

// 解析Sheet数据
function parseSheetData(sheet) {
    const json = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: true });
    if (json.length < 2) return [];
    const data = [];
    for (let i = 1; i < json.length; i++) {
        const row = json[i];
        if (!row || row.length < 2) continue;
        const rawDate = row[0];
        const nav = parseFloat(row[1]);
        if (isNaN(nav)) continue;
        const date = parseDateFlexible(rawDate);
        if (!date || isNaN(date.getTime())) continue;
        data.push({ date: formatDate(date), nav });
    }
    data.sort((a,b) => a.date.localeCompare(b.date));
    return data;
}

// 上传基准按钮事件
document.getElementById('uploadBenchmarkBtn').addEventListener('click', async () => {
    const fileInput = document.getElementById('benchmarkFileInput');
    const nameInput = document.getElementById('benchmarkName');
    const file = fileInput.files[0];
    if (!file) { alert('请选择 Excel 文件'); return; }
    const reader = new FileReader();
    reader.onload = async (e) => {
        const data = new Uint8Array(e.target.result);
        const workbook = XLSX.read(data, { type: 'array', cellDates: true });
        const sheetNames = workbook.SheetNames;
        if (sheetNames.length === 0) { alert('Excel 文件中没有 Sheet'); return; }
        // 按 Excel 工作表顺序累加 order，使多次上传按序追加、表内保持工作表顺序
        const existing = await db.benchmarks.toArray();
        const maxOrder = existing.length ? Math.max.apply(null, existing.map(b => b.order || 0)) : 0;
        let importCount = 0;
        for (let sheetIdx = 0; sheetIdx < sheetNames.length; sheetIdx++) {
            const sheetName = sheetNames[sheetIdx];
            const sheet = workbook.Sheets[sheetName];
            const parsed = parseSheetData(sheet);
            if (parsed.length === 0) continue;
            let benchmarkName = sheetName;
            if (sheetNames.length === 1) {
                const custom = nameInput && nameInput.value.trim();
                if (custom) benchmarkName = custom;
            }
            await db.benchmarks.add({ name: benchmarkName, data: parsed, order: maxOrder + 1 + sheetIdx });
            importCount++;
        }
        if (importCount === 0) alert('未解析到有效数据，请检查日期列格式（支持 Excel 日期、数字序列号、yyyy-mm-dd、yyyy/m/d、yyyy年m月d日、yyyymmdd）');
        else alert(`成功导入 ${importCount} 个基准`);
        fileInput.value = '';
        if (nameInput) nameInput.value = '沪深300';
        await loadBenchmarkList();
    };
    reader.readAsArrayBuffer(file);
});

// 合成基准面板切换
document.getElementById('toggleCompositePanelBtn').addEventListener('click', async () => {
    const panel = document.getElementById('compositePanel');
    panel.classList.toggle('hidden');
    if (!panel.classList.contains('hidden')) {
        await renderCompositePanel();
    }
});

document.getElementById('equalWeightBtn').addEventListener('click', setEqualWeights);
document.getElementById('generateCompositeBtn').addEventListener('click', generateCompositeBenchmark);

// 盈利概率 / 相关性分析 / 图表 独立折叠按钮
function bindToggle(btnId, collapsibleId) {
    const btn = document.getElementById(btnId);
    if (!btn) return;
    btn.addEventListener('click', () => {
        const c = document.getElementById(collapsibleId);
        const collapsed = c.classList.toggle('hidden');
        btn.textContent = collapsed ? '▼ 展开' : '▲ 折叠';
        if (!collapsed && collapsibleId === 'netValueChartCollapsible' && netValueChart) netValueChart.resize();
        if (!collapsed && collapsibleId === 'assetChartCollapsible' && assetChart) assetChart.resize();
    });
}
bindToggle('toggleProfitProbBtn', 'profitProbCollapsible');
bindToggle('toggleCorrelationBtn', 'correlationCollapsible');
bindToggle('toggleNetValueChartBtn', 'netValueChartCollapsible');
bindToggle('toggleAssetChartBtn', 'assetChartCollapsible');
bindToggle('toggleAnalysisTableBtn', 'analysisTableCollapsible');

// 初始化加载基准列表
loadBenchmarkList();

