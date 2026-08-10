/* utils.js —— 由 split_tool.py 从单文件版本按功能拆分生成
 * 可手动编辑（日常维护源）；重新运行 `split` 会覆盖本文件。
 * 加载顺序：config -> utils -> benchmarks -> backtest -> analysis
 *          -> strategy -> report -> main
 */

// 工具函数：格式化日期为 yyyy-mm-dd
        function formatDate(date) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        }

// 三段日期解析：'-' 与 '/' 分隔共用，四位年份优先(yyyy-mm-dd / yyyy/m/d)，
// 否则按两位年份短日期(mm-dd-yy / m/d/yy)回退，含越界回填校验。
// 失败返回 Invalid Date。注意：本函数只处理"文本字符串"，绝不按 Excel 显示
// 格式(number_format)反推——显示格式与存储值无关，按格式猜解析是已被证伪的
// 方向（曾导致 mm-dd-yy 被吐成 '01-04-05' 误判为 1901 年）。
function parseDateString(str) {
    if (!str || typeof str !== 'string') return new Date(NaN);
    str = str.trim();
    const seps = ['-', '/'];
    for (const sep of seps) {
        const parts = str.split(sep);
        if (parts.length !== 3) continue;
        const nums = parts.map(p => parseInt(p, 10));
        if (nums.some(n => isNaN(n))) continue;
        let y, m, d;
        if (parts[0].length === 4) {
            // 四位年份优先：yyyy-mm-dd / yyyy/m/d
            [y, m, d] = [nums[0], nums[1], nums[2]];
        } else if (parts[2].length === 2 || parts[2].length === 4) {
            // 两位年份短日期：mm-dd-yy / m/d/yy（末段为年）
            // 歧义消解：首段 > 12 则判定为 dd-mm-yy（英式），否则默认 mm-dd-yy（美式，与 Excel 短日期默认一致）
            if (nums[0] > 12) { [d, m, y] = [nums[0], nums[1], nums[2]]; }
            else { [m, d, y] = [nums[0], nums[1], nums[2]]; }
            // 两位年份按世纪补全：yy < 50 -> 20yy，否则 19yy
            if (y < 100) y += (y < 50 ? 2000 : 1900);
        } else {
            continue;
        }
        const dt = new Date(y, m - 1, d);
        if (isNaN(dt.getTime())) continue;
        // 年份边界控制：越界（<1900 或 >当前年+2）视为非法
        if (!isValidYear4(y)) continue;
        // 回填校验：拒绝越界值（如 2026-02-31 被 JS 静默滚动成 3-03）
        if (dt.getFullYear() !== y || dt.getMonth() !== m - 1 || dt.getDate() !== d) continue;
        return dt;
    }
    return new Date(NaN);
}

// Excel 序列号 -> 本地日期（时区安全：取 UTC 日期分量按本地构造，避免差一天）
function excelSerialToDate(serial) {
    const utcMs = Math.round((serial - 25569) * 86400000);
    const d = new Date(utcMs);
    const dt = new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
    // 年份边界控制：异常序列号产生 5 位及以上年份时返回 Invalid Date
    if (!isValidYear4(dt.getFullYear())) return new Date(NaN);
    return dt;
}

// 增强解析：Date对象 / 数字序列号 / yyyy-mm-dd / yyyy/m/d / yyyy年m月d日 / yyyymmdd / 纯数字
function parseDateFlexible(raw) {
    if (raw == null) return null;
    // SheetJS(cellDates:true) 给出的 Date，其"本地时间分量"对应单元格字面日期，
    // 但可能带毫秒级负偏移（如 23:59:59.999 导致差一天），故先就近取整到整天再读分量
    if (raw instanceof Date) {
        if (isNaN(raw.getTime())) return null;
        const d = new Date(Math.round(raw.getTime() / 86400000) * 86400000);
        const dt = new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
        // 年份边界控制：越界（<1900 或 >当前年+2）视为非法
        if (!isValidYear4(dt.getFullYear())) return null;
        return dt;
    }
    // 注意：raw:true 已让真日期单元格产出 Date，切勿改用 raw:false/dateNF 按显示格式
    // (number_format) 反推解析——显示格式与存储值无关，mm-dd-yy 会被吐成 '01-04-05'
    // 误判为 1901 年，这是已被证伪的方向。
    if (typeof raw === 'number') return excelSerialToDate(raw);
    if (typeof raw !== 'string') return null;
    const s = raw.trim();
    if (!s) return null;
    const d = parseDateString(s);                 // 已有的 yyyy-mm-dd / yyyy/m/d
    if (!isNaN(d.getTime())) return d;
    let m = s.match(/^(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*$/);
    if (m) {
        const dt = new Date(+m[1], +m[2] - 1, +m[3]);
        if (!isNaN(dt.getTime()) && isValidYear4(dt.getFullYear())) return dt;
    }
    if (/^\d{8}$/.test(s)) {                       // 紧凑 yyyymmdd
        const dt = new Date(+s.slice(0,4), +s.slice(4,6) - 1, +s.slice(6,8));
        if (!isNaN(dt.getTime()) && isValidYear4(dt.getFullYear())) return dt;
    }
    if (/^\d+(\.\d+)?$/.test(s)) return excelSerialToDate(parseFloat(s)); // 数字序列号
    return null;
}

// ============ 年份边界控制 ============
// 仅拦截 5 位及以上年份：任意 4 位正整数年份（1000~9999）均视为合法。
function isValidYear4(year) {
    if (typeof year !== 'number' || !isFinite(year)) return false;
    if (!Number.isInteger(year)) return false;
    return year >= 1000 && year <= 9999;
}

// 校验 yyyy-mm-dd 日期字符串的年份是否为 4 位合法年份；非法返回 false
function isValidDateInput(value) {
    if (typeof value !== 'string') return false;
    const m = value.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return false;
    const y = parseInt(m[1], 10);
    if (!isValidYear4(y)) return false;
    // 回填校验：拒绝 2/30、2/31、4/31 等非法日（避免 JS Date 静默滚动）
    const dt = new Date(y, parseInt(m[2], 10) - 1, parseInt(m[3], 10));
    if (isNaN(dt.getTime())) return false;
    return dt.getFullYear() === y && dt.getMonth() === parseInt(m[2], 10) - 1 && dt.getDate() === parseInt(m[3], 10);
}

// 日期输入边界净化：合法返回原字符串，非法返回 null（调用方据此拒绝写入，不弹窗）
function sanitizeDateInput(value) {
    return isValidDateInput(value) ? value : null;
}

