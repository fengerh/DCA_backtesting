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

// 增强的日期解析：支持 yyyy-mm-dd 和 yyyy/m/d 格式（月日可带或不带前导零）
function parseDateString(str) {
    if (!str || typeof str !== 'string') return new Date(NaN);
    str = str.trim();
    // 尝试 '-' 分隔
    let parts = str.split('-');
    if (parts.length === 3) {
        let year = parseInt(parts[0]), month = parseInt(parts[1])-1, day = parseInt(parts[2]);
        if (!isNaN(year) && !isNaN(month) && !isNaN(day) && month >= 0 && month <= 11 && day >= 1 && day <= 31) {
            return new Date(year, month, day);
        }
    }
    // 尝试 '/' 分隔
    parts = str.split('/');
    if (parts.length === 3) {
        let year = parseInt(parts[0]), month = parseInt(parts[1])-1, day = parseInt(parts[2]);
        if (!isNaN(year) && !isNaN(month) && !isNaN(day) && month >= 0 && month <= 11 && day >= 1 && day <= 31) {
            return new Date(year, month, day);
        }
    }
    return new Date(NaN);
}

// Excel 序列号 -> 本地日期（时区安全：取 UTC 日期分量按本地构造，避免差一天）
function excelSerialToDate(serial) {
    const utcMs = Math.round((serial - 25569) * 86400000);
    const d = new Date(utcMs);
    return new Date(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

// 增强解析：Date对象 / 数字序列号 / yyyy-mm-dd / yyyy/m/d / yyyy年m月d日 / yyyymmdd / 纯数字
function parseDateFlexible(raw) {
    if (raw == null) return null;
    if (raw instanceof Date) return raw;
    if (typeof raw === 'number') return excelSerialToDate(raw);
    if (typeof raw !== 'string') return null;
    const s = raw.trim();
    if (!s) return null;
    const d = parseDateString(s);                 // 已有的 yyyy-mm-dd / yyyy/m/d
    if (!isNaN(d.getTime())) return d;
    let m = s.match(/^(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日\s*$/);
    if (m) {
        const dt = new Date(+m[1], +m[2] - 1, +m[3]);
        if (!isNaN(dt.getTime())) return dt;
    }
    if (/^\d{8}$/.test(s)) {                       // 紧凑 yyyymmdd
        const dt = new Date(+s.slice(0,4), +s.slice(4,6) - 1, +s.slice(6,8));
        if (!isNaN(dt.getTime())) return dt;
    }
    if (/^\d+(\.\d+)?$/.test(s)) return excelSerialToDate(parseFloat(s)); // 数字序列号
    return null;
}

