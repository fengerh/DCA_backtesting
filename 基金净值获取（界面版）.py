import tkinter as tk
from tkinter import messagebox, scrolledtext, filedialog
from tkcalendar import DateEntry
import akshare as ak
import pandas as pd
from datetime import datetime,timedelta
import threading
import time
import pickle
import os

# ---------- 核心获取函数（已按年优化分红逻辑） ----------
def get_fund_data_akshare(fund_code, start_date, end_date, dividend_cache):
    """
    使用缓存数据获取基金的净值和分红数据
    """
    # --- 1. 获取净值数据（保持不变）---
    try:
        nv_df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
        
        if nv_df is None or nv_df.empty:
            return pd.DataFrame()
        
        # 智能重命名
        if '净值日期' in nv_df.columns:
            nv_df.rename(columns={'净值日期': '日期'}, inplace=True)
        elif '日期' not in nv_df.columns:
            nv_df.rename(columns={nv_df.columns[0]: '日期'}, inplace=True)
        
        if '单位净值' not in nv_df.columns:
            nav_col = [col for col in nv_df.columns if '净值' in col]
            if nav_col:
                nv_df.rename(columns={nav_col[0]: '单位净值'}, inplace=True)
            else:
                nv_df.rename(columns={nv_df.columns[1]: '单位净值'}, inplace=True)
        
        nv_df['日期'] = pd.to_datetime(nv_df['日期'], errors='coerce')
        nv_df = nv_df.dropna(subset=['日期'])
        mask = (nv_df['日期'] >= pd.to_datetime(start_date)) & (nv_df['日期'] <= pd.to_datetime(end_date))
        nv_df = nv_df.loc[mask, ['日期', '单位净值']].copy()
        nv_df['单位净值'] = pd.to_numeric(nv_df['单位净值'], errors='coerce')
        nv_df.sort_values(by='日期', ascending=True, inplace=True)
        nv_df.reset_index(drop=True, inplace=True)
        # 剔除周六、周日的冗余净值记录（保留周一~周五真实交易日，dayofweek：周一=0 ... 周日=6）
        nv_df = nv_df[nv_df['日期'].dt.dayofweek < 5]
        nv_df = nv_df.reset_index(drop=True)
    except Exception as e:
        return pd.DataFrame()

    # --- 2. 从缓存中获取分红数据 ---
    try:
        # 收集所有需要的分红数据
        all_div_dfs = []
        
        # 从缓存中按年份查找
        nv_df['year'] = nv_df['日期'].dt.year
        years_needed = nv_df['year'].unique()
        
        for year in years_needed:
            year_str = str(year)
            if year_str in dividend_cache and not dividend_cache[year_str].empty:
                year_div_df = dividend_cache[year_str]
                # 筛选出当前基金的分红记录
                fund_div = year_div_df[year_div_df['基金代码'] == fund_code].copy()
                if not fund_div.empty:
                    all_div_dfs.append(fund_div)
        
        if all_div_dfs:
            div_df = pd.concat(all_div_dfs, ignore_index=True)
            # 重命名列
            div_df.rename(columns={'除息日期': '日期', '分红': '每份分红(元)'}, inplace=True)
            div_df['日期'] = pd.to_datetime(div_df['日期'], errors='coerce')
            div_df = div_df.dropna(subset=['日期', '每份分红(元)'])
            
            # 筛选日期范围
            mask = (div_df['日期'] >= pd.to_datetime(start_date)) & (div_df['日期'] <= pd.to_datetime(end_date))
            div_df = div_df.loc[mask].copy()
            div_df['每份分红(元)'] = pd.to_numeric(div_df['每份分红(元)'], errors='coerce')
            div_df.sort_values(by='日期', ascending=True, inplace=True)
            div_df.reset_index(drop=True, inplace=True)
        else:
            div_df = pd.DataFrame(columns=['日期', '每份分红(元)'])
    except Exception as e:
        div_df = pd.DataFrame(columns=['日期', '每份分红(元)'])

    # --- 3. 合并数据 ---
    if not div_df.empty:
        merged_df = pd.merge(nv_df, div_df, on='日期', how='left')
        merged_df['每份分红(元)'] = merged_df['每份分红(元)'].fillna(0)
    else:
        merged_df = nv_df.copy()
        merged_df['每份分红(元)'] = 0.0

    final_df = merged_df[['日期', '单位净值', '每份分红(元)']].copy()
    final_df.rename(columns={'单位净值': '单位净值(元)'}, inplace=True)
    final_df['日期'] = final_df['日期'].dt.strftime('%Y-%m-%d')
    return final_df


CACHE_FILE = "dividend_cache.pkl"

def load_dividend_cache():
    """加载本地分红缓存（兼容新旧格式）"""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'rb') as f:
                cache = pickle.load(f)
                # 判断是否是旧格式（简单字典）
                if isinstance(cache, dict) and 'last_updated' not in cache:
                    # 旧版缓存（纯按年份字典），转换为新版格式
                    new_cache = {
                        'last_updated': None,  # 旧缓存不知道最后更新时间
                        'data': cache
                    }
                    return new_cache
                return cache
        except:
            return {'last_updated': None, 'data': {}}
    return {'last_updated': None, 'data': {}}

def save_dividend_cache(cache):
    """保存分红缓存到本地文件"""
    try:
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)
    except Exception as e:
        print(f"保存分红缓存失败: {e}")

def get_months_between(start_date, end_date):
    """获取两个日期之间的所有月份（格式：YYYY-MM）"""
    months = []
    current = datetime(start_date.year, start_date.month, 1)
    end = datetime(end_date.year, end_date.month, 1)
    
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        # 下一个月
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    
    return months

# 当年数据被视为“新鲜”的最长小时数，超过则允许重新拉取当年数据
CURRENT_YEAR_TTL_HOURS = 24


def _parse_last_updated(value):
    """把 last_updated 字符串解析为 datetime，失败返回 None"""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _merge_year_data(cache, year_str, new_year_data, log_callback):
    """把新抓到的某年数据合并进缓存（保留本地已有记录，去重）"""
    if new_year_data is None or new_year_data.empty:
        return
    if year_str in cache['data'] and not cache['data'][year_str].empty:
        existing = cache['data'][year_str]
        combined = pd.concat([existing, new_year_data], ignore_index=True)
        if '基金代码' in combined.columns and '除息日期' in combined.columns:
            combined = combined.drop_duplicates(subset=['基金代码', '除息日期'])
        added = len(combined) - len(existing)
        cache['data'][year_str] = combined
        log_callback(f"  🔀 {year_str} 年合并完成：本地 {len(existing)} 条 + 新增 {max(added, 0)} 条 = {len(combined)} 条")
    else:
        cache['data'][year_str] = new_year_data
        log_callback(f"  ➕ {year_str} 年写入缓存，共 {len(new_year_data)} 条")


def update_dividend_cache(years_needed, log_callback, force_refresh=False, offline_only=False):
    """
    以本地缓存为基础，只补齐缺失年份 + 按 TTL 刷新当年数据。

    - offline_only=True：完全不联网，只用本地缓存
    - force_refresh=True：忽略本地缓存，全量重取
    - 默认：历史年份只要缓存里有就不再重抓；当年数据按 last_updated 判断是否过期
    """
    now = datetime.now()
    current_year = now.year
    years_needed = [str(y) for y in years_needed]

    # ---- 离线模式：只读本地 ----
    if offline_only:
        cache = load_dividend_cache()
        if not cache['data']:
            log_callback("📴 离线模式：本地无分红缓存，本次分红数据将为空。")
            return {}
        missing = [y for y in years_needed if y not in cache['data'] or cache['data'][y].empty]
        log_callback(f"📴 离线模式：仅使用本地缓存（最后更新 {cache.get('last_updated') or '未知'}）。")
        if missing:
            log_callback(f"  ⚠ 本地缺少年份 {', '.join(missing)} 的分红数据，这些年份将按无分红处理。")
        return cache['data']

    # ---- 强制刷新 ----
    if force_refresh:
        cache = {'last_updated': None, 'data': {}}
        log_callback("🔄 强制刷新模式：将重新获取所有数据。")
        cache = fetch_full_years_data(years_needed, log_callback, cache)
        cache['last_updated'] = now.strftime("%Y-%m-%d %H:%M:%S")
        save_dividend_cache(cache)
        return cache['data']

    # ---- 正常模式：先读本地 ----
    cache = load_dividend_cache()
    if isinstance(cache, dict) and 'last_updated' not in cache:
        cache = {'last_updated': None, 'data': cache}
        log_callback("🔄 检测到旧版缓存格式，已自动转换。")

    if cache['data']:
        cached_years = sorted(cache['data'].keys())
        log_callback(f"📂 已读取本地缓存：{len(cached_years)} 个年份（{', '.join(cached_years)}），最后更新 {cache.get('last_updated') or '未知'}")
    else:
        log_callback("📂 本地缓存为空。")

    # 1) 缺失年份：必须抓
    years_to_fetch = [y for y in years_needed if y not in cache['data'] or cache['data'][y].empty]

    # 2) 当年数据：按 TTL 判断是否需要刷新（历史年份一律不重抓）
    cy = str(current_year)
    if cy in years_needed and cy not in years_to_fetch:
        last_updated = _parse_last_updated(cache.get('last_updated'))
        if last_updated is None:
            log_callback(f"🕒 缓存无更新时间记录，刷新 {cy} 年数据。")
            years_to_fetch.append(cy)
        else:
            age_hours = (now - last_updated).total_seconds() / 3600
            if age_hours >= CURRENT_YEAR_TTL_HOURS:
                log_callback(f"🕒 缓存已过去 {age_hours:.1f} 小时（阈值 {CURRENT_YEAR_TTL_HOURS} 小时），刷新 {cy} 年数据。")
                years_to_fetch.append(cy)
            else:
                log_callback(f"✅ 缓存 {age_hours:.1f} 小时内更新过，{cy} 年数据无需刷新。")

    if not years_to_fetch:
        log_callback("✅ 所需年份的分红数据均已在本地缓存中，无需联网。")
        return cache['data']

    years_to_fetch = sorted(set(years_to_fetch))
    log_callback(f"📥 需要联网获取的年份：{', '.join(years_to_fetch)}")

    fetched_any = False
    for year_str in years_to_fetch:
        try:
            log_callback(f"  ⏳ 获取 {year_str} 年分红数据...")
            year_data = ak.fund_fh_em(year=year_str)
            if year_data is not None and not year_data.empty:
                log_callback(f"  ✅ 获取 {year_str} 年数据成功，共 {len(year_data)} 条")
                _merge_year_data(cache, year_str, year_data, log_callback)
                fetched_any = True
            else:
                log_callback(f"  ⚠ {year_str} 年返回空数据，保留本地缓存内容。")
            time.sleep(0.5)
        except Exception as e:
            log_callback(f"  ❌ 获取 {year_str} 年数据失败：{e}（保留本地缓存内容）")

    if fetched_any:
        cache['last_updated'] = now.strftime("%Y-%m-%d %H:%M:%S")
        save_dividend_cache(cache)
        log_callback("💾 缓存已更新并保存。")
    else:
        log_callback("ℹ 本次未获取到新数据，缓存保持不变。")

    return cache['data']

def fetch_full_years_data(years_needed, log_callback, cache=None):
    """获取完整年份的数据"""
    if cache is None:
        cache = {'last_updated': None, 'data': {}}
    
    for year in years_needed:
        try:
            log_callback(f"  ⏳ 获取 {year} 年分红数据...")
            year_data = ak.fund_fh_em(year=str(year))
            if not year_data.empty:
                cache['data'][str(year)] = year_data
                log_callback(f"  ✅ {year} 年获取成功，共 {len(year_data)} 条")
            time.sleep(0.5)
        except Exception as e:
            log_callback(f"  ❌ {year} 年获取失败: {e}")
    
    return cache


def get_months_between(start_date, end_date):
    """获取两个日期之间的所有月份（格式：YYYY-MM）"""
    months = []
    current = datetime(start_date.year, start_date.month, 1)
    end = datetime(end_date.year, end_date.month, 1)
    
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        # 下一个月
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    
    return months


def get_fund_name_akshare(fund_code):
    """获取基金名称 - 修复版本"""
    try:
        # 方法1: 使用 fund_name_em 接口获取基金基本信息
        try:
            # 先尝试用东方财富的接口
            fund_info = ak.fund_name_em()
            if not fund_info.empty:
                # 查找匹配的基金
                match = fund_info[fund_info['基金代码'] == fund_code]
                if not match.empty and '基金简称' in match.columns:
                    return match['基金简称'].iloc[0]
        except Exception as e1:
            print(f"方法1获取基金 {fund_code} 名称失败: {e1}")
        
        # 方法2: 使用 fund_info_em 接口
        try:
            fund_info_df = ak.fund_info_em(symbol=fund_code)
            if not fund_info_df.empty:
                # 查看有哪些列可用
                print(f"基金 {fund_code} 可用列: {fund_info_df.columns.tolist()}")
                
                # 尝试不同的可能列名
                possible_columns = ['基金简称', '基金名称', 'name', 'SHORTNAME', 'fund_name']
                for col in possible_columns:
                    if col in fund_info_df.columns and not pd.isna(fund_info_df[col].iloc[0]):
                        return str(fund_info_df[col].iloc[0])
        except Exception as e2:
            print(f"方法2获取基金 {fund_code} 名称失败: {e2}")
        
        # 方法3: 使用 fund_individual_info_em
        try:
            fund_individual = ak.fund_individual_info_em(symbol=fund_code, indicator="单位净值走势")
            if not fund_individual.empty and '基金名称' in fund_individual.columns:
                return fund_individual['基金名称'].iloc[0]
        except Exception as e3:
            print(f"方法3获取基金 {fund_code} 名称失败: {e3}")
        
        # 如果所有方法都失败，使用更智能的默认名称
        return f"基金_{fund_code}"
        
    except Exception as e:
        print(f"获取基金 {fund_code} 名称时发生错误: {e}")
        return f"基金_{fund_code}"
    

def create_excel(fund_list, start_date, end_date, output_file, log_callback, force_refresh=False, offline_only=False):
    placeholder = pd.DataFrame(columns=['日期', '单位净值(元)', '每份分红(元)'])
    
    # 计算所需年份范围
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    years_needed = list(range(start_year, end_year + 1))
    
    # 获取/更新分红缓存
    log_callback("📋 正在准备分红数据缓存...")
    log_callback(f"📅 所需年份范围: {start_year} 到 {end_year}")
    
    # 智能更新缓存
    dividend_cache = update_dividend_cache(years_needed, log_callback, force_refresh, offline_only)
    
    # 统计缓存数据量
    total_records = 0
    for year, data in dividend_cache.items():
        if data is not None and not data.empty:
            total_records += len(data)
    
    log_callback(f"📊 缓存总计: {len(dividend_cache)} 个年份，{total_records} 条分红记录")
    log_callback("")
    
    # 获取基金数据并写入Excel
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for i, code in enumerate(fund_list):
            log_callback(f"正在处理 {code} ...")
            
            # 获取基金名称
            try:
                fund_name = get_fund_name_akshare(code)
                sheet_name = f"{code}_{fund_name}"
                sheet_name = clean_sheet_name(sheet_name)
            except:
                sheet_name = code
            
            try:
                df = get_fund_data_akshare(code, start_date, end_date, dividend_cache)
                
                if df.empty:
                    placeholder.to_excel(writer, sheet_name=sheet_name, index=False)
                    log_callback(f"  ⚠ 无数据，已写入空表")
                else:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    log_callback(f"  ✅ 完成，共 {len(df)} 条记录")
                
                # 自适应列宽
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if cell.value:
                                text = str(cell.value)
                                length = sum(2 if '\u4e00' <= c <= '\u9fff' else 1 for c in text)
                                if length > max_length:
                                    max_length = length
                        except:
                            pass
                    adjusted_width = max(max_length + 2, 10)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

            except Exception as e:
                placeholder.to_excel(writer, sheet_name=sheet_name, index=False)
                log_callback(f"  ❌ 失败: {e}")
            time.sleep(0.5)
    
    log_callback(f"\n🎉 全部完成！文件已保存为：{output_file}")


def clean_sheet_name(name, max_length=31):
    """
    清理Sheet名中的非法字符
    Excel Sheet名限制：不能超过31个字符，不能包含: \ / ? * [ ]
    """
    # 定义非法字符
    invalid_chars = ['\\', '/', '?', '*', '[', ']', ':', '：']
    
    # 移除非法字符
    for char in invalid_chars:
        name = name.replace(char, '')
    
    # 截断到最大长度
    if len(name) > max_length:
        name = name[:max_length-3] + "..."  # 保留3个字符给省略号
    
    return name


# ---------- GUI 界面 ----------
class FundApp:
    def __init__(self, root):
        self.root = root
        root.title("基金数据批量获取工具 v20260423.1500")
        root.geometry("550x550")
        root.resizable(True, True)

        # 基金代码
        tk.Label(root, text="基金代码（每行一个）", font=('微软雅黑', 10)).pack(pady=(15, 5))
        self.code_text = tk.Text(root, height=6, width=50, font=('Consolas', 10))
        self.code_text.pack()
        self.code_text.insert('1.0', "000001\n007466") # 示例代码

        # 日期范围（使用 DateEntry 控件）
        date_frame = tk.Frame(root)
        date_frame.pack(pady=10)
        tk.Label(date_frame, text="开始日期：").grid(row=0, column=0, padx=5)
        self.start_entry = DateEntry(date_frame, width=12, background='darkblue',
                                    foreground='white', borderwidth=2, date_pattern='yyyy-mm-dd')
        self.start_entry.grid(row=0, column=1, padx=5)
        self.start_entry.set_date(datetime(2016, 1, 1))
        
        tk.Label(date_frame, text="结束日期：").grid(row=0, column=2, padx=5)
        self.end_entry = DateEntry(date_frame, width=12, background='darkblue',
                                  foreground='white', borderwidth=2, date_pattern='yyyy-mm-dd')
        self.end_entry.grid(row=0, column=3, padx=5)
        self.end_entry.set_date(datetime.now() - timedelta(days=1)) # 默认设置为昨天，避免当天数据不完整的问题

        # 输出文件名
        file_frame = tk.Frame(root)
        file_frame.pack(pady=5)
        tk.Label(file_frame, text="输出文件名：").pack(side=tk.LEFT, padx=5)
        self.out_entry = tk.Entry(file_frame, width=30)
        self.out_entry.pack(side=tk.LEFT, padx=5)
        self.out_entry.insert(0, f"基金数据_{datetime.now().strftime('%Y%m%d')}.xlsx")

        # 分红缓存选项
        opt_frame = tk.Frame(root)
        opt_frame.pack(pady=5)

        self.force_refresh_var = tk.BooleanVar(value=False)
        self.offline_var = tk.BooleanVar(value=False)

        self.force_cb = tk.Checkbutton(opt_frame, text="强制刷新分红缓存（忽略本地文件）",
                                       variable=self.force_refresh_var, command=self.on_force_toggle)
        self.force_cb.pack(anchor='w')

        self.offline_cb = tk.Checkbutton(opt_frame, text="仅用本地分红缓存（不联网更新分红）",
                                         variable=self.offline_var, command=self.on_offline_toggle)
        self.offline_cb.pack(anchor='w')

        # 开始按钮
        self.btn = tk.Button(root, text="🚀 开始获取", font=('微软雅黑', 11), bg="#2563eb", fg='white',
                             command=self.start_fetch)
        self.btn.pack(pady=15)

        # 日志输出区域
        tk.Label(root, text="运行日志", font=('微软雅黑', 10)).pack()
        self.log_area = scrolledtext.ScrolledText(root, height=12, width=70, state='disabled', font=('Consolas', 9))
        self.log_area.pack(pady=5)

    def on_force_toggle(self):
        """强制刷新与离线模式互斥"""
        if self.force_refresh_var.get():
            self.offline_var.set(False)

    def on_offline_toggle(self):
        """离线模式与强制刷新互斥"""
        if self.offline_var.get():
            self.force_refresh_var.set(False)

    def log(self, msg):
        self.log_area.config(state='normal')
        self.log_area.insert(tk.END, msg + '\n')
        self.log_area.see(tk.END)
        self.log_area.config(state='disabled')
        self.root.update()

    def start_fetch(self):
        codes = self.code_text.get('1.0', tk.END).strip().split('\n')
        codes = [c.strip() for c in codes if c.strip() and c.strip().isdigit()]
        if not codes:
            messagebox.showerror("错误", "请输入至少一个有效的基金代码")
            return

        start = self.start_entry.get().strip()
        end = self.end_entry.get().strip()
        out = self.out_entry.get().strip()
        if not out.endswith('.xlsx'):
            out += '.xlsx'
        
        # 检查文件是否存在，如果存在则自动添加 _1, _2 ...
        original_out = out
        counter = 1
        while os.path.exists(out):
            name, ext = os.path.splitext(original_out)
            out = f"{name}_{counter}{ext}"
            counter += 1
        if out != original_out:
            self.log(f"⚠ 文件名已存在，将自动保存为：{out}")


        self.btn.config(state='disabled', text="获取中...")
        self.log_area.config(state='normal')
        self.log_area.delete('1.0', tk.END)
        self.log_area.config(state='disabled')
        self.log(f"开始获取数据...")

        def task():
            force_refresh = self.force_refresh_var.get()   # 获取复选框状态
            offline_only = self.offline_var.get()
            create_excel(codes, start, end, out, self.log, force_refresh, offline_only)
            self.root.after(0, lambda: self.btn.config(state='normal', text="🚀 开始获取"))
            self.root.after(0, lambda: messagebox.showinfo("完成", f"数据已保存至 {out}"))

        threading.Thread(target=task, daemon=True).start()

# ---------- 启动 ----------
if __name__ == "__main__":
    root = tk.Tk()
    app = FundApp(root)
    root.mainloop()