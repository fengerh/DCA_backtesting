import tkinter as tk
from tkinter import messagebox, scrolledtext, filedialog
from tkcalendar import DateEntry
import akshare as ak
import pandas as pd
from datetime import datetime,timedelta
import threading
import time
import os

# ---------- 核心获取函数（已按年优化分红逻辑） ----------
def get_fund_data_akshare(fund_code, start_date, end_date, dividend_cache):
    """
    使用正确的 akshare 接口获取基金的净值和分红数据。
    dividend_cache: 一个字典，键为年份，值为该年所有基金的分红 DataFrame
    """
    # --- 1. 获取净值数据 ---
    try:
        nv_df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
        
        if nv_df is None or nv_df.empty:
            return pd.DataFrame()
        
        # 智能重命名：探测可能的列名
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
    except Exception as e:
        return pd.DataFrame()

    # --- 2. 从缓存中获取分红数据（新逻辑）---
    try:
        # 收集所有年份的分红数据
        all_div_dfs = []
        years = set()
        # 提取出所有分红的年份
        for date_str in nv_df['日期'].dt.strftime('%Y-%m-%d'):
            year = date_str[:4]
            years.add(year)
        
        # 从缓存中获取对应年份的数据
        for year in years:
            if year in dividend_cache:
                year_div_df = dividend_cache[year]
                # 筛选出当前基金的分红记录
                fund_div = year_div_df[year_div_df['基金代码'] == fund_code].copy()
                if not fund_div.empty:
                    all_div_dfs.append(fund_div)
        
        if all_div_dfs:
            div_df = pd.concat(all_div_dfs, ignore_index=True)
            # 重命名列以匹配后续处理
            div_df.rename(columns={'除息日期': '日期', '分红': '每份分红(元)'}, inplace=True)
            div_df['日期'] = pd.to_datetime(div_df['日期'], errors='coerce')
            div_df = div_df.dropna(subset=['日期', '每份分红(元)'])
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

def create_excel(fund_list, start_date, end_date, output_file, log_callback):
    placeholder = pd.DataFrame(columns=['日期', '单位净值(元)', '每份分红(元)'])
    
    # --- 构建分红缓存 ---
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    years_to_fetch = list(range(start_year, end_year + 1))
    
    dividend_cache = {}
    log_callback("📥 开始构建分红数据缓存，这将需要一些时间...")
    for year in years_to_fetch:
        try:
            log_callback(f"  ⏳ 正在获取 {year} 年的分红数据...")
            year_div_df = ak.fund_fh_em(year=str(year))
            dividend_cache[str(year)] = year_div_df
            log_callback(f"  ✅ {year} 年分红数据获取成功，共 {len(year_div_df)} 条记录。")
            time.sleep(1)  # 避免请求过快
        except Exception as e:
            log_callback(f"  ❌ 获取 {year} 年分红数据失败: {e}")
            dividend_cache[str(year)] = pd.DataFrame()
    log_callback("🎉 分红数据缓存构建完成！\n")


    # --- 逐个处理基金数据 ---
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for i, code in enumerate(fund_list):
            log_callback(f"正在处理 {code} ...")
            try:
                df = get_fund_data_akshare(code, start_date, end_date, dividend_cache)
                ws_name = code
                if df.empty:
                    placeholder.to_excel(writer, sheet_name=ws_name, index=False)
                    worksheet = writer.sheets[ws_name]
                    log_callback(f"  ⚠ {code} 无数据，已写入空表")
                else:
                    df.to_excel(writer, sheet_name=ws_name, index=False)
                    worksheet = writer.sheets[ws_name]
                    log_callback(f"  ✅ {code} 完成，共 {len(df)} 条记录")
                
                # 自适应列宽
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if cell.value:
                                # 粗略计算显示宽度
                                text = str(cell.value)
                                length = sum(2 if '\u4e00' <= c <= '\u9fff' else 1 for c in text)
                                if length > max_length:
                                    max_length = length
                        except:
                            pass
                    adjusted_width = max(max_length + 2, 10)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

            except Exception as e:
                placeholder.to_excel(writer, sheet_name=code, index=False)
                log_callback(f"  ❌ {code} 失败: {e}")
            time.sleep(0.5)
    log_callback(f"\n🎉 全部完成！文件已保存为：{output_file}")


# ---------- GUI 界面 ----------
class FundApp:
    def __init__(self, root):
        self.root = root
        root.title("基金数据批量获取工具 v20260409.1033")
        root.geometry("550x550")
        root.resizable(True, True)

        # 基金代码
        tk.Label(root, text="基金代码（每行一个）", font=('微软雅黑', 10)).pack(pady=(15, 5))
        self.code_text = tk.Text(root, height=6, width=50, font=('Consolas', 10))
        self.code_text.pack()
        self.code_text.insert('1.0', "161725\n005918\n002621")

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

        # 开始按钮
        self.btn = tk.Button(root, text="🚀 开始获取", font=('微软雅黑', 11), bg="#2563eb", fg='white',
                             command=self.start_fetch)
        self.btn.pack(pady=15)

        # 日志输出区域
        tk.Label(root, text="运行日志", font=('微软雅黑', 10)).pack()
        self.log_area = scrolledtext.ScrolledText(root, height=12, width=70, state='disabled', font=('Consolas', 9))
        self.log_area.pack(pady=5)

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
            create_excel(codes, start, end, out, self.log)
            self.root.after(0, lambda: self.btn.config(state='normal', text="🚀 开始获取"))
            self.root.after(0, lambda: messagebox.showinfo("完成", f"数据已保存至 {out}"))

        threading.Thread(target=task, daemon=True).start()

# ---------- 启动 ----------
if __name__ == "__main__":
    root = tk.Tk()
    app = FundApp(root)
    root.mainloop()