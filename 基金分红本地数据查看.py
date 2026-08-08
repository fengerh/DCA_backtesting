import os, pickle, pandas as pd

CACHE_FILE = 'dividend_cache.pkl'

if not os.path.exists(CACHE_FILE):
    print(f"未找到缓存文件：{CACHE_FILE}")
    print("请先运行「基金净值获取（界面版）.py」生成分红缓存。")
    raise SystemExit(0)

def _pickle_load_compat(path):
    """pickle 加载，兼容 pandas 升级导致的内部私有名变更（__nat_unpickle 改名）。"""
    try:
        with open(path, 'rb') as f:
            return pickle.load(f)
    except AttributeError as e:
        msg = str(e)
        if "__nat_unpickle" in msg and "_nat_unpickle" in msg:
            import pandas._libs.tslibs.nattype as _nt
            if not hasattr(_nt, "__nat_unpickle") and hasattr(_nt, "_nat_unpickle"):
                _nt.__nat_unpickle = _nt._nat_unpickle
            with open(path, 'rb') as f:
                return pickle.load(f)
        raise

try:
    cache = _pickle_load_compat(CACHE_FILE)
except Exception as e:
    print(f"读取缓存失败：{e}")
    print("缓存为旧版 pandas 序列化或已损坏。可改用同目录的 dividend_cache_backup.xlsx 查看数据。")
    raise SystemExit(1)

data = cache.get('data', cache) if isinstance(cache, dict) else cache
print("最后更新时间:", cache.get('last_updated', '未知'))

total = 0
for y, df in sorted(data.items()):
    if isinstance(df, pd.DataFrame) and not df.empty and '除息日期' in df.columns:
        dates = pd.to_datetime(df['除息日期'], errors='coerce').dropna()
        total += len(df)
        print(f"{y}年: {len(df)}条, 除息日期 {dates.min().date()} ~ {dates.max().date()}")
    else:
        n = len(df) if hasattr(df, '__len__') else '?'
        print(f"{y}年: {n} 条")
print(f"\n合计: {total} 条分红记录")
