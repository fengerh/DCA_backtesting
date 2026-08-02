import pickle, pandas as pd

with open('dividend_cache.pkl', 'rb') as f:
    cache = pickle.load(f)

data = cache.get('data', cache)
print("最后更新时间:", cache.get('last_updated', '未知'))

for y, df in sorted(data.items()):
    if isinstance(df, pd.DataFrame) and not df.empty and '除息日期' in df.columns:
        dates = pd.to_datetime(df['除息日期'], errors='coerce').dropna()
        print(f"{y}年: {len(df)}条, 除息日期 {dates.min().date()} ~ {dates.max().date()}")
    else:
        print(f"{y}年: {len(df) if hasattr(df,'__len__') else '?'} 条")
