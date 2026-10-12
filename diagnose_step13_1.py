import pandas as pd

p = r"data\gold\financial_intelligence_validation\intelligence_exceptions\intelligence_exceptions.parquet"

df = pd.read_parquet(p)

print("ROWS:", len(df))
print("COLUMNS:", df.columns.tolist())
print()

for _, row in df.iterrows():
    print("=" * 100)
    for key, value in row.to_dict().items():
        print(f"{key}: {value}")
