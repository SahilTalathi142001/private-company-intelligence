from pathlib import Path
import pandas as pd

root = Path.cwd() / "data" / "gold" / "platform" / "dq_gates"

for name in ("dq_results.parquet", "promotion_decisions.parquet"):
    path = root / name
    print(f"\n{'=' * 80}\nFILE: {path}\n")

    if not path.exists():
        print("FILE NOT FOUND")
        continue

    df = pd.read_parquet(path)
    print("Columns:", df.columns.tolist())
    print("Rows:", len(df))

    if df.empty:
        print("No records")
        continue

    # Search all text columns for relevant DQ evidence.
    text_cols = df.select_dtypes(include=["object", "string"]).columns
    mask = pd.Series(False, index=df.index)

    for col in text_cols:
        values = df[col].astype("string")
        mask |= values.str.contains(
            r"step.?21|company.?discovery|screening|fatal|promotion|FAIL",
            case=False, na=False, regex=True
        )

    matched = df.loc[mask]
    print("\nRelevant rows:", len(matched))
    print(matched.tail(30).to_string(index=False) if not matched.empty
          else df.tail(10).to_string(index=False))
