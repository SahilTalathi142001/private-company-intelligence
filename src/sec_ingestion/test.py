import pandas as pd

path = r"C:\Users\stalathi\Desktop\private-company-intelligence\data\gold\financial_company_intelligence\company_intelligence_profile\company_intelligence_profile.parquet"

df = pd.read_parquet(path)

print("Rows:", len(df))
print("\nColumns:")
for c in df.columns:
    print(c)

print("\nVersion:")
print(df["profile_version"].value_counts())

print("\nQuality:")
print(df[[
    "cik",
    "company_name",
    "profile_quality",
    "profile_quality_reason",
    "profile_as_of_period",
    "profile_freshness_status",
    "profile_age_days",
]].to_string(index=False))