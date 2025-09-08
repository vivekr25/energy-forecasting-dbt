# scripts/eda_forecast.py
import os
import duckdb
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_absolute_error
import numpy as np

# Adjust if you run this from a different folder:
DB_PATH = os.path.join("energy_dbt", "energy.duckdb")
CONN = duckdb.connect(DB_PATH)

df = CONN.execute("""
    SELECT ts_utc, target_load, renewables_pct, load_lag_1h, load_lag_24h, load_roll_24h
    FROM main_mart.mart_energy_features
    ORDER BY ts_utc
""").fetchdf()

print("Rows:", len(df), "Range:", df["ts_utc"].min(), "->", df["ts_utc"].max())
print(df.isna().mean().sort_values(ascending=False).head(10))

# Simple NA handling: drop rows with any NA in features/target
feat_cols = ["renewables_pct", "load_lag_1h", "load_lag_24h", "load_roll_24h"]
df_model = df.dropna(subset=feat_cols + ["target_load"]).copy()

# Time-based split: last 90 days as test
df_model["ts_utc"] = pd.to_datetime(df_model["ts_utc"])
cutoff = df_model["ts_utc"].max() - pd.Timedelta(days=90)
train = df_model[df_model["ts_utc"] <= cutoff]
test  = df_model[df_model["ts_utc"]  > cutoff]

X_train, y_train = train[feat_cols].values, train["target_load"].values
X_test,  y_test  = test[feat_cols].values,  test["target_load"].values

# Baseline: Ridge
model = Ridge(alpha=1.0, random_state=42)
model.fit(X_train, y_train)
pred = model.predict(X_test)

mae = mean_absolute_error(y_test, pred)
mape = np.mean(np.abs((y_test - pred) / np.clip(y_test, 1e-6, None))) * 100

print(f"Test MAE:  {mae:,.1f}")
print(f"Test MAPE: {mape:,.2f}%")
print("Coef (feat -> weight):")
for c, w in zip(feat_cols, model.coef_):
    print(f"  {c:>15}: {w:,.3f}")

out = test[["ts_utc", "target_load"]].copy()
out["pred"] = pred
out.to_csv("predictions_preview.csv", index=False)
print("Wrote predictions_preview.csv")