echo '
## Reproduce locally

1) Create venv + install:
   python3 -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt

2) Place OPSD CSV at: energy_dbt/seeds/time_series_60min_singleindex.csv

3) Build:
   cd energy_dbt
   dbt seed
   dbt run --select stg_load
   dbt run --select mart_energy_features
   dbt test --select mart_energy_features
' >> energy_dbt/README.md

git add energy_dbt/README.md
git commit -m "docs: quickstart build steps"
git push