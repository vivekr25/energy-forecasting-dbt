{{ config(materialized='table', schema='mart') }}
-- Purpose: ML-ready features from Germany hourly load

with base as (
  -- ref('stg_load') points to the built stg_load model (dbt resolves the actual schema/table)
  select * from {{ ref('stg_load') }}
)

select
  ts_utc,
  load_de as target_load,

  -- Share of renewables in total load (guard against divide-by-zero)
  (wind_de + solar_de) / nullif(load_de, 0)                                  as renewables_pct,

  -- Time lags (1 hour and 24 hours)
  lag(load_de, 1)  over (order by ts_utc)                                     as load_lag_1h,
  lag(load_de, 24) over (order by ts_utc)                                     as load_lag_24h,

  -- 24h rolling average of load (current row + previous 23 rows)
  avg(load_de) over (
    order by ts_utc
    rows between 23 preceding and current row
  ) as load_roll_24h
from base
where load_de is not null          -- <--- ensures target_load is never NULL