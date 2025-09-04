-- Purpose: Stage Germany load + renewables columns from OPSD seed
{{ config(materialized='table', schema='main') }}
with src as (
    select * from {{ ref('time_series_60min_singleindex') }}
)
select
    cast(utc_timestamp as timestamp)                       as ts_utc,
    cast(DE_load_actual_entsoe_transparency as double)     as load_de,
    cast(DE_wind_generation_actual as double)              as wind_de,
    cast(DE_solar_generation_actual as double)             as solar_de
from src
where utc_timestamp is not null
