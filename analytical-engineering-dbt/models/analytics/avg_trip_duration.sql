{{ config(materialized='view') }}

select
    avg(duration_sec) as avg_trip_duration
from {{ ref('fact_baywheels_trips') }}
