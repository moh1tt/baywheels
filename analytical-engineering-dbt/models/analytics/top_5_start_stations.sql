{{ config(materialized='view') }}

select
    start_station_id,
    count(tripid) as trip_count
from {{ ref('fact_baywheels_trips') }}
group by start_station_id
order by trip_count desc
limit 5
