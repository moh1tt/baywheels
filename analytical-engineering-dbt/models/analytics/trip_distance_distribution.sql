{{ config(materialized='view') }}

select
    case 
        when trip_distance_meters < 1000 then '<1km'
        when trip_distance_meters between 1000 and 5000 then '1-5km'
        when trip_distance_meters between 5000 and 10000 then '5-10km'
        else '>10km'
    end as distance_range,
    count(tripid) as trip_count
from {{ ref('fact_baywheels_trips') }}
group by distance_range
