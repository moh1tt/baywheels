{{ config(materialized='table') }}

with filtered_trips as (
    select *
    from {{ ref('staging_baywheels') }}
    where duration_sec > 60  -- Filtering for trips longer than 1 hour
),

trip_distances as (
    select
        *,
        ST_DISTANCE(ST_GEOGPOINT(start_station_longitude, start_station_latitude), 
                    ST_GEOGPOINT(end_station_longitude, end_station_latitude)) as trip_distance_meters
    from filtered_trips
)

select
    -- Trip identifiers and bike info
    tripid,
    bike_id,

    -- Trip times and duration
    start_time,
    end_time,
    duration_sec,

    -- Station IDs and coordinates
    start_station_id,
    end_station_id,
    start_station_latitude,
    start_station_longitude,
    end_station_latitude,
    end_station_longitude,
    start_station_name,
    end_station_name,

    -- New geolocation fields for Looker
    CONCAT(CAST(start_station_latitude AS STRING), ',', CAST(start_station_longitude AS STRING)) AS start_geo_location,
    CONCAT(CAST(end_station_latitude AS STRING), ',', CAST(end_station_longitude AS STRING)) AS end_geo_location,

    -- Calculated trip distance in meters
    trip_distance_meters,

    -- User information
    user_type,
    bike_share_for_all_trip
from trip_distances
