{{ config(materialized='view') }}

with tripdata as (
  select *,
    row_number() over(partition by start_station_id, start_time) as rn
  from {{ source('staging', 'baywheels_trips') }}
  where start_station_id is not null
)

select
    -- Unique trip identifier
    {{ dbt_utils.generate_surrogate_key(['start_station_id', 'start_time']) }} as tripid,

    -- Station IDs
    {{ dbt.safe_cast("start_station_id", api.Column.translate_type("integer")) }} as start_station_id,
    {{ dbt.safe_cast("end_station_id", api.Column.translate_type("integer")) }} as end_station_id,

    -- Bike information
    {{ dbt.safe_cast("bike_id", api.Column.translate_type("integer")) }} as bike_id,

    -- Timestamps
    cast(start_time as timestamp) as start_time,
    cast(end_time as timestamp) as end_time,

    -- Trip duration
    {{ dbt.safe_cast("duration_sec", api.Column.translate_type("integer")) }} as duration_sec,

    -- Station coordinates
    cast(start_station_latitude as numeric) as start_station_latitude,
    cast(start_station_longitude as numeric) as start_station_longitude,
    cast(end_station_latitude as numeric) as end_station_latitude,
    cast(end_station_longitude as numeric) as end_station_longitude,
    start_station_name,
    end_station_name,

    -- User information
    user_type,
    bike_share_for_all_trip

from tripdata
where rn = 1
