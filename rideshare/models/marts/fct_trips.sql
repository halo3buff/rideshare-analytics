-- Fact table

with trips as (
    select * from {{ref('stg_trips') }}
),

zones as (
    select * from {{ ref('stg_zones') }}
),

joined as (
    select

        -- Primary Key
        row_number() over (order by t.pickup_datetime, t.pickup_zone_id, t.dropoff_zone_id) as trip_id,

        -- Company
        t.company,

        -- Pickup Locatiuons
        t.pickup_zone_id,
        pz.zone_name as pickup_zone,
        pz.borough as pickup_borough,

        -- Dropoff Locations
        t.dropoff_zone_id,
        dz.zone_name as dropoff_zone,
        dz.borough as dropoff_borough,

        -- Time dimensions
        t.pickup_datetime,
        t.dropoff_datetime,
        date(t.pickup_datetime) as trip_date,
        dayofweek(t.pickup_datetime) as day_of_week,
        hour(t.pickup_datetime) as pickup_hour,
        monthname(t.pickup_datetime) as month_name,
        case
            when dayofweek(t.pickup_datetime) in (1, 7) then 'Weekend'
            else 'Weekday'
        end as day_type,

        -- Trip metrics
        t.trip_miles,
        t.trip_duration_minutes,
        t.base_passenger_fare,
        t.total_fare,
        t.tips,
        t.driver_pay,
        t.tolls,
        t.congestion_surcharge,

        -- Derived metrics
        case
            when t.trip_duration_minutes > 0
            then round(t.trip_miles / (t.trip_duration_minutes / 60), 2)
            else null
        end as avg_speed_mph,

        case
            when t.total_fare > 0
            then round(t.tips / t.total_fare * 100.0, 2)
            else 0
        end as tip_percentage,

        -- Flags
        t.shared_request_flag,
        t.wav_request_flag


    from trips t
    left join zones pz on t.pickup_zone_id = pz.zone_id
    left join zones dz on t.dropoff_zone_id = dz.zone_id
)

SELECT * FROM joined