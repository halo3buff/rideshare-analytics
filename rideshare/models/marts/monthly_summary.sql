-- Monthly data

with trips as (
    select * from {{ ref('fct_trips') }}
),

summary as (
    select
        date_trunc('month', trip_date) as month,
        company,
        pickup_borough,
        day_type,

        -- Volume
        count(*) as total_trips,

        -- Trip
        round(avg(trip_miles), 2) as avg_trip_miles,
        round(avg(trip_duration_minutes), 2) as avg_duration_minutes,
        round(avg(avg_speed_mph), 2) as avg_speed_mph,

        -- Revenue metrics
        round(sum(total_fare), 2) as total_revenue,
        round(avg(total_fare), 2) as avg_fare,
        round(avg(tip_percentage), 2) as avg_tip_pct,

        -- Driver
        round(sum(driver_pay), 2) as total_driver_pay,
        round(avg(driver_pay), 2) as avg_driver_pay,
        round(sum(driver_pay) / nullif(sum(total_fare), 0) * 100, 2) as driver_pay_pct

    from trips
    group by 1, 2, 3, 4
)

select * from summary