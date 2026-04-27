-- Route profit data

with trips as (
    select * from {{ ref('fct_trips') }}
),

route_summary as (
    select
        pickup_borough,
        dropoff_borough,
        round(avg((total_fare + tips) / trip_miles), 2) as rev_per_mile,
        round(avg(trip_miles), 2) as route_distance,
        count(*) as total_trips
    from trips
    where pickup_borough != 'N/A'
        and dropoff_borough != 'N/A'
    group by 1, 2
    having count(*) > 10
    -- Routes averaging under 0.7 miles fall below the 5th percentile
    -- of all trip distances. Likely data anomalies or non-meaningful
    -- local trips. Exclude to prevent skewing 
        and avg(trip_miles) > 0.7
)

select * from route_summary
order by rev_per_mile desc