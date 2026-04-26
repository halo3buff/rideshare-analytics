WITH source AS (
    SELECT
        *
    FROM {{ source('raw', 'RAW_TRIPS') }}
),

renamed AS (
    SELECT
        -- Company identifier
        CASE
            WHEN hvfhs_license_num = 'HV0003' THEN 'Uber'
            WHEN hvfhs_license_num = 'HV0005' THEN 'Lyft'
            ELSE 'Other'
        END AS company,

        -- Location IDs
        pulocationid as pickup_zone_id,
        dolocationid as dropoff_zone_id,

        -- Timestamps
        pickup_datetime,
        dropoff_datetime,

        -- Trip metrics
        trip_miles,
        trip_time AS trip_duration_seconds,
        ROUND(trip_time / 60.0, 2) AS trip_duration_minutes,

        -- Financial columns
        base_passenger_fare,
        tolls,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        tips,
        driver_pay,
        ROUND(base_passenger_fare + tolls + sales_tax + congestion_surcharge
            + COALESCE(airport_fee, 0), 2) as total_fare,


        -- Flags
        shared_request_flag,
        wav_request_flag
    
    FROM source
    WHERE pickup_datetime IS NOT NULL
        AND dropoff_datetime IS NOT NULL
        AND trip_miles > 0
        AND base_passenger_fare > 0        
)

SELECT * FROM renamed