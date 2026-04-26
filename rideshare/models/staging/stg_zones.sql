WITH source AS (
    SELECT
        *
    FROM {{ source('raw', 'RAW_ZONES') }}
),

renamed AS (
    SELECT
        locationid AS zone_id,
        borough,
        zone AS zone_name,
        service_zone
    FROM source
    WHERE locationid IS NOT NULL
)

SELECT * FROM renamed