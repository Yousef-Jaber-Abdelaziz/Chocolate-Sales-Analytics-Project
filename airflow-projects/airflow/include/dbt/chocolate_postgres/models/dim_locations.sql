{{ config(
    materialized='incremental',
    unique_key='city' 
) }}

WITH raw_locations AS (
    SELECT 
        -- Generate the Surrogate Key (Hash the natural key)
        md5(cast(city as text)) AS location_sk,
        
        -- The Natural Key and rest of the attributes
        city,
        country,
        lat,
        lng,
        iso2,
        iso3,
        -- Keep your DWH audit trail intact
        CURRENT_TIMESTAMP AS created_at
        
    FROM {{ source('raw_data', 'stg_dim_locations') }} 
)

SELECT * FROM raw_locations

{% if is_incremental() %}

  -- Explicitly filter out any city that is already safely stored in your DWH
  WHERE city NOT IN (SELECT city FROM {{ this }})

{% endif %}