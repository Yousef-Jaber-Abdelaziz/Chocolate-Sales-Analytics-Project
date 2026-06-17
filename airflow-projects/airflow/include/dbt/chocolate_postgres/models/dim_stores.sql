{{ config(
    materialized='incremental',
    unique_key=['store_name', 'store_type'] 
) }}

WITH raw_stores AS (
    SELECT 
        -- Generate the Surrogate Key by hashing the composite natural key
        md5(cast(store_name as text) || cast(store_type as text)) AS store_sk,
        
        -- The Natural Keys and rest of the attributes
        store_name, 
        store_type, 
        is_online, 
        store_batch_id AS batch_id, 
        store_source_system AS source_system,
        CURRENT_TIMESTAMP AS created_at
        
    FROM {{ source('raw_data', 'stg_dim_stores') }}
)

SELECT * FROM raw_stores

{% if is_incremental() %}

  -- Postgres allows us to check multiple columns at once using parentheses (a tuple)
  WHERE (store_name, store_type) NOT IN (
      SELECT store_name, store_type FROM {{ this }}
  )

{% endif %}