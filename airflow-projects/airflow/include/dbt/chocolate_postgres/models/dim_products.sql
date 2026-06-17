{{ config(
    materialized='incremental',
    unique_key=['product_name', 'product_brand', 'product_category', 'product_weight'] 
) }}

WITH raw_products AS (
    SELECT 
        -- Generate the Surrogate Key by concatenating all parts of the composite natural key
        md5(cast(product_name as text) || cast(product_brand as text) || cast(product_category as text) || cast(product_weight as text)) AS product_sk,
        
        -- The Natural Keys and rest of the attributes
        product_name, 
        product_brand, 
        product_category, 
        cocoa_percent, 
        product_weight, 
        product_batch_id, 
        product_source_system,
        
        -- DWH audit column
        CURRENT_TIMESTAMP AS created_at
        
    FROM {{ source('raw_data', 'stg_dim_products') }}
)

SELECT * FROM raw_products

{% if is_incremental() %}

  -- Match on all four columns simultaneously to prevent duplicates
  WHERE (product_name, product_brand, product_category, product_weight) NOT IN (
      SELECT product_name, product_brand, product_category, product_weight 
      FROM {{ this }}
  )

{% endif %}