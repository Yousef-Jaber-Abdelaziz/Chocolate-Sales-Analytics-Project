{{ config(materialized='table') }}

SELECT DISTINCT
    product_name,
    product_brand,
    product_category,
    cocoa_percent,
    product_weight_g AS product_weight,
    product_batch_id,
    product_source_system
FROM {{ ref('obt_chocolate_denormalized') }}
WHERE product_name IS NOT NULL
