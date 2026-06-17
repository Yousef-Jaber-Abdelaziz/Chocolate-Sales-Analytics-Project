{{ config(materialized='table') }}

SELECT 
    order_id,
    order_date,
    quantity,
    unit_price,
    discount,
    cost,
    revenue,
    profit,
    margin_pct,
    sales_batch_id,
    sales_source_system,
    bkey_customer,
    customer_age,
    customer_segment,
    store_name,
    store_type,
    store_city,
    product_name,
    product_category,
    product_brand,
    product_weight_g
FROM {{ ref('obt_chocolate_denormalized') }}
WHERE order_id IS NOT NULL
