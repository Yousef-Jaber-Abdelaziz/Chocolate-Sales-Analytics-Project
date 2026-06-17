{{ config(
    materialized='incremental',
    pre_hook="{% if is_incremental() %} DELETE FROM {{ this }} WHERE batch_id IN (SELECT DISTINCT sales_batch_id FROM {{ source('raw_data', 'stg_fact_sales') }}); {% endif %}"
) }}

WITH stg_sales AS (
    SELECT * FROM {{ source('raw_data', 'stg_fact_sales') }}
),

dim_customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

dim_locations AS (
    SELECT * FROM {{ ref('dim_locations') }}
),

dim_stores AS (
    SELECT * FROM {{ ref('dim_stores') }}
),

dim_products AS (
    SELECT * FROM {{ ref('dim_products') }}
),

dim_calendar AS (
    -- Direct query to the schema as we established earlier
    SELECT * FROM dwh.dwh_dim_calendar
)

SELECT
    -- 1. PRIMARY KEY
    md5(cast(s.order_id as text)) AS sales_sk,
    
    -- 2. MEASURES & FACTS
    s.quantity,
    s.unit_price,
    s.discount,
    s.cost,
    s.revenue,
    s.profit,
    s.margin_pct,

    -- 3. FOREIGN KEYS & LOOKUPS
    c.dbt_scd_id AS customer_id,
    loc.location_sk,
    str.store_sk,
    prd.product_sk,
    cal.date_id AS order_date_id,
    s.order_id, -- Degenerate dimension (Kept for traceability to the source system)

    -- 4. AUDIT COLUMNS
    s.sales_batch_id AS batch_id,
    s.sales_source_system AS source_system,
    CURRENT_TIMESTAMP AS created_at

FROM stg_sales s

-- CUSTOMER LOOKUP (SCD Type 2 Fixes Applied)
LEFT JOIN dim_customers c 
    ON s.bkey_customer = c.bkey_customer 
    AND (
        -- Condition A: Standard SCD2 lookup for ongoing daily loads
        (s.order_date >= c.dbt_valid_from AND s.order_date < coalesce(c.dbt_valid_to, '2099-12-31'::timestamp))
        OR 
        -- Condition B (The Trap Door): If the sale is older than our first snapshot, link to the currently active record
        (s.order_date < c.dbt_valid_from AND c.dbt_valid_to IS NULL)
    )

-- LOCATION LOOKUP
LEFT JOIN dim_locations loc 
    ON s.store_city = loc.city

-- STORE LOOKUP
LEFT JOIN dim_stores str 
    ON s.store_name = str.store_name 
    AND s.store_type = str.store_type

-- PRODUCT LOOKUP
LEFT JOIN dim_products prd 
    ON s.product_name = prd.product_name 
    AND s.product_category = prd.product_category 
    AND s.product_weight_g = prd.product_weight
    AND s.product_brand = prd.product_brand 

-- CALENDAR LOOKUP
LEFT JOIN dim_calendar cal 
    ON DATE(s.order_date) = cal.full_date