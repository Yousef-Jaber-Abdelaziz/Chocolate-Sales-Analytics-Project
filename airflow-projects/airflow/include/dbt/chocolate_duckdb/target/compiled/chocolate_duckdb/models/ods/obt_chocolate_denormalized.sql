

WITH clean_sales AS (
    SELECT 
        TRIM(order_id) AS order_id,
        order_date,
        CAST(quantity AS INT) AS quantity,
        CASE WHEN unit_price > 0 THEN unit_price ELSE NULL END AS unit_price,
        CASE WHEN discount BETWEEN 0 AND 1.0 THEN discount ELSE 0.0 END AS discount,
        cost,
        TRIM(product_id) AS product_id,
        TRIM(customer_id) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(batch_id) AS batch_id,
        TRIM(source_system) AS source_system
    FROM "chocolate_warehouse"."main"."raw_sales"
),

clean_products AS (
    SELECT 
        TRIM(product_id) AS product_id,
        TRIM(product_name) AS product_name,
        TRIM(brand) AS brand,
        TRIM(category) AS category,
        CASE WHEN cocoa_percent BETWEEN 0 AND 100 THEN cocoa_percent ELSE NULL END AS cocoa_percent,
        weight_g,
        TRIM(batch_id) AS batch_id,
        TRIM(source_system) AS source_system
    FROM "chocolate_warehouse"."main"."raw_products"
),

clean_customers AS (
    SELECT 
        TRIM(customer_id) AS customer_id,
        CASE WHEN age BETWEEN 10 AND 100 THEN age ELSE NULL END AS age,
        TRIM(gender) AS gender,
        loyalty_member,
        join_date,
        TRIM(batch_id) AS batch_id,
        TRIM(source_system) AS source_system
    FROM "chocolate_warehouse"."main"."raw_customers"
),

clean_stores AS (
    SELECT 
        TRIM(store_id) AS store_id,
        TRIM(store_name) AS store_name,
        TRIM(city) AS city,
        TRIM(country) AS country,
        TRIM(store_type) AS store_type,
        CASE WHEN TRIM(LOWER(store_type)) = 'online' THEN TRUE ELSE FALSE END AS is_online,
        TRIM(batch_id) AS batch_id,
        TRIM(source_system) AS source_system
    FROM "chocolate_warehouse"."main"."raw_stores"
),

derived_sales AS (
    SELECT 
        *,
        ROUND((quantity * unit_price) * (1 - discount), 2) AS calc_revenue,
        ROUND(((quantity * unit_price) * (1 - discount)) - cost, 2) AS calc_profit,
        CASE 
            WHEN unit_price < 5 THEN 'Budget'
            WHEN unit_price BETWEEN 5 AND 15 THEN 'Standard'
            WHEN unit_price > 15 THEN 'Premium'
            ELSE 'Unknown'
        END AS price_tier
    FROM clean_sales
)

SELECT 
    s.order_id,
    s.order_date,
    s.quantity,
    s.unit_price,
    s.discount,
    s.cost,
    s.calc_revenue AS revenue,
    s.calc_profit AS profit,
    
    CASE 
        WHEN s.calc_revenue > 0 THEN ROUND((s.calc_profit / s.calc_revenue) * 100.0, 2)
        ELSE 0.0 
    END AS margin_pct,

    s.price_tier,
    s.batch_id AS sales_batch_id,
    s.source_system AS sales_source_system,

    COALESCE(s.product_id, p.product_id) AS bkey_product,
    COALESCE(s.customer_id, c.customer_id) AS bkey_customer,
    COALESCE(s.store_id, st.store_id) AS bkey_store,

    c.age AS customer_age,
    CASE 
        WHEN c.age BETWEEN 10 AND 18 THEN '10-18'
        WHEN c.age BETWEEN 19 AND 35 THEN '19-35'
        WHEN c.age BETWEEN 36 AND 55 THEN '36-55'
        WHEN c.age > 55 THEN '56+'
        ELSE 'Unknown'
    END AS age_group,
    CASE 
        WHEN c.loyalty_member = 1 THEN 'Loyalty Member'
        ELSE 'Guest'
    END AS customer_segment,
    c.gender AS customer_gender,
    c.loyalty_member AS customer_loyalty_member,
    c.join_date AS customer_join_date,
    c.batch_id AS customer_batch_id,
    c.source_system AS customer_source_system,

    p.product_name,
    p.brand AS product_brand,
    p.category AS product_category,
    p.cocoa_percent,
    p.weight_g AS product_weight_g,
    p.batch_id AS product_batch_id,
    p.source_system AS product_source_system,

    st.store_name,
    st.city AS store_city,
    st.country AS store_country,
    st.store_type,
    st.is_online,
    st.batch_id AS store_batch_id,
    st.source_system AS store_source_system

FROM derived_sales s
FULL OUTER JOIN clean_products p ON s.product_id = p.product_id
FULL OUTER JOIN clean_customers c ON s.customer_id = c.customer_id
FULL OUTER JOIN clean_stores st ON s.store_id = st.store_id
WHERE s.product_id NOT IN ('P0000','P0201')