
  
    
    

    create  table
      "chocolate_warehouse"."main"."stg_dim_customers__dbt_tmp"
  
    as (
      ﻿

SELECT DISTINCT
    bkey_customer,
    customer_age,
    age_group,
    customer_segment,
    customer_gender,
    customer_loyalty_member,
    customer_join_date,
    customer_batch_id,
    customer_source_system
FROM "chocolate_warehouse"."main"."obt_chocolate_denormalized"
WHERE bkey_customer IS NOT NULL
    );
  
  