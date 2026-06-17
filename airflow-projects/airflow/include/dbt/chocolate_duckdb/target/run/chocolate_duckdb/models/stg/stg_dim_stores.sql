
  
    
    

    create  table
      "chocolate_warehouse"."main"."stg_dim_stores__dbt_tmp"
  
    as (
      ﻿

SELECT DISTINCT
    store_name,
    store_type,
    is_online,
    store_batch_id,
    store_source_system
FROM "chocolate_warehouse"."main"."obt_chocolate_denormalized"
WHERE store_name IS NOT NULL
    );
  
  