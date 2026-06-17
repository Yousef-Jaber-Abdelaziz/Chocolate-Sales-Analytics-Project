
  
    
    

    create  table
      "chocolate_warehouse"."main"."stg_dim_locations__dbt_tmp"
  
    as (
      ﻿

WITH unique_cities AS (
    SELECT DISTINCT store_city
    FROM "chocolate_warehouse"."main"."obt_chocolate_denormalized"
    WHERE store_city IS NOT NULL
),

clean_geo_seed AS (
    -- FIXED: Changed to raw_locations (plural)
    SELECT 
        city_ascii, 
        lat, 
        lng, 
        country, 
        iso2, 
        iso3
    FROM "chocolate_warehouse"."main"."raw_locations"
    QUALIFY ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(city_ascii)) ORDER BY iso2) = 1
)

SELECT 
    c.store_city AS city,
    g.lat,
    g.lng,
    g.country,
    g.iso2,
    g.iso3
FROM unique_cities c
LEFT JOIN clean_geo_seed g
    ON LOWER(TRIM(c.store_city)) = LOWER(TRIM(g.city_ascii))
    );
  
  