{% snapshot dim_customers %}

{{
    config(
      target_schema='dwh',
      unique_key='bkey_customer',      
      strategy='check',             
      check_cols=['age', 'age_group', 'customer_segment', 'gender', 'loyalty_member', 'join_date']               
    )
}}

-- Pull directly from your raw staging table
SELECT 
      bkey_customer,
      customer_age AS age,
      age_group,
      customer_segment,
      customer_gender AS gender,
      customer_loyalty_member AS loyalty_member,
      customer_join_date AS join_date,
      customer_batch_id AS batch_id,
      customer_source_system AS source_system
    
FROM {{ source('raw_data', 'stg_dim_customers') }}

{% endsnapshot %}