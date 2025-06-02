{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
  customer_id,
  first_name,
  last_name,
  email,
  phone,
  city,
  state,
  zip_code,
  DATE(join_date) as join_date,
  loyalty_tier,
  segment,
  credit_score,
  CURRENT_TIMESTAMP() as dbt_loaded_at
FROM {{ source('raw', 'customers') }}
WHERE customer_id IS NOT NULL  