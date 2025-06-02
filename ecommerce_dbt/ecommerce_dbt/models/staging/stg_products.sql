{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
  product_id,
  sku,
  name,
  category,
  subcategory,
  brand,
  cost_price,
  selling_price,
  inventory_qty,
  weight,
  is_active,
  TIMESTAMP(created_at) as created_at,
  CURRENT_TIMESTAMP() as dbt_loaded_at
FROM {{ source('raw', 'products') }}