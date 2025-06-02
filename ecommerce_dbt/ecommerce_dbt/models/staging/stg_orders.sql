{{
  config(
    materialized='view',
    schema='staging'
  )
}}

SELECT
  order_id,
  customer_id,
  is_guest,
  TIMESTAMP(order_date) as order_timestamp,
  DATE(order_date) as order_date,
  status as order_status,
  total_amount,
  shipping_cost,
  payment_method,
  payment_status
FROM {{ source('raw', 'orders') }}