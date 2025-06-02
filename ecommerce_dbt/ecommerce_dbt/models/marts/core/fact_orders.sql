{{
  config(
    materialized='incremental',
    schema='core',
    unique_key='order_item_id',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

SELECT
  oi.order_item_id,
  o.order_id,
  o.customer_id,
  p.product_id,
  p.category,
  p.brand,
  o.order_date,
  o.order_status,
  oi.quantity,
  oi.unit_price,
  oi.discount_pct,
  oi.total_price,
  p.cost_price * oi.quantity as total_cost,
  oi.total_price - (p.cost_price * oi.quantity) as gross_profit,
  oi.return_status,
  oi.return_reason,
  o.payment_method,
  o.shipping_cost,
  CASE 
    WHEN oi.return_status != 'not_returned' THEN TRUE 
    ELSE FALSE 
  END as is_returned
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
JOIN {{ ref('stg_products') }} p ON oi.product_id = p.product_id

{% if is_incremental() %}
  WHERE o.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
{% endif %}