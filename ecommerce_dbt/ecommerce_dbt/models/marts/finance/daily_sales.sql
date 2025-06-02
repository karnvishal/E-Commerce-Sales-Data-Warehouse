{{
  config(
    materialized='table',
    schema='finance',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

SELECT
  DATE(order_date) as order_date,
  p.category,
  p.brand,
  COUNT(DISTINCT o.order_id) as order_count,
  COUNT(DISTINCT o.customer_id) as customer_count,
  SUM(oi.quantity) as total_items,
  SUM(oi.total_price) as gross_revenue,
  SUM(oi.total_price * oi.discount_pct) as total_discounts,
  SUM(oi.total_price * (1 - oi.discount_pct)) as net_revenue,
  SUM(p.cost_price * oi.quantity) as total_cost,
  SUM(oi.total_price * (1 - oi.discount_pct) - (p.cost_price * oi.quantity)) as gross_profit,
  SUM(CASE WHEN oi.return_status != 'not_returned' THEN oi.total_price ELSE 0 END) as returned_amount,
  COUNT(DISTINCT CASE WHEN oi.return_status != 'not_returned' THEN oi.order_item_id END) as returned_items,
  SAFE_DIVIDE(
    COUNT(DISTINCT CASE WHEN oi.return_status != 'not_returned' THEN oi.order_item_id END),
    COUNT(DISTINCT oi.order_item_id)
  ) as return_rate
FROM {{ ref('stg_order_items') }} oi
JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
JOIN {{ ref('stg_products') }} p ON oi.product_id = p.product_id
GROUP BY 1, 2, 3