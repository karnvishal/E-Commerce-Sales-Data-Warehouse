{{
  config(
    materialized='table',
    schema='core'
  )
}}

WITH customer_orders AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) as order_count,
    MIN(order_date) as first_order_date,
    MAX(order_date) as most_recent_order_date,
    SUM(total_amount) as lifetime_value
  FROM {{ ref('stg_orders') }}
  WHERE NOT is_guest
  GROUP BY 1
)

SELECT
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.phone,
  c.city,
  c.state,
  c.zip_code,
  c.join_date,
  c.loyalty_tier,
  c.segment,
  c.credit_score,
  co.order_count,
  co.first_order_date,
  co.most_recent_order_date,
  co.lifetime_value,
  DATE_DIFF(CURRENT_DATE(), c.join_date, DAY) as days_as_customer,
  CASE
    WHEN co.order_count IS NULL THEN 'New'
    WHEN co.order_count = 1 THEN 'One-Time'
    WHEN co.order_count BETWEEN 2 AND 5 THEN 'Repeat'
    ELSE 'Loyal'
  END as purchase_frequency_segment
FROM {{ ref('stg_customers') }} c
LEFT JOIN customer_orders co ON c.customer_id = co.customer_id