{{
  config(
    materialized='table',
    schema='marketing'
  )
}}

WITH customer_metrics AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    DATE_DIFF(MAX(order_date), MIN(order_date), DAY) + 1 as active_days,
    COUNT(DISTINCT order_id) / (DATE_DIFF(MAX(order_date), MIN(order_date), DAY) + 1) as orders_per_day,
    DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) as days_since_last_order
  FROM {{ ref('stg_orders') }}
  WHERE NOT is_guest
  GROUP BY 1
),

customer_segments AS (
  SELECT
    cm.*,
    c.first_name,
    c.last_name,
    c.join_date,
    c.loyalty_tier,
    CASE
      WHEN days_since_last_order <= 30 THEN 'Active'
      WHEN days_since_last_order <= 90 THEN 'Lapsing'
      ELSE 'Inactive'
    END as activity_segment,
    CASE
      WHEN total_revenue < 100 THEN 'Low Value'
      WHEN total_revenue < 500 THEN 'Medium Value'
      ELSE 'High Value'
    END as value_segment,
    total_revenue / NULLIF(active_days, 0) * 365 as projected_annual_value
  FROM customer_metrics cm
  JOIN {{ ref('stg_customers') }} c ON cm.customer_id = c.customer_id
)

SELECT
  *,
  CASE
    WHEN activity_segment = 'Active' AND value_segment = 'High Value' THEN 'Champion'
    WHEN activity_segment = 'Active' AND value_segment = 'Medium Value' THEN 'Loyal'
    WHEN activity_segment = 'Active' AND value_segment = 'Low Value' THEN 'Potential'
    WHEN activity_segment = 'Lapsing' AND value_segment IN ('High Value', 'Medium Value') THEN 'At Risk'
    WHEN activity_segment = 'Inactive' AND value_segment = 'High Value' THEN 'Can\'t Lose'
    ELSE 'Low Priority'
  END as lifecycle_stage
FROM customer_segments