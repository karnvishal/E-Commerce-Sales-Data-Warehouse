{{
  config(
    materialized='table',
    schema='core',
    unique_key='product_id'
  )
}}

WITH product_sales AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id) as order_count,
        SUM(quantity) as total_quantity_sold,
        SUM(total_price) as total_revenue,
        AVG(unit_price) as avg_selling_price,
        MIN(order_date) as first_sale_date,
        MAX(order_date) as last_sale_date
    FROM {{ ref('fact_orders') }}
    GROUP BY 1
),

product_returns AS (
    SELECT
        product_id,
        COUNT(DISTINCT CASE WHEN is_returned THEN order_id END) as return_count,
        SUM(CASE WHEN is_returned THEN quantity ELSE 0 END) as total_quantity_returned,
        SUM(CASE WHEN is_returned THEN total_price ELSE 0 END) as total_return_amount
    FROM {{ ref('fact_orders') }}
    GROUP BY 1
),

inventory_stats AS (
    SELECT
        product_id,
        SUM(quantity) as net_inventory_change,
        SUM(CASE WHEN movement_type = 'purchase' THEN quantity ELSE 0 END) as total_purchased,
        SUM(CASE WHEN movement_type = 'sale' THEN quantity ELSE 0 END) as total_sold,
        SUM(CASE WHEN movement_type = 'return' THEN quantity ELSE 0 END) as total_returned
    FROM {{ ref('stg_inventory_movements') }}
    GROUP BY 1
)

SELECT
    p.product_id,
    p.sku,
    p.name,
    p.category,
    p.subcategory,
    p.brand,
    p.cost_price,
    p.selling_price,
    p.weight,
    p.created_at,
    p.is_active,
    COALESCE(ps.order_count, 0) as order_count,
    COALESCE(ps.total_quantity_sold, 0) as total_quantity_sold,
    COALESCE(ps.total_revenue, 0) as total_revenue,
    COALESCE(ps.avg_selling_price, p.selling_price) as avg_selling_price,
    COALESCE(pr.return_count, 0) as return_count,
    COALESCE(pr.total_quantity_returned, 0) as total_quantity_returned,
    COALESCE(pr.total_return_amount, 0) as total_return_amount,
    SAFE_DIVIDE(
        COALESCE(pr.return_count, 0),
        COALESCE(ps.order_count, 1)
    ) as return_rate,
    COALESCE(ps.first_sale_date, NULL) as first_sale_date,
    COALESCE(ps.last_sale_date, NULL) as last_sale_date,
    COALESCE(i.net_inventory_change, 0) as net_inventory_change,
    COALESCE(i.total_purchased, 0) as total_purchased,
    COALESCE(i.total_sold, 0) as total_sold,
    COALESCE(i.total_returned, 0) as total_returned,
    ps.product_id as fact_orders_product_id,
    p.inventory_qty,
    DATE_DIFF(CURRENT_DATE(), DATE(p.created_at), DAY) as days_since_creation,
    CASE
        WHEN ps.last_sale_date IS NULL THEN 'Never Sold'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(ps.last_sale_date), DAY) <= 30 THEN 'Recently Sold'
        WHEN DATE_DIFF(CURRENT_DATE(), DATE(ps.last_sale_date), DAY) <= 90 THEN 'Warm'
        ELSE 'Cold'
    END as sales_status,
    CASE
        WHEN p.category IN ('Electronics', 'Home') THEN 'High Value'
        WHEN p.category IN ('Clothing', 'Beauty') THEN 'Medium Value'
        ELSE 'Low Value'
    END as value_segment
FROM {{ ref('stg_products') }} p
LEFT JOIN product_sales ps ON p.product_id = ps.product_id
LEFT JOIN product_returns pr ON p.product_id = pr.product_id
LEFT JOIN inventory_stats i ON p.product_id = i.product_id