{{
  config(
    materialized='view',
    schema='staging',
    alias='stg_inventory_movements'
  )
}}

WITH base_movements AS (
    SELECT
        movement_id,
        product_id,
        TIMESTAMP(movement_date) as movement_timestamp,
        DATE(movement_date) as movement_date,
        quantity,
        movement_type,
        reference_id,
        notes,
        CURRENT_TIMESTAMP() as loaded_at
    FROM {{ source('raw', 'inventory_movements') }}
),

enriched_movements AS (
    SELECT
        m.*,
        p.sku,
        p.name as product_name,
        p.category,
        CASE
            WHEN m.movement_type = 'purchase' THEN 'Inbound'
            WHEN m.movement_type = 'sale' THEN 'Outbound'
            WHEN m.movement_type = 'return' THEN 'Inbound'
            WHEN m.movement_type = 'adjustment' AND m.quantity > 0 THEN 'Positive Adjustment'
            WHEN m.movement_type = 'adjustment' AND m.quantity < 0 THEN 'Negative Adjustment'
            ELSE 'Other'
        END as movement_direction,
        ABS(m.quantity) as absolute_quantity,
        ROW_NUMBER() OVER (PARTITION BY m.product_id ORDER BY m.movement_timestamp) as movement_sequence_num
    FROM base_movements m
    LEFT JOIN {{ ref('stg_products') }} p
        ON m.product_id = p.product_id
)

SELECT
    movement_id,
    product_id,
    sku,
    product_name,
    category,
    movement_timestamp,
    movement_date,
    quantity,
    absolute_quantity,
    movement_type,
    movement_direction,
    reference_id,
    notes,
    loaded_at,
    movement_sequence_num,
    -- Inventory snapshot calculations
    SUM(quantity) OVER (
        PARTITION BY product_id
        ORDER BY movement_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_inventory_balance,
    LAG(movement_timestamp, 1) OVER (
        PARTITION BY product_id
        ORDER BY movement_timestamp
    ) as previous_movement_time,
    LEAD(movement_timestamp, 1) OVER (
        PARTITION BY product_id
        ORDER BY movement_timestamp
    ) as next_movement_time
FROM enriched_movements