{{ config(
    materialized='view',
    schema='staging')
     }}

select
        order_item_id,
        order_id,
        product_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as FLOAT64) as unit_price,
        cast(discount_pct as FLOAT64) as discount_pct,
        cast(total_price as FLOAT64) as total_price,
        return_status,
        return_reason
    from {{ source('raw', 'order_items') }}