{{ config(materialized='table') }}
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
joined AS (
    SELECT
        o.order_id,
        o.order_date,
        o.order_year,
        o.order_month,
        o.quantity,
        o.amount,
        o.status,
        o.is_completed,
        o.is_cancelled,
        o.completed_revenue,
        c.customer_id,
        c.customer_name,
        c.city,
        c.state,
        c.customer_segment,
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        p.price_tier,
        p.stock_status
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN products  p ON o.product_id  = p.product_id
),
final AS (
    SELECT
        *,
        SUM(completed_revenue) OVER (
            PARTITION BY category
        )                               AS category_total_revenue,
        RANK() OVER (
            PARTITION BY category
            ORDER BY amount DESC
        )                               AS rank_in_category
    FROM joined
)
SELECT * FROM final