{{ config(materialized='table') }}
WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customer_orders AS (
    SELECT
        customer_id,
        COUNT(order_id)                          AS total_orders,
        COUNT(CASE WHEN is_completed THEN 1 END) AS completed_orders,
        COUNT(CASE WHEN is_cancelled THEN 1 END) AS cancelled_orders,
        SUM(amount)                              AS total_spend,
        SUM(completed_revenue)                   AS total_completed_spend,
        AVG(amount)                              AS avg_order_value,
        MIN(order_date)                          AS first_order_date,
        MAX(order_date)                          AS last_order_date,
        DATEDIFF('day',
            MIN(order_date),
            MAX(order_date)
        )                                        AS customer_lifespan_days
    FROM orders
    GROUP BY customer_id
),
joined AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.email,
        c.city,
        c.state,
        c.created_at,
        c.customer_segment,
        c.days_since_signup,
        COALESCE(co.total_orders, 0)           AS total_orders,
        COALESCE(co.completed_orders, 0)       AS completed_orders,
        COALESCE(co.cancelled_orders, 0)       AS cancelled_orders,
        COALESCE(co.total_spend, 0)            AS total_spend,
        COALESCE(co.total_completed_spend, 0)  AS total_completed_spend,
        COALESCE(co.avg_order_value, 0)        AS avg_order_value,
        co.first_order_date,
        co.last_order_date,
        COALESCE(co.customer_lifespan_days, 0) AS customer_lifespan_days,
        CASE
            WHEN COALESCE(co.total_completed_spend, 0) >= 2000 THEN 'High Value'
            WHEN COALESCE(co.total_completed_spend, 0) >= 500  THEN 'Mid Value'
            WHEN COALESCE(co.total_completed_spend, 0) > 0     THEN 'Low Value'
            ELSE 'No Orders'
        END                                    AS customer_value_tier
    FROM customers c
    LEFT JOIN customer_orders co
        ON c.customer_id = co.customer_id
)
SELECT * FROM joined