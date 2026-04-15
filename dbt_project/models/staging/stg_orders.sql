{{ config(materialized='view') }}
WITH source AS (
    SELECT * FROM raw.orders

),
cleaned AS (
    SELECT
        CAST(order_id    AS INTEGER)  AS order_id,
        CAST(customer_id AS INTEGER)  AS customer_id,
        CAST(product_id  AS INTEGER)  AS product_id,
        CAST(quantity    AS INTEGER)  AS quantity,
        CAST(amount AS DECIMAL(10,2)) AS amount,
        LOWER(TRIM(status))           AS status,
        CAST(order_date AS DATE)      AS order_date,
        EXTRACT(YEAR  FROM CAST(order_date AS DATE)) AS order_year,
        EXTRACT(MONTH FROM CAST(order_date AS DATE)) AS order_month,
        CASE
            WHEN LOWER(status) = 'completed'  THEN true
            ELSE false
        END AS is_completed,
        CASE
            WHEN LOWER(status) = 'cancelled'  THEN true
            ELSE false
        END AS is_cancelled,
        CASE
            WHEN LOWER(status) = 'completed'
            THEN CAST(amount AS DECIMAL(10,2))
            ELSE 0
        END AS completed_revenue

    FROM source

    WHERE order_id    IS NOT NULL
      AND customer_id IS NOT NULL
      AND amount      IS NOT NULL
      AND amount      > 0

)
SELECT * FROM cleaned