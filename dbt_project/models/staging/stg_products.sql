{{ config(materialized='view') }}
WITH source AS (
    SELECT * FROM raw.products
),
cleaned AS (
    SELECT
        CAST(product_id AS INTEGER)                 AS product_id,
        TRIM(name)                                  AS product_name,
        TRIM(category)                              AS category,
        CAST(price AS DECIMAL(10,2))                AS price,
        CAST(stock AS INTEGER)                      AS stock,
        CASE
            WHEN CAST(price AS DECIMAL) < 50        THEN 'Budget'
            WHEN CAST(price AS DECIMAL) < 200       THEN 'Mid-Range'
            ELSE 'Premium'
        END                                         AS price_tier,
        CASE
            WHEN CAST(stock AS INTEGER) = 0         THEN 'Out of Stock'
            WHEN CAST(stock AS INTEGER) < 10        THEN 'Low Stock'
            ELSE 'In Stock'
        END                                         AS stock_status
    FROM source
    WHERE product_id IS NOT NULL
      AND price      IS NOT NULL
      AND price      > 0
)
SELECT * FROM cleaned