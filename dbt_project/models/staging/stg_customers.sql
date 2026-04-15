{{ config(materialized='view') }}
WITH source AS (
    SELECT * FROM raw.customers
),
cleaned AS (
    SELECT
        CAST(customer_id AS INTEGER)                AS customer_id,
        TRIM(name)                                  AS customer_name,
        LOWER(TRIM(email))                          AS email,
        TRIM(city)                                  AS city,
        TRIM(state)                                 AS state,
        CAST(created_at AS DATE)                    AS created_at,
        DATEDIFF('day',
            CAST(created_at AS DATE),
            CURRENT_DATE
        )                                           AS days_since_signup,
        CASE
            WHEN DATEDIFF('day', CAST(created_at AS DATE), CURRENT_DATE) <= 90
            THEN 'New'
            WHEN DATEDIFF('day', CAST(created_at AS DATE), CURRENT_DATE) <= 365
            THEN 'Regular'
            ELSE 'Veteran'
        END                                         AS customer_segment
    FROM source
    WHERE customer_id IS NOT NULL
      AND name        IS NOT NULL
      AND email       IS NOT NULL
)
SELECT * FROM cleaned