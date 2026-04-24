{{ config(materialized='table', schema='staging') }}

-- Date spine covering the full range of actual orders (oldest → newest).
-- Uses SalesOrderHeader row numbers as a natural integer sequence generator
-- (task-specified ROW_NUMBER pattern; header count > date range in days).

WITH date_range AS (
    SELECT
        MIN(order_date) AS start_date,
        MAX(order_date) AS end_date
    FROM {{ ref('stg_sales_order') }}
),
nums AS (
    SELECT ROW_NUMBER() OVER (ORDER BY sales_order_id) - 1 AS n
    FROM {{ source('extract', 'sales_order_header') }}
)
SELECT CAST(DATEADD(day, n.n, dr.start_date) AS DATE) AS date_day
FROM   date_range AS dr
CROSS JOIN nums   AS n
WHERE  DATEADD(day, n.n, dr.start_date) <= dr.end_date
