{{ config(materialized='table', schema='staging') }}

-- Date spine covering the full range of actual orders (oldest → newest).
-- MSSQL version uses the row-number pattern from the task specification
-- (ROW_NUMBER OVER SalesOrderHeader rows as a natural number generator).

{% if target.type == 'duckdb' %}

WITH date_range AS (
    SELECT
        MIN(order_date) AS start_date,
        MAX(order_date) AS end_date
    FROM {{ ref('stg_sales_order') }}
)
SELECT CAST(range AS DATE) AS date_day
FROM date_range,
     range(start_date, end_date + INTERVAL '1' DAY, INTERVAL '1' DAY)

{% else %}

WITH date_range AS (
    SELECT
        MIN(order_date) AS start_date,
        MAX(order_date) AS end_date
    FROM {{ ref('stg_sales_order') }}
),
-- Use SalesOrderHeader row numbers as an integer sequence generator
-- (task-specified approach; header count > date range in days)
nums AS (
    SELECT ROW_NUMBER() OVER (ORDER BY sales_order_id) - 1 AS n
    FROM {{ source('extract', 'sales_order_header') }}
)
SELECT CAST(DATEADD(day, n.n, dr.start_date) AS DATE) AS date_day
FROM   date_range AS dr
CROSS JOIN nums   AS n
WHERE  DATEADD(day, n.n, dr.start_date) <= dr.end_date

{% endif %}
