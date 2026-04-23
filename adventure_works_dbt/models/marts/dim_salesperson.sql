{{ config(materialized='table', schema='main') }}

SELECT
    salesperson_id              AS salesperson_key,
    salesperson_id,
    territory_id,
    full_name,
    first_name,
    middle_name,
    last_name,
    CAST(sales_quota AS DECIMAL(18, 2))         AS sales_quota,
    CAST(bonus AS DECIMAL(10, 2))               AS bonus,
    CAST(commission_pct AS DECIMAL(5, 4))       AS commission_pct,
    CAST(sales_ytd AS DECIMAL(18, 2))           AS sales_ytd,
    CAST(sales_last_year AS DECIMAL(18, 2))     AS sales_last_year
FROM {{ ref('stg_salesperson') }}
