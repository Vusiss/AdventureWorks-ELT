{{ config(materialized='table', schema='main') }}

SELECT
    territory_id                AS territory_key,
    territory_id,
    territory_name,
    country_region_code,
    country_name,
    territory_group,
    CAST(sales_ytd AS DECIMAL(18, 2))       AS sales_ytd,
    CAST(sales_last_year AS DECIMAL(18, 2)) AS sales_last_year
FROM {{ ref('stg_territory') }}
