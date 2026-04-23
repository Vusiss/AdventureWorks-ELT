{{ config(materialized='table', schema='staging') }}

WITH src_territory AS (
    SELECT
        territory_id,
        TRIM(name)              AS territory_name,
        country_region_code,
        -- DLT maps SQL Server keyword "group" → "group" (quoted in MSSQL)
        "group"                 AS territory_group,
        COALESCE(sales_ytd, 0.0)        AS sales_ytd,
        COALESCE(sales_last_year, 0.0)  AS sales_last_year
    FROM {{ source('extract', 'sales_territory') }}
),

src_country AS (
    SELECT
        country_region_code,
        TRIM(name) AS country_name
    FROM {{ source('extract', 'country_region') }}
)

SELECT
    t.territory_id,
    t.territory_name,
    t.country_region_code,
    COALESCE(c.country_name, t.country_region_code) AS country_name,
    t.territory_group,
    t.sales_ytd,
    t.sales_last_year
FROM src_territory AS t
LEFT JOIN src_country AS c ON t.country_region_code = c.country_region_code
