{{ config(materialized='table', schema='staging') }}

WITH src_salesperson AS (
    SELECT
        business_entity_id,
        territory_id,
        COALESCE(sales_quota, 0.0)      AS sales_quota,
        COALESCE(bonus, 0.0)            AS bonus,
        COALESCE(commission_pct, 0.0)   AS commission_pct,
        COALESCE(sales_ytd, 0.0)        AS sales_ytd,
        COALESCE(sales_last_year, 0.0)  AS sales_last_year
    FROM {{ source('extract', 'sales_person') }}
),

src_person AS (
    SELECT
        business_entity_id,
        COALESCE(NULLIF(TRIM(title), ''), '')           AS title,
        TRIM(first_name)                                AS first_name,
        COALESCE(NULLIF(TRIM(middle_name), ''), '')     AS middle_name,
        TRIM(last_name)                                 AS last_name,
        COALESCE(NULLIF(TRIM(suffix), ''), '')          AS suffix
    FROM {{ source('extract', 'person') }}
    WHERE person_type = 'SP'
)

SELECT
    sp.business_entity_id           AS salesperson_id,
    sp.territory_id,

    -- Enrichment: standardised full name
    TRIM(
        CASE
            WHEN p.title <> '' THEN p.title || ' '
            ELSE ''
        END
        || p.first_name || ' '
        || CASE
            WHEN p.middle_name <> '' THEN p.middle_name || ' '
            ELSE ''
        END
        || p.last_name
        || CASE
            WHEN p.suffix <> '' THEN ' ' || p.suffix
            ELSE ''
        END
    )                               AS full_name,

    p.first_name,
    p.middle_name,
    p.last_name,

    sp.sales_quota,
    sp.bonus,
    sp.commission_pct,
    sp.sales_ytd,
    sp.sales_last_year

FROM src_salesperson AS sp
LEFT JOIN src_person AS p ON sp.business_entity_id = p.business_entity_id
