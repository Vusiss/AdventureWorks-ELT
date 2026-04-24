{{ config(materialized='table', schema='main') }}

SELECT
    sp.salesperson_id                               AS salesperson_key,
    sp.salesperson_id,

    sp.full_name,
    sp.first_name,
    sp.middle_name,
    sp.last_name,

    -- Denormalised territory attributes
    sp.territory_id,
    COALESCE(t.territory_name,  'N/A')              AS territory_name,
    COALESCE(t.country_name,    'N/A')              AS country_name,
    COALESCE(t.territory_group, 'N/A')              AS territory_group,

    CAST(sp.sales_quota    AS DECIMAL(18, 2))       AS sales_quota,
    CAST(sp.bonus          AS DECIMAL(10, 2))       AS bonus,
    CAST(sp.commission_pct AS DECIMAL(5, 4))        AS commission_pct,
    CAST(sp.sales_ytd      AS DECIMAL(18, 2))       AS sales_ytd,
    CAST(sp.sales_last_year AS DECIMAL(18, 2))      AS sales_last_year

FROM {{ ref('stg_salesperson') }}  AS sp
LEFT JOIN {{ ref('stg_territory') }} AS t ON sp.territory_id = t.territory_id
