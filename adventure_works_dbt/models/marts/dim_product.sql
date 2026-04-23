{{ config(materialized='table', schema='main') }}

SELECT
    -- Surrogate key (uses natural key directly — ProductID is stable in AW)
    product_id                  AS product_key,

    -- Business / natural key
    product_id,
    product_number,

    -- Descriptive attributes
    product_name,
    category_name,
    subcategory_name,
    product_line,
    class,
    style,
    color,
    size,
    size_unit,
    CAST(weight_kg AS DECIMAL(10, 4))   AS weight_kg,
    weight_unit,

    -- Pricing
    CAST(list_price AS DECIMAL(10, 2))      AS list_price,
    CAST(standard_cost AS DECIMAL(10, 2))   AS standard_cost,
    CAST(profit AS DECIMAL(10, 2))          AS profit,
    CAST(margin AS DECIMAL(7, 2))           AS margin_pct,
    discrete_price,

    -- Lifecycle
    active,
    sell_start_date,
    sell_end_date,
    CAST(sold_for AS INTEGER)               AS sold_for_months,

    -- External ratings
    CAST(avg_rating_product AS DECIMAL(5, 2))   AS avg_rating_product,
    CAST(avg_rating_overall AS DECIMAL(5, 2))   AS avg_rating_overall,
    CAST(review_count AS INTEGER)               AS review_count

FROM {{ ref('stg_product') }}
