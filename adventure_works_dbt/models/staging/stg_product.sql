{{ config(materialized='table', schema='staging') }}

WITH src_product AS (
    SELECT
        product_id,
        name                                        AS product_name,
        product_number,
        {{ bool_cast('make_flag') }}                AS make_flag,
        {{ bool_cast('finished_goods_flag') }}      AS finished_goods_flag,

        -- Cleaning: replace NULL with descriptive defaults
        COALESCE(NULLIF(TRIM(color), ''), 'N/A')            AS color,
        COALESCE(standard_cost, 0.0)                        AS standard_cost,
        COALESCE(list_price, 0.0)                           AS list_price,
        COALESCE(NULLIF(TRIM(size), ''), 'N/A')             AS size,

        -- Standardise size unit: convert inches to cm, unify label
        CASE
            WHEN UPPER(TRIM(size_unit_measure_code)) = 'IN'
                THEN 'CM'
            WHEN size_unit_measure_code IS NULL OR TRIM(size_unit_measure_code) = ''
                THEN 'N/A'
            ELSE UPPER(TRIM(size_unit_measure_code))
        END                                                  AS size_unit,

        -- Standardise weight: convert LB → KG (1 lb ≈ 0.453592 kg)
        CASE
            WHEN UPPER(TRIM(weight_unit_measure_code)) = 'LB'
                THEN ROUND(COALESCE(weight, 0) * 0.453592, 4)
            ELSE COALESCE(weight, 0)
        END                                                  AS weight_kg,

        CASE
            WHEN weight_unit_measure_code IS NULL OR TRIM(weight_unit_measure_code) = ''
                THEN 'N/A'
            ELSE 'KG'
        END                                                  AS weight_unit,

        -- Standardise categorical flags with readable labels
        CASE UPPER(TRIM(product_line))
            WHEN 'R' THEN 'Road'
            WHEN 'M' THEN 'Mountain'
            WHEN 'T' THEN 'Touring'
            WHEN 'S' THEN 'Standard'
            ELSE 'N/A'
        END                                                  AS product_line,

        CASE UPPER(TRIM(class))
            WHEN 'L' THEN 'Low'
            WHEN 'M' THEN 'Medium'
            WHEN 'H' THEN 'High'
            ELSE 'N/A'
        END                                                  AS class,

        CASE UPPER(TRIM(style))
            WHEN 'W' THEN 'Womens'
            WHEN 'M' THEN 'Mens'
            WHEN 'U' THEN 'Universal'
            ELSE 'N/A'
        END                                                  AS style,

        product_subcategory_id,
        CAST(sell_start_date AS DATE)                       AS sell_start_date,
        CAST(sell_end_date   AS DATE)                       AS sell_end_date,
        CAST(discontinued_date AS DATE)                     AS discontinued_date
    FROM {{ source('extract', 'product') }}
),

src_subcategory AS (
    SELECT
        product_subcategory_id,
        name                AS subcategory_name,
        product_category_id
    FROM {{ source('extract', 'product_subcategory') }}
),

src_category AS (
    SELECT
        product_category_id,
        name AS category_name
    FROM {{ source('extract', 'product_category') }}
),

-- Aggregate ratings per product from the external CSV
src_ratings AS (
    SELECT
        CAST(productid AS INTEGER)              AS product_id,
        ROUND(AVG(CAST(rating_product AS FLOAT)), 2)    AS avg_rating_product,
        ROUND(AVG(CAST(rating_overall AS FLOAT)), 2)    AS avg_rating_overall,
        COUNT(*)                                AS review_count
    FROM {{ source('extract', 'product_rating') }}
    WHERE productid IS NOT NULL
    GROUP BY productid
),

joined AS (
    SELECT
        p.product_id,
        p.product_name,
        p.product_number,
        p.make_flag,
        p.finished_goods_flag,
        p.color,
        p.standard_cost,
        p.list_price,
        p.size,
        p.size_unit,
        p.weight_kg,
        p.weight_unit,
        p.product_line,
        p.class,
        p.style,
        COALESCE(sc.subcategory_name, 'N/A')    AS subcategory_name,
        COALESCE(cat.category_name,  'N/A')     AS category_name,
        p.sell_start_date,
        p.sell_end_date,
        p.discontinued_date,

        -- Enrichment: PROFIT = list_price - standard_cost
        ROUND(p.list_price - p.standard_cost, 2)        AS profit,

        -- Enrichment: MARGIN = (profit / list_price) * 100
        CASE
            WHEN p.list_price > 0
                THEN ROUND((p.list_price - p.standard_cost) / p.list_price * 100, 2)
            ELSE 0.0
        END                                              AS margin,

        -- Enrichment: ACTIVE — product is sold if no sell_end and no discontinuation
        CASE
            WHEN p.sell_end_date IS NULL AND p.discontinued_date IS NULL
                THEN 'ACTIVE'
            ELSE 'INACTIVE'
        END                                              AS active,

        -- Enrichment: SOLDFOR — months the product has been / was on offer
        -- Products with list_price = 0 are not for sale; mark as 0
        CASE
            WHEN p.list_price = 0 THEN 0
            ELSE {{ datediff_months('p.sell_start_date', 'COALESCE(p.sell_end_date, ' ~ today() ~ ')') }}
        END                                              AS sold_for,

        -- Enrichment: DISCRETEPRICE — tiered price bucket
        CASE
            WHEN p.list_price = 0       THEN 'N/A'
            WHEN p.list_price < 100     THEN 'LOW'
            WHEN p.list_price < 300     THEN 'MEDIUM'
            WHEN p.list_price < 500     THEN 'HIGH'
            ELSE                             'VERY HIGH'
        END                                              AS discrete_price,

        -- External ratings (CSV)
        COALESCE(r.avg_rating_product, 0.0)             AS avg_rating_product,
        COALESCE(r.avg_rating_overall, 0.0)             AS avg_rating_overall,
        COALESCE(r.review_count, 0)                     AS review_count

    FROM src_product           AS p
    LEFT JOIN src_subcategory  AS sc  ON p.product_subcategory_id = sc.product_subcategory_id
    LEFT JOIN src_category     AS cat ON sc.product_category_id   = cat.product_category_id
    LEFT JOIN src_ratings      AS r   ON p.product_id             = r.product_id
)

SELECT * FROM joined
