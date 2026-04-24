{{ config(materialized='table', schema='main') }}

-- Grain: one row per order line (SalesOrderDetail).
-- Only shipped orders (status = 5).
-- line_total_pln: line revenue converted to PLN using NBP USD/PLN rate
--   for the order date (weekends/holidays carry forward last known rate).
-- rate_change_direction: whether the USD/PLN rate rose or fell vs previous
--   published rate on that order day.

WITH orders AS (
    SELECT
        sales_order_detail_id,
        sales_order_id,
        sales_order_number,
        order_date,
        online_order_flag,
        product_id,
        sales_person_id,
        territory_id,
        order_qty,
        unit_price,
        unit_price_discount,
        line_total,
        sub_total,
        tax_amt,
        freight,
        total_due
    FROM {{ ref('stg_sales_order') }}
),

dim_date AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

dim_product AS (
    SELECT product_key FROM {{ ref('dim_product') }}
),

dim_salesperson AS (
    SELECT salesperson_key FROM {{ ref('dim_salesperson') }}
),

dim_territory AS (
    SELECT territory_key FROM {{ ref('dim_territory') }}
),

exchange AS (
    SELECT
        date_day,
        usd_pln_rate,
        rate_change_direction
    FROM {{ ref('stg_exchange_rate') }}
)

SELECT
    -- Surrogate key
    o.sales_order_detail_id                         AS sales_fact_key,

    -- Foreign keys
    d.date_key                                      AS order_date_key,
    o.product_id                                    AS product_key,
    COALESCE(o.sales_person_id, -1)                 AS salesperson_key,
    o.territory_id                                  AS territory_key,

    -- Degenerate dimensions
    o.sales_order_id,
    o.sales_order_number,
    o.online_order_flag,

    -- Measures (USD)
    CAST(o.order_qty             AS INTEGER)            AS order_qty,
    CAST(o.unit_price            AS DECIMAL(10, 2))     AS unit_price,
    CAST(o.unit_price_discount   AS DECIMAL(5, 4))      AS unit_price_discount,
    CAST(o.line_total            AS DECIMAL(18, 4))     AS line_total,

    -- Measure in PLN (line revenue × NBP USD/PLN rate on the order date)
    CAST(
        o.line_total * COALESCE(er.usd_pln_rate, 0)
        AS DECIMAL(18, 4)
    )                                                   AS line_total_pln,

    -- Exchange rate context
    CAST(er.usd_pln_rate AS DECIMAL(10, 4))             AS usd_pln_rate,
    COALESCE(er.rate_change_direction, 'UNKNOWN')       AS rate_change_direction,

    -- Order-level context (additive at header level only)
    CAST(o.sub_total   AS DECIMAL(18, 4))               AS order_sub_total,
    CAST(o.tax_amt     AS DECIMAL(18, 4))               AS order_tax_amt,
    CAST(o.freight     AS DECIMAL(18, 4))               AS order_freight,
    CAST(o.total_due   AS DECIMAL(18, 4))               AS order_total_due

FROM orders               AS o
JOIN    dim_date          AS d   ON o.order_date = d.full_date
LEFT JOIN dim_product     AS dp  ON o.product_id = dp.product_key
LEFT JOIN dim_salesperson AS ds  ON o.sales_person_id = ds.salesperson_key
LEFT JOIN dim_territory   AS dt  ON o.territory_id = dt.territory_key
LEFT JOIN exchange        AS er  ON o.order_date = er.date_day
