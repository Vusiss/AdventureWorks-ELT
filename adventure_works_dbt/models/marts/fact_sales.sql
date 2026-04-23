{{ config(materialized='table', schema='main') }}

-- Grain: one row per order line (SalesOrderDetail)
-- Facts: quantity, unit price, discount, line revenue
-- Degenerate dimensions: sales_order_id, sales_order_number

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
)

SELECT
    -- Surrogate key
    o.sales_order_detail_id                     AS sales_fact_key,

    -- Foreign keys to dimensions
    d.date_key                                  AS order_date_key,
    o.product_id                                AS product_key,
    -- Web orders have no salesperson; use -1 as "unknown" sentinel
    COALESCE(o.sales_person_id, -1)             AS salesperson_key,
    o.territory_id                              AS territory_key,

    -- Degenerate dimensions (order-level identifiers)
    o.sales_order_id,
    o.sales_order_number,
    o.online_order_flag,

    -- Measures
    CAST(o.order_qty AS INTEGER)                    AS order_qty,
    CAST(o.unit_price AS DECIMAL(10, 2))            AS unit_price,
    CAST(o.unit_price_discount AS DECIMAL(5, 4))    AS unit_price_discount,
    CAST(o.line_total AS DECIMAL(18, 4))            AS line_total,

    -- Order-level context (additive at header level only)
    CAST(o.sub_total AS DECIMAL(18, 4))             AS order_sub_total,
    CAST(o.tax_amt   AS DECIMAL(18, 4))             AS order_tax_amt,
    CAST(o.freight   AS DECIMAL(18, 4))             AS order_freight,
    CAST(o.total_due AS DECIMAL(18, 4))             AS order_total_due

FROM orders               AS o
JOIN dim_date             AS d   ON o.order_date = d.full_date
-- Products in orders should always exist in the dimension
LEFT JOIN dim_product     AS dp  ON o.product_id = dp.product_key
-- Salesperson may be NULL for web orders
LEFT JOIN dim_salesperson AS ds  ON o.sales_person_id = ds.salesperson_key
LEFT JOIN dim_territory   AS dt  ON o.territory_id = dt.territory_key
