{{ config(materialized='table', schema='staging') }}

WITH src_header AS (
    SELECT
        sales_order_id,
        sales_order_number,
        CAST(order_date AS DATE)        AS order_date,
        CAST(due_date   AS DATE)        AS due_date,
        CAST(ship_date  AS DATE)        AS ship_date,
        status,
        CAST(online_order_flag AS BOOLEAN)  AS online_order_flag,
        customer_id,
        -- NULL sales_person_id means the order came in via the web (no salesperson)
        sales_person_id,
        territory_id,
        COALESCE(sub_total, 0.0)        AS sub_total,
        COALESCE(tax_amt,   0.0)        AS tax_amt,
        COALESCE(freight,   0.0)        AS freight,
        COALESCE(total_due, 0.0)        AS total_due
    FROM {{ source('extract', 'sales_order_header') }}
    WHERE status = 5  -- 5 = Shipped (completed orders only)
),

src_detail AS (
    SELECT
        sales_order_id,
        sales_order_detail_id,
        product_id,
        COALESCE(order_qty, 0)              AS order_qty,
        COALESCE(unit_price, 0.0)           AS unit_price,
        COALESCE(unit_price_discount, 0.0)  AS unit_price_discount,
        -- Recompute line_total to ensure consistency
        ROUND(
            COALESCE(order_qty, 0)
            * COALESCE(unit_price, 0.0)
            * (1.0 - COALESCE(unit_price_discount, 0.0)),
            4
        )                                   AS line_total
    FROM {{ source('extract', 'sales_order_detail') }}
)

SELECT
    d.sales_order_detail_id,
    d.sales_order_id,
    h.sales_order_number,
    h.order_date,
    h.due_date,
    h.ship_date,
    h.online_order_flag,
    h.customer_id,
    h.sales_person_id,
    h.territory_id,
    d.product_id,
    d.order_qty,
    d.unit_price,
    d.unit_price_discount,
    d.line_total,
    h.sub_total,
    h.tax_amt,
    h.freight,
    h.total_due
FROM src_detail  AS d
JOIN src_header  AS h ON d.sales_order_id = h.sales_order_id
