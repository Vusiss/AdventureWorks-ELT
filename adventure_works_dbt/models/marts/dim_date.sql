{{ config(materialized='table', schema='main') }}

-- Date dimension built from the actual sales date range (via stg_date).
-- One row per calendar day; grain matches SalesOrderHeader.OrderDate.

SELECT
    {{ date_to_int('date_day') }}                           AS date_key,

    date_day                                                AS full_date,

    CAST(YEAR(date_day)  AS INTEGER)                        AS year,

    CAST(CASE WHEN MONTH(date_day) <= 6 THEN 1 ELSE 2 END
         AS INTEGER)                                        AS half_year,

    CAST({{ extract_quarter('date_day') }} AS INTEGER)      AS quarter,

    CAST(MONTH(date_day) AS INTEGER)                        AS month,
    {{ month_name('date_day') }}                            AS month_name,

    CAST(DAY(date_day)   AS INTEGER)                        AS day_of_month,
    {{ day_name('date_day') }}                              AS day_name,

    CAST({{ extract_isodow('date_day') }}    AS INTEGER)    AS day_of_week,
    CAST({{ extract_dayofyear('date_day') }} AS INTEGER)    AS day_of_year,
    CAST({{ extract_weekofyear('date_day') }} AS INTEGER)   AS week_of_year,

    {{ quarter_label('date_day') }}                         AS quarter_label,

    {{ is_weekend('date_day') }}                            AS is_weekend

FROM {{ ref('stg_date') }}
