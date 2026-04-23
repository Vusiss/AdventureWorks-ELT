{{ config(materialized='table', schema='main') }}

-- Generated date dimension covering the full AdventureWorks data range
WITH date_spine AS (
    SELECT CAST(d AS DATE) AS date_day
    FROM range(DATE '2001-01-01', DATE '2026-12-31', INTERVAL '1' DAY) AS t(d)
)

SELECT
    -- Surrogate key: YYYYMMDD integer for easy joining with order_date
    CAST(strftime(date_day, '%Y%m%d') AS INTEGER)   AS date_key,

    date_day                                        AS full_date,

    CAST(year(date_day) AS INTEGER)                 AS year,
    CAST(quarter(date_day) AS INTEGER)              AS quarter,
    CAST(month(date_day) AS INTEGER)                AS month,
    CAST(day(date_day) AS INTEGER)                  AS day_of_month,

    -- ISO day-of-week: 1 = Monday … 7 = Sunday
    CAST(isodow(date_day) AS INTEGER)               AS day_of_week,
    CAST(dayofyear(date_day) AS INTEGER)            AS day_of_year,
    CAST(weekofyear(date_day) AS INTEGER)           AS week_of_year,

    monthname(date_day)                             AS month_name,
    dayname(date_day)                               AS day_name,

    'Q' || CAST(quarter(date_day) AS VARCHAR)
    || ' ' || CAST(year(date_day) AS VARCHAR)       AS quarter_label,

    CASE WHEN isodow(date_day) >= 6 THEN TRUE ELSE FALSE END AS is_weekend

FROM date_spine
