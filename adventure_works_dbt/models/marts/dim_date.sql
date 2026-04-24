{{ config(materialized='table', schema='main') }}

{% if target.type == 'duckdb' %}

WITH date_spine AS (
    SELECT CAST(d AS DATE) AS date_day
    FROM range(DATE '2001-01-01', DATE '2026-12-31', INTERVAL '1' DAY) AS t(d)
)

{% else %}

-- MSSQL: generate 10 000 rows via cross-joined value lists (covers 9 497 days)
WITH nums AS (
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
    FROM        (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) a(x)
    CROSS JOIN  (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) b(x)
    CROSS JOIN  (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) c(x)
    CROSS JOIN  (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) d(x)
),
date_spine AS (
    SELECT CAST(DATEADD(day, n, '2001-01-01') AS DATE) AS date_day
    FROM nums
    WHERE n <= DATEDIFF(day, '2001-01-01', '2026-12-31')
)

{% endif %}

SELECT
    {{ date_to_int('date_day') }}                           AS date_key,

    date_day                                                AS full_date,

    CAST(YEAR(date_day) AS INTEGER)                         AS year,
    CAST({{ extract_quarter('date_day') }} AS INTEGER)      AS quarter,
    CAST(MONTH(date_day) AS INTEGER)                        AS month,
    CAST(DAY(date_day) AS INTEGER)                          AS day_of_month,

    CAST({{ extract_isodow('date_day') }} AS INTEGER)       AS day_of_week,
    CAST({{ extract_dayofyear('date_day') }} AS INTEGER)    AS day_of_year,
    CAST({{ extract_weekofyear('date_day') }} AS INTEGER)   AS week_of_year,

    {{ month_name('date_day') }}                            AS month_name,
    {{ day_name('date_day') }}                              AS day_name,

    {{ quarter_label('date_day') }}                         AS quarter_label,

    {{ is_weekend('date_day') }}                            AS is_weekend

FROM date_spine
