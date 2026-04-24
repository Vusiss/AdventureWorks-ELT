{{ config(materialized='table', schema='staging') }}

-- USD/PLN exchange rates for every day in the sales date range.
-- Weekends and NBP holidays have no published rate, so we apply
-- Last Observation Carried Forward (LOCF) from the preceding trading day.
-- rate_change_direction reflects whether the published rate rose or fell
-- vs the previous published rate; LOCF days inherit the last known direction.

WITH raw_rates AS (
    SELECT
        CAST("date"  AS DATE)   AS rate_date,
        CAST(rate    AS FLOAT)  AS usd_pln_rate
    FROM {{ source('extract', 'currency_rate_data') }}
    WHERE currency = 'USD'
),

-- Calculate rate direction on actual trading days only
rates_with_direction AS (
    SELECT
        rate_date,
        usd_pln_rate,
        CASE
            WHEN usd_pln_rate > LAG(usd_pln_rate) OVER (ORDER BY rate_date) THEN 'INCREASE'
            WHEN usd_pln_rate < LAG(usd_pln_rate) OVER (ORDER BY rate_date) THEN 'DECREASE'
            WHEN usd_pln_rate = LAG(usd_pln_rate) OVER (ORDER BY rate_date) THEN 'STABLE'
            ELSE 'UNKNOWN'
        END AS rate_change_direction
    FROM raw_rates
),

all_dates AS (
    SELECT date_day FROM {{ ref('stg_date') }}
),

-- Left-join: non-trading days get NULL rate, filled below via LOCF
joined AS (
    SELECT
        d.date_day,
        r.usd_pln_rate,
        r.rate_change_direction
    FROM all_dates                d
    LEFT JOIN rates_with_direction r ON d.date_day = r.rate_date
)

{% if target.type == 'duckdb' %}

SELECT
    date_day,
    ROUND(
        LAST_VALUE(usd_pln_rate        IGNORE NULLS) OVER (
            ORDER BY date_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        4
    )                                   AS usd_pln_rate,
    LAST_VALUE(rate_change_direction IGNORE NULLS) OVER (
        ORDER BY date_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                        AS rate_change_direction
FROM joined

{% else %}

-- MSSQL: correlated subquery LOCF (exchange rate table is small, ~1 200 rows)
SELECT
    d.date_day,
    ROUND(
        (SELECT TOP 1 rwd.usd_pln_rate
         FROM rates_with_direction rwd
         WHERE rwd.rate_date <= d.date_day
         ORDER BY rwd.rate_date DESC),
        4
    )                                   AS usd_pln_rate,
    (SELECT TOP 1 rwd.rate_change_direction
     FROM rates_with_direction rwd
     WHERE rwd.rate_date <= d.date_day
     ORDER BY rwd.rate_date DESC)       AS rate_change_direction
FROM all_dates d

{% endif %}
