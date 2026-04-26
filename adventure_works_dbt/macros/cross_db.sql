{# ── Boolean cast ──────────────────────────────────────────────────── #}
{% macro bool_cast(col) %}
    CAST({{ col }} AS BIT)
{% endmacro %}

{# ── Current date ──────────────────────────────────────────────────── #}
{% macro today() %}
    CAST(GETDATE() AS DATE)
{% endmacro %}

{# ── Date diff in months ───────────────────────────────────────────── #}
{% macro datediff_months(start, end) %}
    DATEDIFF(month, {{ start }}, {{ end }})
{% endmacro %}

{# ── Date → YYYYMMDD integer key ───────────────────────────────────── #}
{% macro date_to_int(col) %}
    CAST(CONVERT(VARCHAR(8), {{ col }}, 112) AS INT)
{% endmacro %}

{# ── Extract quarter (1-4) ─────────────────────────────────────────── #}
{% macro extract_quarter(col) %}
    DATEPART(quarter, {{ col }})
{% endmacro %}

{# ── ISO day of week (Mon=1 … Sun=7) ──────────────────────────────── #}
{% macro extract_isodow(col) %}
    ((DATEPART(weekday, {{ col }}) + @@DATEFIRST - 2) % 7) + 1
{% endmacro %}

{# ── Day of year ───────────────────────────────────────────────────── #}
{% macro extract_dayofyear(col) %}
    DATEPART(dayofyear, {{ col }})
{% endmacro %}

{# ── Week of year ──────────────────────────────────────────────────── #}
{% macro extract_weekofyear(col) %}
    DATEPART(iso_week, {{ col }})
{% endmacro %}

{# ── Month name (full) ─────────────────────────────────────────────── #}
{% macro month_name(col) %}
    DATENAME(month, {{ col }})
{% endmacro %}

{# ── Month name (short, 3-char) ────────────────────────────────────── #}
{% macro month_short(col) %}
    LEFT(DATENAME(month, {{ col }}), 3)
{% endmacro %}

{# ── Day name (full) ───────────────────────────────────────────────── #}
{% macro day_name(col) %}
    DATENAME(weekday, {{ col }})
{% endmacro %}

{# ── Is weekend flag ───────────────────────────────────────────────── #}
{% macro is_weekend(col) %}
    CAST(CASE WHEN DATENAME(weekday, {{ col }}) IN ('Saturday','Sunday')
              THEN 1 ELSE 0 END AS BIT)
{% endmacro %}

{# ── Quarter label  (e.g. 'Q1 2024') ──────────────────────────────── #}
{% macro quarter_label(col) %}
    'Q' + CAST(DATEPART(quarter, {{ col }}) AS VARCHAR(1))
        + ' ' + CAST(YEAR({{ col }}) AS VARCHAR(4))
{% endmacro %}
