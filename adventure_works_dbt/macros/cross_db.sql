{# ── Boolean cast ──────────────────────────────────────────────────── #}
{% macro bool_cast(col) %}
    {% if target.type == 'duckdb' %}
        CAST({{ col }} AS BOOLEAN)
    {% else %}
        CAST({{ col }} AS BIT)
    {% endif %}
{% endmacro %}

{# ── Current date ──────────────────────────────────────────────────── #}
{% macro today() %}
    {% if target.type == 'duckdb' %}
        current_date
    {% else %}
        CAST(GETDATE() AS DATE)
    {% endif %}
{% endmacro %}

{# ── Date diff in months ───────────────────────────────────────────── #}
{% macro datediff_months(start, end) %}
    {% if target.type == 'duckdb' %}
        datediff('month', {{ start }}, {{ end }})
    {% else %}
        DATEDIFF(month, {{ start }}, {{ end }})
    {% endif %}
{% endmacro %}

{# ── Date → YYYYMMDD integer key ───────────────────────────────────── #}
{% macro date_to_int(col) %}
    {% if target.type == 'duckdb' %}
        CAST(strftime({{ col }}, '%Y%m%d') AS INTEGER)
    {% else %}
        CAST(CONVERT(VARCHAR(8), {{ col }}, 112) AS INT)
    {% endif %}
{% endmacro %}

{# ── Extract quarter (1-4) ─────────────────────────────────────────── #}
{% macro extract_quarter(col) %}
    {% if target.type == 'duckdb' %}
        quarter({{ col }})
    {% else %}
        DATEPART(quarter, {{ col }})
    {% endif %}
{% endmacro %}

{# ── ISO day of week (Mon=1 … Sun=7) ──────────────────────────────── #}
{% macro extract_isodow(col) %}
    {% if target.type == 'duckdb' %}
        isodow({{ col }})
    {% else %}
        -- MSSQL: normalize DATEFIRST-independent ISO DOW
        ((DATEPART(weekday, {{ col }}) + @@DATEFIRST - 2) % 7) + 1
    {% endif %}
{% endmacro %}

{# ── Day of year ───────────────────────────────────────────────────── #}
{% macro extract_dayofyear(col) %}
    {% if target.type == 'duckdb' %}
        dayofyear({{ col }})
    {% else %}
        DATEPART(dayofyear, {{ col }})
    {% endif %}
{% endmacro %}

{# ── Week of year ──────────────────────────────────────────────────── #}
{% macro extract_weekofyear(col) %}
    {% if target.type == 'duckdb' %}
        weekofyear({{ col }})
    {% else %}
        DATEPART(iso_week, {{ col }})
    {% endif %}
{% endmacro %}

{# ── Month name (full) ─────────────────────────────────────────────── #}
{% macro month_name(col) %}
    {% if target.type == 'duckdb' %}
        monthname({{ col }})
    {% else %}
        DATENAME(month, {{ col }})
    {% endif %}
{% endmacro %}

{# ── Day name (full) ───────────────────────────────────────────────── #}
{% macro day_name(col) %}
    {% if target.type == 'duckdb' %}
        dayname({{ col }})
    {% else %}
        DATENAME(weekday, {{ col }})
    {% endif %}
{% endmacro %}

{# ── Is weekend flag ───────────────────────────────────────────────── #}
{% macro is_weekend(col) %}
    {% if target.type == 'duckdb' %}
        CASE WHEN isodow({{ col }}) >= 6 THEN TRUE ELSE FALSE END
    {% else %}
        CAST(CASE WHEN DATENAME(weekday, {{ col }}) IN ('Saturday','Sunday')
                  THEN 1 ELSE 0 END AS BIT)
    {% endif %}
{% endmacro %}

{# ── Quarter label  (e.g. 'Q1 2024') ──────────────────────────────── #}
{% macro quarter_label(col) %}
    {% if target.type == 'duckdb' %}
        'Q' || CAST(quarter({{ col }}) AS VARCHAR)
             || ' ' || CAST(year({{ col }}) AS VARCHAR)
    {% else %}
        'Q' + CAST(DATEPART(quarter, {{ col }}) AS VARCHAR(1))
            + ' ' + CAST(YEAR({{ col }}) AS VARCHAR(4))
    {% endif %}
{% endmacro %}
