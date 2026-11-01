{# Small cross-database helpers. dbt picks the version that matches the adapter in use:
   duckdb__name or bigquery__name, falling back to default__name. Everything else in the models is plain SQL that both engines accept. #}

{% macro try_cast_timestamp(col) %}{{ return(adapter.dispatch('try_cast_timestamp')(col)) }}{% endmacro %}
{% macro default__try_cast_timestamp(col) %}try_cast({{ col }} as timestamp){% endmacro %}
{% macro bigquery__try_cast_timestamp(col) %}safe_cast({{ col }} as timestamp){% endmacro %}

{# 90th percentile. DuckDB is exact; BigQuery only has an exact percentile as a window function, so the approximate
   aggregate is used there (the difference is tiny but not zero). #}
{% macro p90(col) %}{{ return(adapter.dispatch('p90')(col)) }}{% endmacro %}
{% macro default__p90(col) %}quantile_cont({{ col }}, 0.9){% endmacro %}
{% macro bigquery__p90(col) %}approx_quantiles({{ col }}, 100 ignore nulls)[offset(90)]{% endmacro %}

{# A DATE as a whole number of days. BigQuery only allows a RANGE window with a numeric offset on a number. #}
{% macro epoch_day(col) %}{{ return(adapter.dispatch('epoch_day')(col)) }}{% endmacro %}
{% macro default__epoch_day(col) %}date_diff('day', date '1970-01-01', {{ col }}){% endmacro %}
{% macro bigquery__epoch_day(col) %}unix_date({{ col }}){% endmacro %}

{# Incremental strategy: BigQuery does not support delete+insert, it uses merge. Both replace rows by unique_key. #}
{% macro incremental_strategy_for_target() %}{{ 'merge' if target.type == 'bigquery' else 'delete+insert' }}{% endmacro %}

{# 64-bit floating point. dbt's own type_float() is a 32-bit float on DuckDB, which would silently change money values. #}
{% macro type_double() %}{{ return(adapter.dispatch('type_double')()) }}{% endmacro %}
{% macro default__type_double() %}double{% endmacro %}
{% macro bigquery__type_double() %}float64{% endmacro %}

{# A timestamp minus N days, keeping the TIMESTAMP type. (dbt.dateadd returns DATETIME on BigQuery, and BigQuery refuses to compare
   a DATETIME with a TIMESTAMP column.) #}
{% macro ts_minus_days(expr, n) %}{{ return(adapter.dispatch('ts_minus_days')(expr, n)) }}{% endmacro %}
{% macro default__ts_minus_days(expr, n) %}({{ expr }} - interval {{ n }} day){% endmacro %}
{% macro bigquery__ts_minus_days(expr, n) %}timestamp_sub({{ expr }}, interval {{ n }} day){% endmacro %}

{# The BigQuery sandbox (no billing) forbids DML, and incremental models need it (MERGE). On the target named bq_sandbox the fact table
   is rebuilt as a plain table each run; everywhere else it is incremental. #}
{% macro fct_orders_materialization() %}{{ return('table' if target.name == 'bq_sandbox' else 'incremental') }}{% endmacro %}
