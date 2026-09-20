-- Required by MetricFlow/the dbt Semantic Layer before it'll parse any
-- semantic model with a time dimension -- confirmed live: dbt run failed
-- with "requires a time spine model... none was found" until this existed.
-- DuckDB-native (generate_series), not dbt_utils.date_spine, to avoid
-- pulling in a new package dependency for one table.
{{ config(materialized='table') }}

select cast(date_day as date) as date_day
from (
    select unnest(generate_series(
        cast('2020-01-01' as date),
        cast('2030-01-01' as date),
        interval 1 day
    )) as date_day
)
