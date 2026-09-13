with raw as (
    select
        value ->> 'op' as op,
        value -> 'after' as after,
        value -> 'before' as before,
        (value -> 'source' ->> 'ts_ms')::BIGINT as source_ts_ms
    from read_json(
        's3://cdc-events/business.public.products/dt=*/*.jsonl',
        columns = {'value': 'JSON'},
        format = 'newline_delimited'
    )
),

keyed as (
    select
        coalesce((after ->> 'product_id')::BIGINT, (before ->> 'product_id')::BIGINT) as product_id,
        op,
        after,
        row_number() over (
            partition by coalesce((after ->> 'product_id')::BIGINT, (before ->> 'product_id')::BIGINT)
            order by source_ts_ms desc
        ) as rn
    from raw
)

select
    product_id,
    after ->> 'category' as category,
    after ->> 'subcategory' as subcategory,
    (after ->> 'cost')::DOUBLE as cost,
    (after ->> 'list_price')::DOUBLE as list_price,
    date '1970-01-01' + (after ->> 'launch_date')::BIGINT * interval 1 day as launch_date,
    case
        when after ->> 'discontinued_date' is null then null
        else date '1970-01-01' + (after ->> 'discontinued_date')::BIGINT * interval 1 day
    end as discontinued_date,
    (after ->> 'created_at')::TIMESTAMP as created_at,
    (after ->> 'updated_at')::TIMESTAMP as updated_at
from keyed
where rn = 1 and op != 'd'
