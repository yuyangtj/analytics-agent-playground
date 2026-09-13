with raw as (
    select
        value ->> 'op' as op,
        value -> 'after' as after,
        value -> 'before' as before,
        (value -> 'source' ->> 'ts_ms')::BIGINT as source_ts_ms
    from read_json(
        's3://cdc-events/business.public.order_items/dt=*/*.jsonl',
        columns = {'value': 'JSON'},
        format = 'newline_delimited'
    )
),

keyed as (
    select
        coalesce((after ->> 'order_item_id')::BIGINT, (before ->> 'order_item_id')::BIGINT) as order_item_id,
        op,
        after,
        row_number() over (
            partition by coalesce((after ->> 'order_item_id')::BIGINT, (before ->> 'order_item_id')::BIGINT)
            order by source_ts_ms desc
        ) as rn
    from raw
)

select
    order_item_id,
    (after ->> 'order_id')::BIGINT as order_id,
    (after ->> 'product_id')::BIGINT as product_id,
    (after ->> 'quantity')::INTEGER as quantity,
    (after ->> 'unit_price')::DOUBLE as unit_price,
    (after ->> 'discount')::DOUBLE as discount,
    (after ->> 'created_at')::TIMESTAMP as created_at,
    (after ->> 'updated_at')::TIMESTAMP as updated_at
from keyed
where rn = 1 and op != 'd'
