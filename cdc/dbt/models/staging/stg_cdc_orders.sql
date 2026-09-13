with raw as (
    select
        value ->> 'op' as op,
        value -> 'after' as after,
        value -> 'before' as before,
        (value -> 'source' ->> 'ts_ms')::BIGINT as source_ts_ms
    from read_json(
        's3://cdc-events/business.public.orders/dt=*/*.jsonl',
        columns = {'value': 'JSON'},
        format = 'newline_delimited'
    )
),

keyed as (
    select
        coalesce((after ->> 'order_id')::BIGINT, (before ->> 'order_id')::BIGINT) as order_id,
        op,
        after,
        row_number() over (
            partition by coalesce((after ->> 'order_id')::BIGINT, (before ->> 'order_id')::BIGINT)
            order by source_ts_ms desc
        ) as rn
    from raw
)

select
    order_id,
    (after ->> 'customer_id')::BIGINT as customer_id,
    date '1970-01-01' + (after ->> 'order_date')::BIGINT * interval 1 day as order_date,
    after ->> 'status' as status,
    after ->> 'channel' as channel,
    after ->> 'promo_code' as promo_code,
    (after ->> 'created_at')::TIMESTAMP as created_at,
    (after ->> 'updated_at')::TIMESTAMP as updated_at
from keyed
where rn = 1 and op != 'd'
