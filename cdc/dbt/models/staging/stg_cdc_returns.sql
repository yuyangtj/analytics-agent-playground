with raw as (
    select
        value ->> 'op' as op,
        value -> 'after' as after,
        value -> 'before' as before,
        (value -> 'source' ->> 'ts_ms')::BIGINT as source_ts_ms
    from read_json(
        's3://cdc-events/business.public.returns/dt=*/*.jsonl',
        columns = {'value': 'JSON'},
        format = 'newline_delimited'
    )
),

keyed as (
    select
        coalesce((after ->> 'return_id')::BIGINT, (before ->> 'return_id')::BIGINT) as return_id,
        op,
        after,
        row_number() over (
            partition by coalesce((after ->> 'return_id')::BIGINT, (before ->> 'return_id')::BIGINT)
            order by source_ts_ms desc
        ) as rn
    from raw
)

select
    return_id,
    (after ->> 'order_id')::BIGINT as order_id,
    date '1970-01-01' + (after ->> 'return_date')::BIGINT * interval 1 day as return_date,
    after ->> 'reason' as reason,
    (after ->> 'refund_amount')::DOUBLE as refund_amount,
    (after ->> 'created_at')::TIMESTAMP as created_at,
    (after ->> 'updated_at')::TIMESTAMP as updated_at
from keyed
where rn = 1 and op != 'd'
