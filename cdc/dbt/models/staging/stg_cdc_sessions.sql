with raw as (
    select
        value ->> 'op' as op,
        value -> 'after' as after,
        value -> 'before' as before,
        (value -> 'source' ->> 'ts_ms')::BIGINT as source_ts_ms
    from read_json(
        's3://cdc-events/business.public.sessions/dt=*/*.jsonl',
        columns = {'value': 'JSON'},
        format = 'newline_delimited'
    )
),

keyed as (
    select
        coalesce((after ->> 'session_id')::BIGINT, (before ->> 'session_id')::BIGINT) as session_id,
        op,
        after,
        row_number() over (
            partition by coalesce((after ->> 'session_id')::BIGINT, (before ->> 'session_id')::BIGINT)
            order by source_ts_ms desc
        ) as rn
    from raw
)

select
    session_id,
    (after ->> 'customer_id')::BIGINT as customer_id,  -- nullable: anonymous sessions
    date '1970-01-01' + (after ->> 'session_date')::BIGINT * interval 1 day as session_date,
    after ->> 'channel' as channel,
    after ->> 'device' as device,
    (after ->> 'created_at')::TIMESTAMP as created_at,
    (after ->> 'updated_at')::TIMESTAMP as updated_at
from keyed
where rn = 1 and op != 'd'
