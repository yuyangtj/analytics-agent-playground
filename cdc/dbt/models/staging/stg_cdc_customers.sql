-- Squashes the raw Debezium change-event log (landed by s3-sink) into
-- current state, read directly off MinIO/S3 -- no cdc/consumer.py or
-- cdc/batch_load.py involved, this is the third, independent arm.
--
-- Uses explicit JSON-path extraction (value->'after'->>'col'), not struct
-- auto-inference (read_json_auto's per-column type), because DuckDB infers
-- before/after's type per read from whatever data is actually present --
-- a table with zero deletes in the read gets `before` typed as plain JSON
-- instead of STRUCT, which would need different SQL. Explicit paths work
-- the same way regardless of what ops happen to be present.
with raw as (
    select
        value ->> 'op' as op,
        value -> 'after' as after,
        value -> 'before' as before,
        (value -> 'source' ->> 'ts_ms')::BIGINT as source_ts_ms
    from read_json(
        's3://cdc-events/business.public.customers/dt=*/*.jsonl',
        columns = {'value': 'JSON'},
        format = 'newline_delimited'
    )
),

keyed as (
    select
        coalesce((after ->> 'customer_id')::BIGINT, (before ->> 'customer_id')::BIGINT) as customer_id,
        op,
        after,
        row_number() over (
            partition by coalesce((after ->> 'customer_id')::BIGINT, (before ->> 'customer_id')::BIGINT)
            order by source_ts_ms desc
        ) as rn
    from raw
)

select
    customer_id,
    date '1970-01-01' + (after ->> 'signup_date')::BIGINT * interval 1 day as signup_date,
    after ->> 'region' as region,
    after ->> 'acquisition_channel' as acquisition_channel,
    after ->> 'email_domain' as email_domain,
    (after ->> 'created_at')::TIMESTAMP as created_at,
    (after ->> 'updated_at')::TIMESTAMP as updated_at
from keyed
where rn = 1 and op != 'd'
