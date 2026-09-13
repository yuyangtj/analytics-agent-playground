with raw as (
    select
        value ->> 'op' as op,
        value -> 'after' as after,
        value -> 'before' as before,
        (value -> 'source' ->> 'ts_ms')::BIGINT as source_ts_ms
    from read_json(
        's3://cdc-events/business.public.marketing_spend/dt=*/*.jsonl',
        columns = {'value': 'JSON'},
        format = 'newline_delimited'
    )
),

keyed as (
    select
        coalesce((after ->> 'marketing_spend_id')::BIGINT, (before ->> 'marketing_spend_id')::BIGINT) as marketing_spend_id,
        op,
        after,
        row_number() over (
            partition by coalesce((after ->> 'marketing_spend_id')::BIGINT, (before ->> 'marketing_spend_id')::BIGINT)
            order by source_ts_ms desc
        ) as rn
    from raw
)

select
    marketing_spend_id,
    date '1970-01-01' + (after ->> 'spend_date')::BIGINT * interval 1 day as spend_date,
    after ->> 'channel' as channel,
    (after ->> 'spend')::DOUBLE as spend,
    (after ->> 'impressions')::INTEGER as impressions,
    (after ->> 'clicks')::INTEGER as clicks,
    (after ->> 'created_at')::TIMESTAMP as created_at,
    (after ->> 'updated_at')::TIMESTAMP as updated_at
from keyed
where rn = 1 and op != 'd'
