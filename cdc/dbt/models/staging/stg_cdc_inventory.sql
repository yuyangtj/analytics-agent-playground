-- inventory is a mutable row per (product_id, warehouse) in the OLTP source
-- (see cdc/schema.sql, ARCHITECTURE_DECISIONS.md ADR-4) -- the first weekly
-- snapshot for a given product+warehouse is an insert, every later one an
-- update of the *same* row. The squash logic here is identical to every
-- other staging model regardless: keyed by inventory_id, latest op wins.
with raw as (
    select
        value ->> 'op' as op,
        value -> 'after' as after,
        value -> 'before' as before,
        (value -> 'source' ->> 'ts_ms')::BIGINT as source_ts_ms
    from read_json(
        's3://cdc-events/business.public.inventory/dt=*/*.jsonl',
        columns = {'value': 'JSON'},
        format = 'newline_delimited'
    )
),

keyed as (
    select
        coalesce((after ->> 'inventory_id')::BIGINT, (before ->> 'inventory_id')::BIGINT) as inventory_id,
        op,
        after,
        row_number() over (
            partition by coalesce((after ->> 'inventory_id')::BIGINT, (before ->> 'inventory_id')::BIGINT)
            order by source_ts_ms desc
        ) as rn
    from raw
)

select
    inventory_id,
    (after ->> 'product_id')::BIGINT as product_id,
    after ->> 'warehouse' as warehouse,
    date '1970-01-01' + (after ->> 'snapshot_date')::BIGINT * interval 1 day as snapshot_date,
    (after ->> 'quantity_on_hand')::INTEGER as quantity_on_hand,
    (after ->> 'quantity_reserved')::INTEGER as quantity_reserved,
    (after ->> 'reorder_point')::INTEGER as reorder_point,
    (after ->> 'created_at')::TIMESTAMP as created_at,
    (after ->> 'updated_at')::TIMESTAMP as updated_at
from keyed
where rn = 1 and op != 'd'
