-- Các bản ghi CDC có priority không thể chuẩn hóa về miền 1..4.
-- Grain: một hàng cho một bản ghi CDC bị loại, không phải một ticket.

{{ config(materialized = 'table') }}

with normalized as (

    select
        *,
        {{ normalize_priority('priority_raw') }} as priority
    from {{ source('bronze', 'bronze_tickets_cdc') }}

)

select
    ticket_id,
    cdc_seq,
    op,
    priority_raw,
    event_time,
    _ingested_at,
    'priority không ánh xạ được về 1..4' as reject_reason,
    current_timestamp as _quarantined_at
from normalized
where priority is null
