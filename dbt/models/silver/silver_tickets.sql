-- Silver: trạng thái MỚI NHẤT của mỗi ticket, dựng lại từ luồng CDC.
--
-- Quy tắc CDC đã cài sẵn và đang đúng:
--   * mỗi ticket lấy bản ghi có (event_time, cdc_seq) lớn nhất;
--   * ticket có op = 'd' bị loại khỏi Silver.
--
-- Phần còn lại của model thì… bạn tự đọc.

{{ config(materialized = 'table') }}

with normalized as (

    select
        *,
        {{ normalize_priority('priority_raw') }} as priority
    from {{ source('bronze', 'bronze_tickets_cdc') }}

),

valid as (

    select *
    from normalized
    where priority is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by ticket_id
            order by event_time desc, cdc_seq desc
        ) as _rn
    from valid

),

latest as (select * from ranked where _rn = 1)

select
    ticket_id,
    customer_id,
    customer_name,
    segment,

    priority,

    category,
    channel,
    status,
    csat,
    first_response_sec,
    subject,
    body,
    event_time                                               as updated_at,
    _ingested_at
from latest
where op <> 'd'
