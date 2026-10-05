select distinct
    channel_key as channel_id,
    open_data_channel_type
from {{ ref('stg_311_requests') }}
where open_data_channel_type is not null
