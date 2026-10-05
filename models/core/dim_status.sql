select distinct
    status_key as status_id,
    status
from {{ ref('stg_311_requests') }}
where status is not null
