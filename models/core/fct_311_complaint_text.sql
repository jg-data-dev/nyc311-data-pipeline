select
    unique_key,
    resolution_description
from {{ ref('stg_311_requests') }}
where unique_key is not null
