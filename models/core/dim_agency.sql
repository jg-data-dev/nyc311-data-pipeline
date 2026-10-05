select distinct
    agency_key as agency_id,
    agency,
    agency_name
from {{ ref('stg_311_requests') }}
where agency is not null
   or agency_name is not null
