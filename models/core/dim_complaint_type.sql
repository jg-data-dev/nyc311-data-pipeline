select distinct
    complaint_type_key as complaint_type_id,
    complaint_type,
    descriptor,
    descriptor_2
from {{ ref('stg_311_requests') }}
where complaint_type is not null
   or descriptor is not null
   or descriptor_2 is not null
