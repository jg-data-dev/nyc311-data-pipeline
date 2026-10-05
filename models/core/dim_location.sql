select distinct
    location_key as location_id,
    location_key,
    city,
    borough,
    landmark,
    street_name,
    incident_zip,
    park_borough,
    cross_street_1,
    cross_street_2,
    intersection_street_1,
    intersection_street_2,
    incident_address,
    park_facility_name,
    latitude,
    longitude,
    x_coordinate_state_plane,
    y_coordinate_state_plane,
    community_board,
    council_district
from {{ ref('stg_311_requests') }}
where location_key is not null
