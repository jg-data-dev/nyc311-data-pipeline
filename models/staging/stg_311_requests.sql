with cleaned as (
    select
        unique_key,
        created_date,
        closed_date,
        resolution_action_updated_date,

        nullif(trim(upper(agency)), '') as agency,
        nullif(trim(upper(agency_name)), '') as agency_name,
        nullif(trim(upper(complaint_type)), '') as complaint_type,
        nullif(trim(upper(descriptor)), '') as descriptor,
        nullif(trim(upper(descriptor_2)), '') as descriptor_2,
        nullif(trim(upper(status)), '') as status,
        nullif(trim(resolution_description), '') as resolution_description,
        nullif(trim(upper(open_data_channel_type)), '') as open_data_channel_type,

        nullif(trim(upper(incident_address)), '') as incident_address,
        nullif(trim(upper(street_name)), '') as street_name,
        nullif(trim(upper(cross_street_1)), '') as cross_street_1,
        nullif(trim(upper(cross_street_2)), '') as cross_street_2,
        nullif(trim(upper(intersection_street_1)), '') as intersection_street_1,
        nullif(trim(upper(intersection_street_2)), '') as intersection_street_2,
        nullif(trim(upper(landmark)), '') as landmark,
        nullif(trim(upper(city)), '') as city,
        coalesce(nullif(trim(upper(borough)), ''), 'UNSPECIFIED') as borough,
        nullif(trim(incident_zip), '') as incident_zip,

        nullif(trim(upper(community_board)), '') as community_board,
        nullif(trim(council_district), '') as council_district,
        nullif(trim(upper(police_precinct)), '') as police_precinct,

        nullif(trim(upper(park_borough)), '') as park_borough,
        nullif(trim(upper(park_facility_name)), '') as park_facility_name,
        nullif(trim(taxi_pick_up_location), '') as taxi_pick_up_location,

        nullif(trim(bbl), '') as bbl,
        nullif(trim(x_coordinate_state_plane), '') as x_coordinate_state_plane,
        nullif(trim(y_coordinate_state_plane), '') as y_coordinate_state_plane,

        safe_cast(latitude as numeric) as latitude,
        safe_cast(longitude as numeric) as longitude,
        location,
        raw_json
    from {{ source('nyc311_raw', 'raw_311_requests') }}
)

select
    *,
    to_hex(md5(concat(
        coalesce(city, ''), '|',
        coalesce(borough, ''), '|',
        coalesce(landmark, ''), '|',
        coalesce(street_name, ''), '|',
        coalesce(incident_zip, ''), '|',
        coalesce(park_borough, ''), '|',
        coalesce(cross_street_1, ''), '|',
        coalesce(cross_street_2, ''), '|',
        coalesce(intersection_street_1, ''), '|',
        coalesce(intersection_street_2, ''), '|',
        coalesce(incident_address, ''), '|',
        coalesce(park_facility_name, ''), '|',
        coalesce(cast(latitude as string), ''), '|',
        coalesce(cast(longitude as string), ''), '|',
        coalesce(x_coordinate_state_plane, ''), '|',
        coalesce(y_coordinate_state_plane, ''), '|',
        coalesce(community_board, ''), '|',
        coalesce(council_district, '')
    ))) as location_key,

    to_hex(md5(concat(
        coalesce(agency, '<NULL>'), '|',
        coalesce(agency_name, '<NULL>')
    ))) as agency_key,

    to_hex(md5(concat(
        coalesce(complaint_type, '<NULL>'), '|',
        coalesce(descriptor, '<NULL>'), '|',
        coalesce(descriptor_2, '<NULL>')
    ))) as complaint_type_key,

    to_hex(md5(coalesce(status, '<NULL>'))) as status_key,

    to_hex(md5(coalesce(open_data_channel_type, '<NULL>'))) as channel_key,

    date(created_date) as created_day,
    date(closed_date) as closed_day,

    closed_date is not null
        and created_date is not null
        and closed_date < created_date as invalid_close_before_create,

    latitude is not null
        and longitude is not null
        and latitude between -90 and 90
        and longitude between -180 and 180 as valid_lat_long
from cleaned
