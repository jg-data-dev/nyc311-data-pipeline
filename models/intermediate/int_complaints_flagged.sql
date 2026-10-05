select
    f.complaint_key,
    f.unique_key,

    f.created_at,
    f.closed_at,

    date(f.created_at) as metric_date,
    date_trunc(date(f.created_at), month) as metric_month,

    f.location_id,
    f.complaint_type_id,
    f.agency_id,
    f.status_id,

    timestamp_diff(f.closed_at, f.created_at, second) / 3600.0
        as resolution_hours,

    case
        when f.closed_at is not null
         and f.created_at is not null
         and f.closed_at < f.created_at
        then 1 else 0
    end as is_invalid_time_order,

    case when f.location_id is null then 1 else 0 end
        as is_missing_location_id,
    case when f.complaint_type_id is null then 1 else 0 end
        as is_missing_complaint_type_id,
    case when f.agency_id is null then 1 else 0 end
        as is_missing_agency_id,
    case when f.status_id is null then 1 else 0 end
        as is_missing_status_id,

    case
        when f.location_id is null
          or f.complaint_type_id is null
          or f.agency_id is null
          or f.status_id is null
        then 1 else 0
    end as is_missing_critical_fields,

    case
        when f.closed_at is not null
         and f.created_at is not null
         and f.closed_at >= f.created_at
         and s.status <> 'CLOSED'
        then 1 else 0
    end as is_closed_at_status_mismatch

from {{ ref('fct_311_complaint') }} f
left join {{ ref('dim_status') }} s
  on f.status_id = s.status_id
