with created_by_day as (
    select
        date(created_at) as metric_date,

        count(*) as created_count,

        countif(extract(hour from created_at) between 6 and 11)
            as created_morning_count,
        countif(extract(hour from created_at) between 12 and 17)
            as created_afternoon_count,
        countif(extract(hour from created_at) between 18 and 23)
            as created_evening_count,
        countif(extract(hour from created_at) between 0 and 5)
            as created_overnight_count,

        countif(is_invalid_time_order = 1)
            as invalid_time_order_count,
        countif(is_missing_critical_fields = 1)
            as missing_critical_fields_count,
        countif(is_missing_location_id = 1)
            as missing_location_id_count,
        countif(is_missing_complaint_type_id = 1)
            as missing_complaint_type_id_count,
        countif(is_missing_agency_id = 1)
            as missing_agency_id_count,
        countif(is_missing_status_id = 1)
            as missing_status_id_count,
        countif(is_closed_at_status_mismatch = 1)
            as closed_at_status_mismatch_count

    from {{ ref('int_complaints_flagged') }}
    where created_at is not null
    group by date(created_at)
),

closed_by_day as (
    select
        date(closed_at) as metric_date,

        count(*) as closed_count,
        avg(resolution_hours) as avg_resolution_hours,

        approx_quantiles(resolution_hours, 100)[offset(50)]
            as median_resolution_hours,
        approx_quantiles(resolution_hours, 100)[offset(90)]
            as p90_resolution_hours,
        approx_quantiles(resolution_hours, 100)[offset(95)]
            as p95_resolution_hours

    from {{ ref('int_complaints_flagged') }}
    where closed_at is not null
      and created_at is not null
      and is_invalid_time_order = 0
    group by date(closed_at)
)

select
    c.metric_date,

    c.created_count,
    coalesce(cl.closed_count, 0) as closed_count,
    c.created_count - coalesce(cl.closed_count, 0) as daily_net_case_flow,

    cl.avg_resolution_hours,
    cl.median_resolution_hours,
    cl.p90_resolution_hours,
    cl.p95_resolution_hours,

    c.created_morning_count,
    c.created_afternoon_count,
    c.created_evening_count,
    c.created_overnight_count,

    c.invalid_time_order_count,
    c.missing_critical_fields_count,
    c.missing_location_id_count,
    c.missing_complaint_type_id_count,
    c.missing_agency_id_count,
    c.missing_status_id_count,
    c.closed_at_status_mismatch_count

from created_by_day c
left join closed_by_day cl
    on c.metric_date = cl.metric_date
