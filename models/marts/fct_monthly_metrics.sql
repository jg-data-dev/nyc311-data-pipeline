select
    metric_month as month_start,

    count(*) as created_count,

    countif(
        closed_at is not null
        and created_at is not null
        and is_invalid_time_order = 0
    ) as closed_count,

    count(*) - countif(
        closed_at is not null
        and created_at is not null
        and is_invalid_time_order = 0
    ) as net_case_flow,

    round(avg(
        if(
            closed_at is not null
            and created_at is not null
            and is_invalid_time_order = 0,
            resolution_hours,
            null
        )
    ), 2) as avg_resolution_hours,

    round(
        approx_quantiles(
            if(
                closed_at is not null
                and created_at is not null
                and is_invalid_time_order = 0,
                resolution_hours,
                null
            ),
            100 ignore nulls
        )[offset(50)],
        2
    ) as median_resolution_hours,

    round(
        approx_quantiles(
            if(
                closed_at is not null
                and created_at is not null
                and is_invalid_time_order = 0,
                resolution_hours,
                null
            ),
            100 ignore nulls
        )[offset(90)],
        2
    ) as p90_resolution_hours,

    round(
        approx_quantiles(
            if(
                closed_at is not null
                and created_at is not null
                and is_invalid_time_order = 0,
                resolution_hours,
                null
            ),
            100 ignore nulls
        )[offset(95)],
        2
    ) as p95_resolution_hours,

    sum(is_invalid_time_order) as invalid_time_order_count,
    sum(is_missing_critical_fields) as missing_critical_fields_count,
    sum(is_missing_location_id) as missing_location_id_count,
    sum(is_missing_complaint_type_id) as missing_complaint_type_id_count,
    sum(is_missing_agency_id) as missing_agency_id_count,
    sum(is_missing_status_id) as missing_status_id_count,
    sum(is_closed_at_status_mismatch) as closed_at_status_mismatch_count

from {{ ref('int_complaints_flagged') }}
where metric_month is not null
group by metric_month
order by metric_month
