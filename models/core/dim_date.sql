select distinct
    cast(format_date('%Y%m%d', d) as int64) as date_id,
    d as full_date,
    extract(day from d) as day_of_month,
    extract(month from d) as month,
    format_date('%B', d) as month_name,
    extract(quarter from d) as quarter,
    extract(year from d) as year,
    cast(format_date('%u', d) as int64) as day_of_week,
    format_date('%A', d) as day_name,
    cast(format_date('%u', d) as int64) in (6, 7) as is_weekend
from (
    select date(created_date) as d
    from {{ ref('stg_311_requests') }}
    where created_date is not null

    union distinct

    select date(closed_date) as d
    from {{ ref('stg_311_requests') }}
    where closed_date is not null

    union distinct

    select date(resolution_action_updated_date) as d
    from {{ ref('stg_311_requests') }}
    where resolution_action_updated_date is not null
) dates
