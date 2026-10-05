select
    sum(pct_of_all_requests) as pct_sum
from {{ ref('fct_borough_complaint_mix') }}
having abs(sum(pct_of_all_requests) - 100.00) > 1.00
