select
    day_of_week,
    delay_hour,
    is_weekend,
    count(*)        as incidents,
    sum(min_delay)  as total_delay_min
from {{ ref('fct_delays') }}
group by 1, 2, 3
