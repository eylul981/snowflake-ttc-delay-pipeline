select
    delay_month,
    line_name,
    station,
    count(*)            as incidents,
    count_if(is_delay)  as delayed_incidents,
    sum(min_delay)      as total_delay_min
from {{ ref('fct_delays') }}
group by 1, 2, 3
