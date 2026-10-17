select
    delay_code,
    delay_reason,
    count(*)                                             as incidents,
    count_if(is_delay)                                   as delayed_incidents,
    sum(min_delay)                                       as total_delay_min,
    round(avg(case when is_delay then min_delay end), 1) as avg_delay_min
from {{ ref('fct_delays') }}
group by 1, 2
