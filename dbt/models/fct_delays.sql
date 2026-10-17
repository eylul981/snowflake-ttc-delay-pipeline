-- One row per delay event, with reason description and derived fields

with delays as (

    select
        *,
        -- normalize line codes: uppercase, no spaces, drop 'LINES', YUS -> YU
        replace(replace(replace(upper(line), ' ', ''), 'LINES', ''), 'YUS', 'YU') as line_norm
    from {{ source('staging', 'delays') }}

),

reasons as (

    select * from {{ ref('dim_delay_reason') }}

)

select
    d.delay_id,
    d.mode,
    d.delay_date,
    d.delay_time,
    d.day_of_week,
    hour(d.delay_time)                        as delay_hour,
    date_trunc('month', d.delay_date)         as delay_month,
    d.day_of_week in ('Saturday', 'Sunday')   as is_weekend,
    d.line                                    as line_code_raw,
    case
        when d.line_norm is null     then 'Unknown'
        when d.line_norm = 'YU'      then 'Line 1 Yonge-University'
        when d.line_norm = 'BD'      then 'Line 2 Bloor-Danforth'
        when d.line_norm = 'SRT'     then 'Line 3 Scarborough'
        when d.line_norm = 'SHP'     then 'Line 4 Sheppard'
        when d.line_norm like '%/%'  then 'Multiple lines'
        else 'Other / Unknown'
    end                                       as line_name,
    d.station,
    d.delay_code,
    coalesce(r.description, 'Unknown code')   as delay_reason,
    d.min_delay,
    d.min_gap,
    d.min_delay > 0                           as is_delay,
    d.bound,
    d.vehicle
from delays d
left join reasons r
       on d.mode = r.mode
      and d.delay_code = r.delay_code
