select
    lower(split_part(_source_file, '/', 1)) as mode,
    upper(trim(code)) as delay_code,
    trim(
        regexp_replace(
            regexp_replace(description, '[^ -~]+', ' - '),  -- any non-standard characters -> " - "
            ' +', ' '                                        -- collapse double spaces
        )
    ) as description
from {{ source('raw', 'delay_codes') }}
where code is not null
