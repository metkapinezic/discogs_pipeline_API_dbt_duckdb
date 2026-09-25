with releases as (
    select * from {{ ref('stg_label_releases') }}
)

select
    md5(artist)                                             as artist_key,
    artist_clean                                            as artist_name,
    trim(regexp_replace(artist_clean, '\s\(\d+\)$', ''))    as artist_display_name,
    artist = 'Various'                                      as is_various,
    min(release_year)                                       as first_release_year,
    max(release_year)                                       as last_release_year,
    count(distinct release_id)                              as release_count
from releases
where artist is not null
group by artist