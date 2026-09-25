with source as (
    select
        md5(payload::varchar) as row_hash,
        *
    from {{ source('discogs_raw', 'raw_label_releases') }}
)

select
    row_hash,
    label_id,
    release_id,
    payload->>'title'                                                       as title,
    regexp_replace(payload->>'artist', '\*', '', 'g')                       as artist,
    trim(
    regexp_replace(
        regexp_replace(artist, '[\x{200B}-\x{200F}\x{FEFF}\x{2060}]', '', 'g'),   -- invisible characters
        '[\s\x{00A0}]+', ' ', 'g'                                                   -- any run of spaces -> one space
    )
    )                                                                       as artist_clean,
    case
        when regexp_matches(artist, '(?i)\s(feat\.?|feat:|featuring|ft\.?)\s')
        then trim(regexp_extract(artist, '(?i)\s(?:feat\.?|feat:|featuring|ft\.?)\s(.*)$', 1))
        else null
        end                                                                 as featured_artist,
    payload->>'catno'                                                       as catalog_num,
    payload->>'format'                                                      as format_raw,
    list_transform(string_split(payload->>'format', ','), x -> trim(x))     as format_list,
    format_list[1]                                                          as format_type,
    format_list[2:]                                                         as format_descriptions,
    nullif(try_cast(payload->>'year' as integer), 0)                 as release_year,
    payload->>'status'                                               as status,
    try_cast(payload->>'$.stats.community.in_wantlist' as integer)   as community_want,
    try_cast(payload->>'$.stats.community.in_collection' as integer) as community_have,
    payload->>'thumb'                                                as thumb_url,
    payload->>'resource_url'                                         as resource_url,
    loaded_at
from source
qualify row_number() over (partition by row_hash order by loaded_at) = 1