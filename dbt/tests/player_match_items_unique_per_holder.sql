select
    builds.participant_key,
    item.item_id,
    item.item_name
from {{ ref('player_match_builds') }} as builds
lateral view explode(builds.items) exploded as item
group by builds.participant_key, item.item_id, item.item_name
having count(*) > 1
