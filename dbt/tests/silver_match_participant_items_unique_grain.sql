select participant_key, item_slot
from {{ source('silver', 'match_participant_items') }}
group by participant_key, item_slot
having count(*) > 1
