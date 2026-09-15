select
    cast(participant_key as string) as participant_key,
    cast(recording_id as string) as recording_id,
    cast(item_slot as bigint) as item_slot,
    cast(item_id as bigint) as item_id,
    trim(cast(item_name as string)) as item_name,
    greatest(coalesce(cast(item_count as bigint), 1), 0) as item_count,
    coalesce(cast(is_consumable as boolean), false) as is_consumable
from {{ source('silver', 'match_participant_items') }}
