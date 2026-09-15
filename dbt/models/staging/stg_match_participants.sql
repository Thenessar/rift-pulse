select
    cast(participant_key as string) as participant_key,
    cast(recording_id as string) as recording_id,
    cast(participant_slot as bigint) as participant_slot,
    upper(cast(team as string)) as team,
    cast(champion_name as string) as champion_name,
    cast(position_observed as string) as position_observed,
    coalesce(cast(is_bot as boolean), false) as is_bot
from {{ source('silver', 'match_participants') }}
