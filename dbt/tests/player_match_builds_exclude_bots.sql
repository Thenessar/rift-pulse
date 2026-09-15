select builds.recording_id, builds.participant_key
from {{ ref('player_match_builds') }} as builds
inner join {{ ref('stg_match_participants') }} as participants
    on builds.participant_key = participants.participant_key
where participants.is_bot
