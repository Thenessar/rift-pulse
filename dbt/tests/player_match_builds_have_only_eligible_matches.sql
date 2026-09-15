select builds.recording_id, builds.participant_key
from {{ ref('player_match_builds') }} as builds
left join {{ ref('stg_matches') }} as matches
    on builds.recording_id = matches.recording_id
where matches.recording_id is null
   or not matches.is_balance_eligible
