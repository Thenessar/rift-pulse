select recording_id, participant_key
from {{ ref('player_match_builds') }}
group by recording_id, participant_key
having count(*) > 1
