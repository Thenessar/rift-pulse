select recording_id, participant_key
from {{ ref('player_match_builds') }}
where (not is_outcome_confirmed and won_match is not null)
   or (is_outcome_confirmed and won_match is null)
