select
    cast(recording_id as string) as recording_id,
    upper(cast(game_mode as string)) as game_mode,
    cast(observed_winner as string) as observed_winner,
    coalesce(cast(is_outcome_confirmed as boolean), false) as is_outcome_confirmed,
    coalesce(cast(is_balance_eligible as boolean), false) as is_balance_eligible,
    cast(observation_count as bigint) as observation_count,
    cast(min_participant_count as bigint) as min_participant_count,
    cast(max_participant_count as bigint) as max_participant_count,
    cast(last_observed_at_utc as timestamp) as last_observed_at_utc,
    cast(recorded_at_utc as timestamp) as recorded_at_utc,
    coalesce(cast(recorded_at_utc as timestamp), cast(last_observed_at_utc as timestamp)) as match_ended_at
from {{ source('silver', 'matches') }}
