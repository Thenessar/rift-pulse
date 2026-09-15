select *
from {{ ref('champion_balance') }}
where picks > player_slots
   or wins > games_with_confirmed_result
   or pick_rate_pct < 0 or pick_rate_pct > 100
   or win_rate_pct < 0 or win_rate_pct > 100
   or confirmed_outcome_share_pct < 0 or confirmed_outcome_share_pct > 100
   or (games_with_confirmed_result = 0 and win_rate_pct is not null)
   or (games_with_confirmed_result > 0 and win_rate_pct is null)
