select *
from {{ ref('item_balance') }}
where item_id = 0
   or trim(item_name) = ''
   or holders > eligible_players
   or holder_wins > observations_with_confirmed_result
   or pick_rate_pct < 0 or pick_rate_pct > 100
   or win_rate_pct < 0 or win_rate_pct > 100
   or avg_item_count <= 0
   or (observations_with_confirmed_result = 0 and win_rate_pct is not null)
   or (observations_with_confirmed_result > 0 and win_rate_pct is null)
