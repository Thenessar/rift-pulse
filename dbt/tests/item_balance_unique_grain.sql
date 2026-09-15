select game_mode, item_id, item_name
from {{ ref('item_balance') }}
group by game_mode, item_id, item_name
having count(*) > 1
