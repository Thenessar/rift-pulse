select game_mode, champion_name
from {{ ref('champion_balance') }}
group by game_mode, champion_name
having count(*) > 1
