with player_slots as (
    select
        game_mode,
        count(*) as player_slots
    from {{ ref('player_match_builds') }}
    group by game_mode
)

select
    builds.game_mode,
    builds.champion_name,
    count(*) as picks,
    max(player_slots.player_slots) as player_slots,
    round(100.0 * count(*) / nullif(max(player_slots.player_slots), 0), 4) as pick_rate_pct,
    count_if(builds.is_outcome_confirmed) as games_with_confirmed_result,
    count_if(builds.is_outcome_confirmed and builds.won_match) as wins,
    round(
        100.0 * count_if(builds.is_outcome_confirmed and builds.won_match)
        / nullif(count_if(builds.is_outcome_confirmed), 0),
        4
    ) as win_rate_pct,
    max(builds.match_ended_at) as last_match_at,
    round(
        100.0 * count_if(builds.is_outcome_confirmed) / nullif(count(*), 0),
        4
    ) as confirmed_outcome_share_pct
from {{ ref('player_match_builds') }} as builds
inner join player_slots
    on builds.game_mode = player_slots.game_mode
group by builds.game_mode, builds.champion_name
