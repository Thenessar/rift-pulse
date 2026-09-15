with eligible_players as (
    select
        game_mode,
        count(*) as eligible_players
    from {{ ref('player_match_builds') }}
    group by game_mode
),

holder_items as (
    select
        builds.recording_id,
        builds.participant_key,
        builds.game_mode,
        builds.won_match,
        builds.is_outcome_confirmed,
        builds.match_ended_at,
        item.item_id,
        item.item_name,
        item.item_count
    from {{ ref('player_match_builds') }} as builds
    lateral view explode(builds.items) exploded as item
)

select
    items.game_mode,
    items.item_id,
    items.item_name,
    count(*) as holders,
    count(distinct items.recording_id) as matches_with_item,
    max(eligible_players.eligible_players) as eligible_players,
    round(100.0 * count(*) / nullif(max(eligible_players.eligible_players), 0), 4) as pick_rate_pct,
    count_if(items.is_outcome_confirmed and items.won_match) as holder_wins,
    count_if(items.is_outcome_confirmed) as observations_with_confirmed_result,
    round(
        100.0 * count_if(items.is_outcome_confirmed and items.won_match)
        / nullif(count_if(items.is_outcome_confirmed), 0),
        4
    ) as win_rate_pct,
    round(avg(items.item_count), 4) as avg_item_count,
    max(items.match_ended_at) as last_match_at
from holder_items as items
inner join eligible_players
    on items.game_mode = eligible_players.game_mode
group by items.game_mode, items.item_id, items.item_name
