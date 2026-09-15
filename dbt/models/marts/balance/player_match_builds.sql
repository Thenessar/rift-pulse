with eligible_participants as (
    select
        matches.recording_id,
        participants.participant_key,
        participants.participant_slot,
        matches.game_mode,
        participants.champion_name,
        participants.team,
        case
            when matches.is_outcome_confirmed
                then participants.team = matches.observed_winner
        end as won_match,
        matches.is_outcome_confirmed,
        matches.is_balance_eligible,
        matches.match_ended_at
    from {{ ref('stg_matches') }} as matches
    inner join {{ ref('stg_match_participants') }} as participants
        on matches.recording_id = participants.recording_id
    where matches.is_balance_eligible
      and not participants.is_bot
),

valid_items as (
    select
        participant_key,
        item_id,
        item_name,
        sum(item_count) as item_count
    from {{ ref('stg_match_participant_items') }}
    where item_id is not null
      and item_id <> 0
      and item_name is not null
      and trim(item_name) <> ''
      and item_count > 0
      and not is_consumable
    group by participant_key, item_id, item_name
),

item_builds as (
    select
        participant_key,
        count(*) as distinct_item_count,
        sum(item_count) as item_unit_count,
        sort_array(
            collect_list(
                named_struct(
                    'item_id', item_id,
                    'item_name', item_name,
                    'item_count', item_count
                )
            )
        ) as items
    from valid_items
    group by participant_key
)

select
    participants.recording_id,
    participants.participant_key,
    participants.participant_slot,
    participants.game_mode,
    participants.champion_name,
    participants.team,
    participants.won_match,
    participants.is_outcome_confirmed,
    participants.is_balance_eligible,
    coalesce(item_builds.distinct_item_count, 0) as distinct_item_count,
    coalesce(item_builds.item_unit_count, 0) as item_unit_count,
    coalesce(
        item_builds.items,
        cast(array() as array<struct<item_id:bigint,item_name:string,item_count:bigint>>)
    ) as items,
    participants.match_ended_at
from eligible_participants as participants
left join item_builds
    on participants.participant_key = item_builds.participant_key
