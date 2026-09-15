from typing import Any


def _identity_values(player: dict[str, Any]) -> set[str]:
    values = {
        str(player.get("riotId") or "").strip().casefold(),
        str(player.get("summonerName") or "").strip().casefold(),
    }
    game_name = str(player.get("riotIdGameName") or "").strip()
    tag_line = str(player.get("riotIdTagLine") or "").strip()
    if game_name and tag_line:
        values.add(f"{game_name}#{tag_line}".casefold())
    return {value for value in values if value}


def extract_observed_outcome(raw_data: dict[str, Any]) -> dict[str, Any]:
    """Return a trusted winner only for a GameEnd event and exact player match."""
    events = (raw_data.get("events") or {}).get("Events") or []
    game_end = next(
        (event for event in reversed(events) if str(event.get("EventName") or "").casefold() == "gameend"),
        None,
    )
    result = str((game_end or {}).get("Result") or "").strip().upper()
    if result not in {"WIN", "LOSE"}:
        return {"observed_winner": None, "outcome_status": "UNKNOWN", "outcome_source": None}

    active_identities = _identity_values(raw_data.get("activePlayer") or {})
    matched_teams = {
        str(player.get("team") or "").strip().upper()
        for player in (raw_data.get("allPlayers") or [])
        if active_identities.intersection(_identity_values(player))
    }
    matched_teams.discard("")
    if len(matched_teams) != 1:
        return {"observed_winner": None, "outcome_status": "UNKNOWN", "outcome_source": None}

    active_team = matched_teams.pop()
    if active_team not in {"ORDER", "CHAOS"}:
        return {"observed_winner": None, "outcome_status": "UNKNOWN", "outcome_source": None}

    winner_source = active_team if result == "WIN" else ("CHAOS" if active_team == "ORDER" else "ORDER")
    return {
        "observed_winner": "BLUE" if winner_source == "ORDER" else "RED",
        "outcome_status": "OBSERVED",
        "outcome_source": "riot_live_client_game_end",
    }
