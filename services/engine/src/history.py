import glob
import gzip
import json
import logging
import os
from datetime import datetime
from typing import Any

from services.engine.src.poller import CANONICAL_ROLES, ROLE_LABELS, assign_team_roles

logger = logging.getLogger("rift-pulse.history")

# Simple in-memory cache based on file mtime
_MATCHES_CACHE: dict[str, dict[str, Any]] = {}


def get_match_history(data_dir: str = "data/matches") -> list[dict[str, Any]]:
    """
    Scans the match data directory and returns a sorted list of completed/recorded games.
    Uses an in-memory cache (mtime check) to avoid excessive disk I/O on each request.
    """
    jsonl_files = glob.glob(os.path.join(data_dir, "session_*.jsonl"))
    matches = []

    for fpath in jsonl_files:
        try:
            mtime = os.path.getmtime(fpath)
            cached = _MATCHES_CACHE.get(fpath)
            if cached and cached.get("_mtime") == mtime:
                matches.append(cached["data"])
                continue

            parsed = parse_single_session(fpath, data_dir)
            if parsed:
                parsed["_mtime"] = mtime
                _MATCHES_CACHE[fpath] = {"_mtime": mtime, "data": parsed}
                matches.append(parsed)
        except Exception as e:
            logger.warning(f"Error processing session {fpath}: {e}")

    # Sort descending by timestamp (newest first)
    matches.sort(key=lambda m: m.get("timestamp", 0), reverse=True)
    return matches


def parse_single_session(fpath: str, data_dir: str = "data/matches") -> dict[str, Any] | None:
    sid = os.path.basename(fpath).replace("session_", "").replace(".jsonl", "")
    sum_path = os.path.join(data_dir, f"session_{sid}_summary.json")
    summary = {}
    if os.path.exists(sum_path):
        try:
            with open(sum_path, encoding="utf-8") as sf:
                summary = json.load(sf)
        except Exception:
            pass

    first_line = None
    last_line = None
    win_prob_history = []
    seen_minutes = set()

    # Read and sample minute-by-minute win probability
    try:
        with open(fpath, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if first_line is None:
                    first_line = line
                last_line = line

                try:
                    obj = json.loads(line)
                    sec = obj.get("game_time_seconds", 0.0)
                    m = int(sec // 60)
                    if m not in seen_minutes:
                        seen_minutes.add(m)
                        inf = obj.get("inference", {})
                        p_blue = inf.get("win_prob_blue_pct")
                        if p_blue is None:
                            p_blue = round(float(inf.get("win_probability_blue", 0.5)) * 100.0, 1)
                        win_prob_history.append({"minute": m, "time": f"{m:02d}:00", "prob_blue": p_blue})
                except Exception:
                    pass
    except Exception as e:
        logger.error(f"Error reading file {fpath}: {e}")
        return None

    if not last_line:
        return None

    # Ensure minute 0 is always present as baseline
    if 0 not in seen_minutes:
        win_prob_history.insert(0, {"minute": 0, "time": "00:00", "prob_blue": 50.0})

    try:
        last_obj = json.loads(last_line)
    except Exception:
        return None

    mtime = os.path.getmtime(fpath)
    date_str = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")

    matchups = last_obj.get("matchups", [])
    blue_team = []
    red_team = []
    for m in matchups:
        bp = m.get("blue_player")
        rp = m.get("red_player")
        if bp and bp.get("champion"):
            blue_team.append(bp)
        if rp and rp.get("champion"):
            red_team.append(rp)

    # Enrich players with keystone if missing from session .jsonl by reading first line of raw gz
    raw_gz_path = os.path.join(data_dir, f"raw_session_{sid}.jsonl.gz")
    if os.path.exists(raw_gz_path):
        try:
            with gzip.open(raw_gz_path, "rt", encoding="utf-8") as gzf:
                raw_first_line = gzf.readline()
                if raw_first_line:
                    raw_data = json.loads(raw_first_line)
                    raw_players = raw_data.get("allPlayers", [])

                    rune_lookup = {}
                    for rp_raw in raw_players:
                        tm = "ORDER" if rp_raw.get("team") == "ORDER" else "CHAOS"
                        ch = rp_raw.get("championName", "")
                        sn = rp_raw.get("riotIdGameName") or rp_raw.get("summonerName") or ch
                        ks = rp_raw.get("runes", {}).get("keystone", {})
                        kid = ks.get("id")
                        kname = ks.get("displayName", "")
                        if kid:
                            rune_lookup[(tm, ch)] = (kid, kname)
                            rune_lookup[(tm, sn)] = (kid, kname)
                            rune_lookup[ch] = (kid, kname)

                    for p in blue_team:
                        if not p.get("keystone_id"):
                            kid, kname = rune_lookup.get(
                                ("ORDER", p.get("champion")), rune_lookup.get(p.get("champion"), (None, ""))
                            )
                            if kid:
                                p["keystone_id"] = kid
                                p["keystone_name"] = kname

                    for p in red_team:
                        if not p.get("keystone_id"):
                            kid, kname = rune_lookup.get(
                                ("CHAOS", p.get("champion")), rune_lookup.get(p.get("champion"), (None, ""))
                            )
                            if kid:
                                p["keystone_id"] = kid
                                p["keystone_name"] = kname
        except Exception as e:
            logger.debug(f"Could not extract runes from {raw_gz_path}: {e}")

    # Re-order matchups strictly into canonical role order (TOP -> JUNGLE -> MID -> BOT -> SUPPORT)
    if blue_team or red_team:
        blue_role_map = assign_team_roles(blue_team)
        red_role_map = assign_team_roles(red_team)
        canonical_matchups = []

        for i, role in enumerate(CANONICAL_ROLES):
            bp = blue_role_map.get(role)
            rp = red_role_map.get(role)
            if not bp and not rp:
                continue

            b_gold = bp["items_gold"] if bp else 0
            r_gold = rp["items_gold"] if rp else 0
            diff = b_gold - r_gold
            leader = "EQUAL"
            if diff > 0:
                leader = "BLUE"
            elif diff < 0:
                leader = "RED"

            canonical_matchups.append(
                {
                    "lane_index": i,
                    "role": role,
                    "role_label": ROLE_LABELS.get(role, role),
                    "blue_player": bp,
                    "red_player": rp,
                    "gold_diff_abs": abs(diff),
                    "leader": leader,
                }
            )

        if canonical_matchups:
            matchups = canonical_matchups
            blue_team = [m["blue_player"] for m in matchups if m.get("blue_player")]
            red_team = [m["red_player"] for m in matchups if m.get("red_player")]

    metrics = last_obj.get("metrics", {})
    inference = last_obj.get("inference", {})
    win_prob_blue = summary.get("final_win_prob_blue", inference.get("win_probability_blue", 0.5))
    winner = summary.get("predicted_winner", "BLUE" if win_prob_blue >= 0.5 else "RED")
    duration = summary.get("duration_formatted", last_obj.get("game_time_formatted", "00:00"))
    game_mode = summary.get("game_mode", last_obj.get("game_mode", "CLASSIC"))
    if str(game_mode).upper() in ("CHERRY", "ARENA"):
        return None

    mode_labels = {
        "PRACTICETOOL": "Practice Tool",
        "CLASSIC": "Summoner's Rift",
        "ARAM": "ARAM",
        "CHERRY": "Arena",
        "URF": "Ultra Rapid Fire",
    }
    game_mode_label = mode_labels.get(game_mode, game_mode)

    if not win_prob_history:
        win_prob_history = [
            {"minute": 0, "time": "00:00", "prob_blue": 50.0},
            {
                "minute": max(1, int(last_obj.get("game_time_seconds", 60) // 60)),
                "time": duration,
                "prob_blue": round(win_prob_blue * 100.0, 1),
            },
        ]
    elif len(win_prob_history) == 1:
        win_prob_history.append(
            {
                "minute": max(1, int(last_obj.get("game_time_seconds", 60) // 60)),
                "time": duration,
                "prob_blue": round(win_prob_blue * 100.0, 1),
            }
        )

    return {
        "session_id": sid,
        "timestamp": mtime,
        "date_formatted": date_str,
        "duration": duration,
        "game_mode": game_mode,
        "game_mode_label": game_mode_label,
        "winner": winner,
        "win_prob_blue_pct": round(win_prob_blue * 100.0, 1),
        "win_prob_red_pct": round((1.0 - win_prob_blue) * 100.0, 1),
        "win_prob_history": win_prob_history,
        "metrics": metrics,
        "objectives": last_obj.get("objectives", {}),
        "matchups": matchups,
        "blue_team": blue_team,
        "red_team": red_team,
    }
