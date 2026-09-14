import asyncio
import glob
import gzip
import json
import logging
import os
import threading
import time
import uuid
from contextlib import suppress
from datetime import datetime
from typing import Any

import httpx
import numpy as np
import onnxruntime as ort

from services.engine.src.items import item_price_manager

logger = logging.getLogger("rift-pulse.engine")

ROLE_ORDER = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
CANONICAL_ROLES = ROLE_ORDER
ROLE_LABELS = {"TOP": "Top", "JUNGLE": "Jungle", "MIDDLE": "Mid", "BOTTOM": "Bot", "UTILITY": "Support"}

EXCLUDED_GAME_MODES = {"CHERRY", "ARENA"}


def normalize_role(pos: str | None) -> str | None:
    if not pos:
        return None
    p = str(pos).strip().upper()
    if p in ("TOP",):
        return "TOP"
    if p in ("JUNGLE", "JUG"):
        return "JUNGLE"
    if p in ("MIDDLE", "MID"):
        return "MIDDLE"
    if p in ("BOTTOM", "BOT", "ADC"):
        return "BOTTOM"
    if p in ("UTILITY", "SUPPORT", "SUP"):
        return "UTILITY"
    return None


def assign_team_roles(players: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    assigned = {}
    used_roles = set()
    unassigned = []

    for p in players:
        r = normalize_role(p.get("position"))
        if r and r not in used_roles:
            assigned[r] = p
            used_roles.add(r)
        else:
            unassigned.append(p)

    # If jungle is missing, look for Smite in spells
    if "JUNGLE" not in used_roles:
        for p in list(unassigned):
            spells = [str(s).lower() for s in p.get("spells", [])]
            if any("smite" in s for s in spells):
                assigned["JUNGLE"] = p
                used_roles.add("JUNGLE")
                unassigned.remove(p)
                break

    # Assign remaining unassigned players to remaining free roles in canonical order
    free_roles = [r for r in CANONICAL_ROLES if r not in used_roles]
    for p in unassigned:
        if free_roles:
            r = free_roles.pop(0)
            assigned[r] = p
            used_roles.add(r)
        else:
            break

    for r, p in assigned.items():
        p["assigned_role"] = r

    return assigned


class TelemetryEngine:
    def __init__(
        self,
        model_path: str = "services/engine/models/model.onnx",
        data_dir: str = "data/matches",
        auto_poll_idle: bool | None = None,
        idle_poll_interval: float | None = None,
        active_poll_interval: float | None = None,
        client_url: str | None = None,
    ):
        if client_url is not None:
            self.client_url = client_url
        else:
            self.client_url = os.getenv("LOL_CLIENT_URL", "https://127.0.0.1:2999/liveclientdata/allgamedata")

        self.model_path = model_path
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)

        self.session: ort.InferenceSession | None = None
        self.latest_data: dict[str, Any] | None = None
        self.history: list[dict[str, Any]] = []
        self.subscribers: list[asyncio.Queue] = []
        self._background_tasks: set[asyncio.Task[Any]] = set()

        self.session_id: str = str(uuid.uuid4())
        self.is_running: bool = False
        self.is_game_active: bool = False
        self.game_status: str = "WAITING_FOR_GAME"  # WAITING_FOR_GAME | IN_GAME | MATCH_COMPLETED
        self.tick_counter: int = 0

        # Output handles: normalized JSONL + True Bronze (raw gzip).
        self._current_file = None
        self._current_raw_file = None
        self._match_start_time: float | None = None

        # Auto-poll configuration: silently check every idle_poll_interval seconds
        if auto_poll_idle is None:
            env_auto = os.getenv("AUTO_POLL_IDLE", "true").strip().lower()
            self.auto_poll_idle: bool = env_auto in ("true", "1", "yes", "on")
        else:
            self.auto_poll_idle: bool = bool(auto_poll_idle)

        if idle_poll_interval is None:
            try:
                self.idle_poll_interval: float = float(os.getenv("IDLE_POLL_INTERVAL", "2.5"))
            except ValueError:
                self.idle_poll_interval: float = 2.5
        else:
            self.idle_poll_interval: float = float(idle_poll_interval)

        if active_poll_interval is None:
            try:
                self.active_poll_interval: float = float(os.getenv("ACTIVE_POLL_INTERVAL", "1.0"))
            except ValueError:
                self.active_poll_interval: float = 1.0
        else:
            self.active_poll_interval: float = float(active_poll_interval)

        self._wake_event: asyncio.Event | None = None

        self._load_model()

    def set_auto_poll(self, enabled: bool, interval: float | None = None):
        """Dynamically enable or disable background idle polling."""
        self.auto_poll_idle = bool(enabled)
        if interval is not None and interval > 0:
            self.idle_poll_interval = float(interval)
        if self.auto_poll_idle and self._wake_event is not None:
            self._wake_event.set()
        logger.info(
            f"Auto-poll configuration updated: enabled={self.auto_poll_idle}, interval={self.idle_poll_interval}s"
        )

    def get_minute_win_prob_history(self) -> list[dict[str, Any]]:
        """Samples win probability per minute from the current session history."""
        if not self.history:
            return [{"minute": 0, "time": "00:00", "prob_blue": 50.0}]

        minute_points = []
        seen_minutes = set()

        for h in self.history:
            sec = h.get("time_sec", 0.0)
            minute = int(sec // 60)
            if minute not in seen_minutes:
                seen_minutes.add(minute)
                prob_blue = round(float(h.get("prob_blue", 0.5)) * 100.0, 1)
                minute_points.append({"minute": minute, "time": f"{minute:02d}:00", "prob_blue": prob_blue})

        # Ensure the match always has minute 0 baseline (50.0%)
        if 0 not in seen_minutes:
            minute_points.insert(0, {"minute": 0, "time": "00:00", "prob_blue": 50.0})
            seen_minutes.add(0)

        # Ensure the latest recorded point is included
        if self.history:
            last = self.history[-1]
            last_sec = last.get("time_sec", 0.0)
            last_min = int(last_sec // 60)
            last_prob = round(float(last.get("prob_blue", 0.5)) * 100.0, 1)
            if minute_points and minute_points[-1]["minute"] != last_min:
                minute_points.append(
                    {"minute": last_min, "time": last.get("time", f"{last_min:02d}:00"), "prob_blue": last_prob}
                )
            elif not minute_points:
                minute_points.append({"minute": last_min, "time": last.get("time", "00:00"), "prob_blue": last_prob})

        return minute_points

    def _load_model(self):
        try:
            self.session = ort.InferenceSession(self.model_path)
            logger.info(f"ONNX model loaded successfully from: {self.model_path}")
        except Exception as e:
            logger.error(f"Failed to load ONNX model: {e}")
            self.session = None

    def _find_resumable_session(self, current_champs: set, game_time: float) -> tuple | None:
        """Searches recent session files to find if current live match matches an existing unfinalized/recent file."""
        now = time.time()
        pattern = os.path.join(self.data_dir, "session_*.jsonl")
        candidates = glob.glob(pattern)

        for fpath in sorted(candidates, key=lambda p: os.path.getmtime(p), reverse=True):
            try:
                # Only inspect files modified within the last 45 minutes
                mtime = os.path.getmtime(fpath)
                if now - mtime > 2700:
                    continue

                sid = os.path.basename(fpath).replace("session_", "").replace(".jsonl", "")
                if "_summary" in sid:
                    continue

                # Read last line
                last_line = None
                with open(fpath, encoding="utf-8") as f:
                    for line in f:
                        s = line.strip()
                        if s:
                            last_line = s

                if not last_line:
                    continue

                last_obj = json.loads(last_line)
                last_time = float(last_obj.get("game_time_seconds", 0.0) or 0.0)
                last_tick = int(last_obj.get("tick_id", 0) or 0)

                # Check champion roster from matchups and team arrays
                file_champs = set()
                for mu in last_obj.get("matchups", []):
                    bp = mu.get("blue_player")
                    rp = mu.get("red_player")
                    if bp and bp.get("champion"):
                        file_champs.add(bp["champion"])
                    if rp and rp.get("champion"):
                        file_champs.add(rp["champion"])

                for p in last_obj.get("blue_team", []) + last_obj.get("red_team", []):
                    if p.get("champion"):
                        file_champs.add(p["champion"])

                # If at least 60% of recorded champions match and game time is forward continuous (gap <= 300s)
                if file_champs:
                    overlap = len(file_champs.intersection(current_champs))
                    if (overlap / len(file_champs)) >= 0.6:
                        time_diff = game_time - last_time
                        if 0.0 <= time_diff <= 300.0:
                            return (sid, last_tick)
            except Exception as e:
                logger.debug(f"Error checking session {fpath} for resumption: {e}")

        return None

    def _ensure_match_session(self, raw_json: dict[str, Any]):
        """Starts a new match session or seamlessly resumes an existing ongoing session."""
        if self.is_game_active:
            return

        game_data = raw_json.get("gameData", {}) or {}
        game_mode = str(game_data.get("gameMode") or "").upper()
        if game_mode in EXCLUDED_GAME_MODES:
            logger.info(f"Skipping match session: game mode '{game_mode}' is excluded from tracking.")
            return

        game_time = float(game_data.get("gameTime", 0.0) or 0.0)
        all_players = raw_json.get("allPlayers", []) or []
        current_champs = {str(p.get("championName") or "") for p in all_players if p.get("championName")}

        # Check if we can resume an existing recent session
        if game_time > 10.0 and current_champs:
            resumed = self._find_resumable_session(current_champs, game_time)
            if resumed:
                sid, last_tick = resumed
                norm_filename = os.path.join(self.data_dir, f"session_{sid}.jsonl")
                raw_filename = os.path.join(self.data_dir, f"raw_session_{sid}.jsonl.gz")
                try:
                    self.session_id = sid
                    self.tick_counter = last_tick
                    self._current_file = open(norm_filename, "a", encoding="utf-8")
                    self._current_raw_file = gzip.open(raw_filename, "at", encoding="utf-8")
                    self.is_game_active = True
                    self.game_status = "IN_GAME"
                    logger.info(
                        f"RESUMED existing match session: {self.session_id} from tick {self.tick_counter} (game_time: {game_time:.1f}s)"
                    )
                    return
                except Exception as e:
                    logger.warning(f"Failed to resume session {sid}: {e}, starting fresh session.")

        self._start_new_match_session()

    def _start_new_match_session(self):
        """Initializes new match session files: normalized and True Bronze gzip."""
        self.session_id = str(uuid.uuid4())
        self.tick_counter = 0
        self.history = [{"time": "00:00", "time_sec": 0.0, "gold_diff": 0, "prob_blue": 0.5}]
        self._match_start_time = time.time()
        self.is_game_active = True
        self.game_status = "IN_GAME"

        norm_filename = os.path.join(self.data_dir, f"session_{self.session_id}.jsonl")
        raw_filename = os.path.join(self.data_dir, f"raw_session_{self.session_id}.jsonl.gz")

        try:
            self._current_file = open(norm_filename, "a", encoding="utf-8")
            self._current_raw_file = gzip.open(raw_filename, "wt", encoding="utf-8")
            logger.info(f"Started recording match: {norm_filename} and True Bronze: {raw_filename}")
        except Exception as e:
            logger.error(f"Failed to open session files: {e}")
            self._current_file = None
            self._current_raw_file = None

    def _persist_tick(self, payload: dict[str, Any], raw_json: dict[str, Any]):
        """Appends normalized state and 100% raw JSON to True Bronze."""
        if self._current_file is not None:
            try:
                line = json.dumps(payload, ensure_ascii=False)
                self._current_file.write(line + "\n")
                self._current_file.flush()
            except Exception as e:
                logger.error(f"Error writing tick: {e}")

        if self._current_raw_file is not None:
            try:
                raw_line = json.dumps(raw_json, ensure_ascii=False)
                self._current_raw_file.write(raw_line + "\n")
                self._current_raw_file.flush()
            except Exception as e:
                logger.error(f"Error writing True Bronze: {e}")

    def _finalize_match_session(self):
        """Closes session files, cleans up empty 0-tick files, and generates summary manifest."""
        if self._current_file is not None:
            with suppress(Exception):
                self._current_file.close()
            self._current_file = None

        if self._current_raw_file is not None:
            with suppress(Exception):
                self._current_raw_file.close()
                logger.info(f"Successfully compressed and closed True Bronze for session: {self.session_id}")
            self._current_raw_file = None

        self.game_status = "MATCH_COMPLETED"

        # Discard empty 0-tick sessions or excluded game modes completely
        current_mode = str((self.latest_data or {}).get("game_mode", "")).upper()
        if self.tick_counter == 0 or current_mode in EXCLUDED_GAME_MODES:
            norm_filename = os.path.join(self.data_dir, f"session_{self.session_id}.jsonl")
            raw_filename = os.path.join(self.data_dir, f"raw_session_{self.session_id}.jsonl.gz")
            sum_filename = os.path.join(self.data_dir, f"session_{self.session_id}_summary.json")
            for p in (norm_filename, raw_filename, sum_filename):
                if os.path.exists(p):
                    with suppress(Exception):
                        os.remove(p)
            logger.info(f"Discarded match session {self.session_id} (ticks={self.tick_counter}, mode={current_mode})")
            return

        if self.latest_data is not None and self.latest_data.get("session_id") == self.session_id:
            summary = {
                "session_id": self.session_id,
                "recorded_at": datetime.utcnow().isoformat(),
                "duration_formatted": self.latest_data.get("game_time_formatted", "00:00"),
                "duration_seconds": self.latest_data.get("game_time_seconds", 0.0),
                "total_ticks": self.tick_counter,
                "final_metrics": self.latest_data.get("metrics", {}),
                "final_win_prob_blue": self.latest_data.get("inference", {}).get("win_probability_blue", 0.5),
                "predicted_winner": "BLUE"
                if self.latest_data.get("inference", {}).get("win_probability_blue", 0.5) >= 0.5
                else "RED",
                "game_mode": self.latest_data.get("game_mode", "UNKNOWN"),
                "raw_bronze_file": f"raw_session_{self.session_id}.jsonl.gz",
            }
            summary_path = os.path.join(self.data_dir, f"session_{self.session_id}_summary.json")
            try:
                with open(summary_path, "w", encoding="utf-8") as f:
                    json.dump(summary, f, indent=2, ensure_ascii=False)
                logger.info(f"Saved match summary: {summary_path}")
            except Exception as e:
                logger.error(f"Error saving match summary: {e}")

            # Background upload to S3 landing zone with bounded retries.
            try:
                sid = self.session_id
                end_date = summary.get("recorded_at", "")[:10] or None
                t = threading.Thread(
                    target=self._upload_session_with_retries,
                    args=(sid, end_date),
                    daemon=True,
                    name=f"S3Upload-{sid[:8]}",
                )
                t.start()
                logger.info(f"Triggered background S3 landing upload for session {sid} (date={end_date})")
            except Exception as e:
                logger.warning(f"Failed to initiate background S3 upload: {e}")

    def _upload_session_with_retries(self, session_id: str, game_end_date: str | None) -> None:
        """Uploads a completed session and retries transient failures without blocking telemetry."""
        from services.archiver.s3_archiver import S3Archiver

        try:
            max_attempts = max(1, int(os.getenv("S3_UPLOAD_MAX_ATTEMPTS", "3")))
        except ValueError:
            max_attempts = 3
        try:
            retry_delay = max(0.0, float(os.getenv("S3_UPLOAD_RETRY_DELAY", "5")))
        except ValueError:
            retry_delay = 5.0

        archiver = S3Archiver(data_dir=self.data_dir)
        for attempt in range(1, max_attempts + 1):
            result = archiver.upload_session_to_landing(session_id, game_end_date)
            if result.get("status") != "error":
                return
            if attempt < max_attempts:
                delay = retry_delay * attempt
                logger.warning(
                    "S3 upload for session %s failed (attempt %s/%s); retrying in %.1fs",
                    session_id,
                    attempt,
                    max_attempts,
                    delay,
                )
                time.sleep(delay)

        logger.error("S3 upload for session %s failed after %s attempts", session_id, max_attempts)

    def calculate_metrics_and_features(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        game_data = raw_data.get("gameData", {})
        game_time = float(game_data.get("gameTime", 0.0))
        all_players = raw_data.get("allPlayers", [])
        events_list = raw_data.get("events", {}).get("Events", [])

        # 1. Collect detailed player stats
        blue_players = []
        red_players = []
        blue_items_gold = 0
        red_items_gold = 0
        blue_kills = 0
        red_kills = 0
        blue_levels = []
        red_levels = []

        for p in all_players:
            team = p.get("team", "")
            items = p.get("items") or []

            # Partition items: 6 main slots + 1 trinket & calculate total gold via Data Dragon
            items_gold = 0
            regular_items = []
            trinket = None
            for item in items:
                if not isinstance(item, dict):
                    continue
                iid = item.get("itemID", 0)
                raw_price = item.get("price", 0)
                count = item.get("count", 1)
                unit_total = item_price_manager.get_item_price(iid, fallback=raw_price)
                items_gold += unit_total * count

                item_enriched = dict(item)
                item_enriched["total_price"] = unit_total

                slot = item.get("slot", 0)
                if slot == 6:
                    trinket = item_enriched
                else:
                    regular_items.append(item_enriched)

            scores = p.get("scores") or {}
            kills = scores.get("kills", 0)
            deaths = scores.get("deaths", 0)
            assists = scores.get("assists", 0)
            cs = scores.get("creepScore", 0)
            level = p.get("level", 1)
            champ_name = p.get("championName") or "Unknown"
            summoner_name = str(p.get("riotIdGameName") or p.get("summonerName") or champ_name)

            # Summoner spells
            spells = p.get("summonerSpells") or {}
            spell1 = (spells.get("summonerSpellOne") or {}).get("displayName", "")
            spell2 = (spells.get("summonerSpellTwo") or {}).get("displayName", "")

            # Keystone rune
            keystone = (p.get("runes") or {}).get("keystone") or {}
            keystone_id = keystone.get("id")
            keystone_name = keystone.get("displayName", "")

            player_obj = {
                "summoner_name": summoner_name,
                "champion": champ_name,
                "team": "ORDER" if team == "ORDER" else "CHAOS",
                "team_label": "Blue" if team == "ORDER" else "Red",
                "position": p.get("position", "NONE"),
                "level": level,
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "cs": cs,
                "items_gold": items_gold,
                "items": regular_items[:6],
                "trinket": trinket,
                "spells": [spell1, spell2],
                "keystone_id": keystone_id,
                "keystone_name": keystone_name,
            }

            if team == "ORDER":
                blue_players.append(player_obj)
                blue_items_gold += items_gold
                blue_kills += kills
                blue_levels.append(level)
            else:
                red_players.append(player_obj)
                red_items_gold += items_gold
                red_kills += kills
                red_levels.append(level)

        # 2. Budowa 5 par meczowych (Lane Matchups dla Scoreboardu - canonical order)
        matchups = []
        blue_role_map = assign_team_roles(blue_players)
        red_role_map = assign_team_roles(red_players)

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

            matchups.append(
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

        # 3. Analyze map events, dragon types and timers
        blue_towers = 0
        red_towers = 0
        blue_dragons = 0
        red_dragons = 0
        blue_barons = 0
        red_barons = 0
        blue_heralds = 0
        red_heralds = 0
        blue_voidgrubs = 0
        red_voidgrubs = 0
        blue_inhibs = 0
        red_inhibs = 0

        dragon_events = []
        herald_events = []
        baron_events = []
        blue_dragons_list = []
        red_dragons_list = []

        for ev in events_list:
            ev_name = ev.get("EventName", "")
            killer = str(ev.get("KillerName") or "")
            ev_time = float(ev.get("EventTime", 0.0))

            killer_team = None
            killer_clean = killer.split("#")[0].strip().lower() if killer else ""
            for p in all_players:
                p_sname = str(p.get("summonerName") or "").split("#")[0].strip().lower()
                p_rname = str(p.get("riotIdGameName") or "").strip().lower()
                p_cname = str(p.get("championName") or "").strip().lower()
                player_names = [name for name in (p_sname, p_rname, p_cname) if name]
                if killer_clean and any(
                    killer_clean == name or killer_clean in name or name in killer_clean for name in player_names
                ):
                    killer_team = p.get("team")
                    break

            if ev_name == "TurretKilled":
                turret_id = ev.get("TurretKilled", "")
                if "TOrder" in turret_id:
                    red_towers += 1
                elif "TChaos" in turret_id or killer_team == "ORDER":
                    blue_towers += 1
                elif killer_team == "CHAOS":
                    red_towers += 1

            elif ev_name == "DragonKill":
                d_type = ev.get("DragonType") or "Elemental"
                if "Fire" in d_type or "Infernal" in d_type:
                    d_name = "Infernal"
                elif "Earth" in d_type or "Mountain" in d_type:
                    d_name = "Mountain"
                elif "Water" in d_type or "Ocean" in d_type:
                    d_name = "Ocean"
                elif "Air" in d_type or "Cloud" in d_type:
                    d_name = "Cloud"
                elif "Hextech" in d_type:
                    d_name = "Hextech"
                elif "Chemtech" in d_type:
                    d_name = "Chemtech"
                elif "Elder" in d_type:
                    d_name = "Elder"
                else:
                    d_name = d_type

                d_record = {"time": ev_time, "type": d_name, "killer": killer, "team": killer_team}
                dragon_events.append(d_record)

                if killer_team == "ORDER":
                    blue_dragons += 1
                    blue_dragons_list.append(d_name)
                elif killer_team == "CHAOS":
                    red_dragons += 1
                    red_dragons_list.append(d_name)

            elif ev_name in ("HordeKill", "VoidgrubKill"):
                if killer_team == "ORDER":
                    blue_voidgrubs += 1
                elif killer_team == "CHAOS":
                    red_voidgrubs += 1

            elif ev_name == "BaronKill":
                baron_events.append({"time": ev_time, "killer": killer, "team": killer_team})
                if killer_team == "ORDER":
                    blue_barons += 1
                elif killer_team == "CHAOS":
                    red_barons += 1

            elif ev_name == "HeraldKill":
                herald_events.append({"time": ev_time, "killer": killer, "team": killer_team})
                if killer_team == "ORDER":
                    blue_heralds += 1
                elif killer_team == "CHAOS":
                    red_heralds += 1

            elif ev_name == "InhibKilled":
                inhib_id = ev.get("InhibKilled", "")
                if "TOrder" in inhib_id or killer_team == "CHAOS":
                    red_inhibs += 1
                else:
                    blue_inhibs += 1

        # Calculate Dragon Timers (Spawn: 5:00 = 300s, Respawn: 5:00 = 300s, Elder: 6:00 = 360s)
        last_d_time = max([e["time"] for e in dragon_events], default=None)
        if last_d_time is None:
            if game_time < 300.0:
                rem = max(0, int(300.0 - game_time))
                dragon_timer = {
                    "status": "SPAWNING",
                    "label": f"{rem // 60}:{rem % 60:02d}",
                    "seconds": rem,
                    "name": "DRAGON",
                    "is_elder": False,
                }
            else:
                dragon_timer = {"status": "ALIVE", "label": "ALIVE", "seconds": 0, "name": "DRAGON", "is_elder": False}
        else:
            is_elder = (blue_dragons >= 4 or red_dragons >= 4) or any(e["type"] == "Elder" for e in dragon_events)
            interval = 360.0 if is_elder else 300.0
            respawn_at = last_d_time + interval
            if game_time < respawn_at:
                rem = max(0, int(respawn_at - game_time))
                dragon_timer = {
                    "status": "RESPAWNING",
                    "label": f"{rem // 60}:{rem % 60:02d}",
                    "seconds": rem,
                    "name": "ELDER" if is_elder else "DRAGON",
                    "is_elder": is_elder,
                }
            else:
                dragon_timer = {
                    "status": "ALIVE",
                    "label": "ALIVE",
                    "seconds": 0,
                    "name": "ELDER" if is_elder else "DRAGON",
                    "is_elder": is_elder,
                }

        # Calculate Baron / Herald Timers (Herald: 14:00 = 840s, Baron: 20:00 = 1200s, Baron Respawn: 6:00 = 360s)
        if game_time < 840.0:
            rem = max(0, int(840.0 - game_time))
            baron_herald_timer = {
                "type": "HERALD",
                "name": "HERALD",
                "status": "SPAWNING",
                "label": f"{rem // 60}:{rem % 60:02d}",
                "seconds": rem,
            }
        elif game_time < 1185.0:
            if len(herald_events) > 0:
                rem = max(0, int(1200.0 - game_time))
                baron_herald_timer = {
                    "type": "BARON",
                    "name": "BARON",
                    "status": "SPAWNING",
                    "label": f"{rem // 60}:{rem % 60:02d}",
                    "seconds": rem,
                }
            else:
                baron_herald_timer = {
                    "type": "HERALD",
                    "name": "HERALD",
                    "status": "ALIVE",
                    "label": "ALIVE",
                    "seconds": 0,
                }
        elif game_time < 1200.0:
            rem = max(0, int(1200.0 - game_time))
            baron_herald_timer = {
                "type": "BARON",
                "name": "BARON",
                "status": "SPAWNING",
                "label": f"{rem // 60}:{rem % 60:02d}",
                "seconds": rem,
            }
        else:
            last_b_time = max([e["time"] for e in baron_events], default=None)
            if last_b_time is None:
                baron_herald_timer = {
                    "type": "BARON",
                    "name": "BARON",
                    "status": "ALIVE",
                    "label": "ALIVE",
                    "seconds": 0,
                }
            else:
                respawn_at = last_b_time + 360.0
                if game_time < respawn_at:
                    rem = max(0, int(respawn_at - game_time))
                    baron_herald_timer = {
                        "type": "BARON",
                        "name": "BARON",
                        "status": "RESPAWNING",
                        "label": f"{rem // 60}:{rem % 60:02d}",
                        "seconds": rem,
                    }
                else:
                    baron_herald_timer = {
                        "type": "BARON",
                        "name": "BARON",
                        "status": "ALIVE",
                        "label": "ALIVE",
                        "seconds": 0,
                    }

        # 4. Total gold and feature vector
        base_gold_pool = (game_time / 60.0) * 5 * 100.0 if game_time > 90 else 0
        blue_total_gold = blue_items_gold + (blue_towers * 250) + (blue_kills * 300) + int(base_gold_pool)
        red_total_gold = red_items_gold + (red_towers * 250) + (red_kills * 300) + int(base_gold_pool)
        gold_diff = float(blue_total_gold - red_total_gold)

        kills_diff = blue_kills - red_kills
        towers_diff = blue_towers - red_towers
        dragons_diff = blue_dragons - red_dragons
        barons_diff = blue_barons - red_barons
        heralds_diff = blue_heralds - red_heralds
        inhibs_diff = blue_inhibs - red_inhibs

        mean_blue_lvl = np.mean(blue_levels) if blue_levels else 1.0
        mean_red_lvl = np.mean(red_levels) if red_levels else 1.0
        level_advantage = float(mean_blue_lvl - mean_red_lvl)

        features = np.array(
            [
                [
                    game_time,
                    gold_diff,
                    float(kills_diff),
                    float(towers_diff),
                    float(dragons_diff),
                    float(barons_diff),
                    float(heralds_diff),
                    float(inhibs_diff),
                    level_advantage,
                ]
            ],
            dtype=np.float32,
        )

        start_inf = time.perf_counter()
        if self.session is not None:
            try:
                preds = self.session.run(None, {"features": features})[0][0]
                win_prob_blue = float(preds[0])
                win_prob_red = float(preds[1])
            except Exception as e:
                logger.error(f"Error during ONNX inference: {e}")
                win_prob_blue = 0.5
                win_prob_red = 0.5
        else:
            z = (gold_diff / 3000.0) + (towers_diff * 0.2) + (dragons_diff * 0.15)
            win_prob_blue = 1.0 / (1.0 + np.exp(-z))
            win_prob_red = 1.0 - win_prob_blue

        inf_latency_ms = (time.perf_counter() - start_inf) * 1000.0

        mins = int(game_time // 60)
        secs = int(game_time % 60)
        formatted_time = f"{mins:02d}:{secs:02d}"

        self.tick_counter += 1

        payload = {
            "session_id": self.session_id,
            "tick_id": self.tick_counter,
            "game_status": self.game_status,
            "game_time_seconds": round(game_time, 1),
            "game_time_formatted": formatted_time,
            "game_mode": game_data.get("gameMode", "CLASSIC"),
            "metrics": {
                "blue_gold": int(blue_total_gold),
                "red_gold": int(red_total_gold),
                "gold_diff": int(gold_diff),
                "blue_gold_k": f"{blue_total_gold / 1000.0:.1f}K",
                "red_gold_k": f"{red_total_gold / 1000.0:.1f}K",
                "gold_diff_k": f"{abs(gold_diff) / 1000.0:.1f}K",
                "blue_kills": blue_kills,
                "red_kills": red_kills,
                "kills_diff": kills_diff,
                "blue_towers": blue_towers,
                "red_towers": red_towers,
                "towers_diff": towers_diff,
                "blue_dragons": blue_dragons,
                "red_dragons": red_dragons,
                "dragons_diff": dragons_diff,
                "blue_barons": blue_barons,
                "red_barons": red_barons,
                "blue_heralds": blue_heralds,
                "red_heralds": red_heralds,
                "blue_voidgrubs": blue_voidgrubs,
                "red_voidgrubs": red_voidgrubs,
                "level_advantage": round(level_advantage, 2),
            },
            "objectives": {
                "timers": {"dragon": dragon_timer, "baron_herald": baron_herald_timer},
                "blue": {
                    "dragons": blue_dragons_list,
                    "dragons_count": blue_dragons,
                    "voidgrubs": blue_voidgrubs,
                    "heralds": blue_heralds,
                    "barons": blue_barons,
                },
                "red": {
                    "dragons": red_dragons_list,
                    "dragons_count": red_dragons,
                    "voidgrubs": red_voidgrubs,
                    "heralds": red_heralds,
                    "barons": red_barons,
                },
            },
            "matchups": matchups,
            "blue_team": blue_players,
            "red_team": red_players,
            "win_prob_history": self.get_minute_win_prob_history(),
            "inference": {
                "win_probability_blue": round(win_prob_blue, 4),
                "win_probability_red": round(win_prob_red, 4),
                "win_prob_blue_pct": round(win_prob_blue * 100.0, 1),
                "win_prob_red_pct": round(win_prob_red * 100.0, 1),
                "confidence_score": round(max(win_prob_blue, win_prob_red), 4),
                "inference_latency_ms": round(inf_latency_ms, 2),
                "model_engine": "onnxruntime-cpu",
                "model_version": "v1.0.0-calibrated",
            },
        }
        return payload

    async def check_for_live_game(self) -> dict[str, Any]:
        """Checks for active League of Legends match on-demand (Find Live Game)."""
        if self._wake_event is None:
            self._wake_event = asyncio.Event()

        try:
            async with httpx.AsyncClient(verify=False, timeout=1.5) as client:
                resp = await client.get(self.client_url)
                if resp.status_code == 200:
                    raw_json = resp.json()
                    game_data = raw_json.get("gameData", {}) or {}
                    game_mode = str(game_data.get("gameMode") or "").upper()
                    if game_mode in EXCLUDED_GAME_MODES:
                        logger.info(f"check_for_live_game: detected excluded game mode '{game_mode}' (Arena).")
                        return {
                            "found": True,
                            "active": False,
                            "ignored": True,
                            "game_mode": game_mode,
                            "message": f"Arena mode detected ({game_mode}); this mode is excluded from tracking.",
                        }

                    if not self.is_game_active:
                        self._ensure_match_session(raw_json)

                    payload = self.calculate_metrics_and_features(raw_json)
                    self.latest_data = payload
                    self._persist_tick(payload, raw_json)

                    # Wake up polling loop
                    self._wake_event.set()

                    for queue in list(self.subscribers):
                        with suppress(asyncio.QueueFull):
                            queue.put_nowait(payload)

                    return {
                        "found": True,
                        "active": True,
                        "session_id": self.session_id,
                        "game_time": payload.get("game_time_formatted"),
                        "game_mode": payload.get("game_mode"),
                        "tick_id": self.tick_counter,
                    }
                else:
                    return {
                        "found": False,
                        "active": False,
                        "message": f"Live Client API returned status {resp.status_code}",
                    }
        except Exception as e:
            logger.debug(f"check_for_live_game failed: {e}")
            return {
                "found": False,
                "active": False,
                "message": "No active League of Legends game detected (Live Client API unreachable).",
            }

    async def poll_loop(self):
        """1 Hz loop persisting True Bronze and broadcasting Hot Path telemetry with background auto-detection."""
        self.is_running = True
        if self._wake_event is None:
            self._wake_event = asyncio.Event()
        logger.info(
            f"Telemetry loop started for {self.client_url} (auto_poll_idle={self.auto_poll_idle}, idle_interval={self.idle_poll_interval}s)"
        )

        # Trigger background sync of item prices from Data Dragon without blocking
        try:
            sync_task = asyncio.create_task(item_price_manager.update_from_ddragon())
            self._background_tasks.add(sync_task)
            sync_task.add_done_callback(self._background_tasks.discard)
        except Exception as dde:
            logger.debug(f"Could not schedule background Data Dragon sync: {dde}")

        async with httpx.AsyncClient(verify=False, timeout=1.5) as client:
            consecutive_failures = 0
            while self.is_running:
                if not self.is_game_active and not self.auto_poll_idle:
                    # Wait for signal from 'Find Live Game' button or auto_poll activation
                    try:
                        await asyncio.wait_for(self._wake_event.wait(), timeout=10.0)
                    except asyncio.TimeoutError:
                        continue
                    finally:
                        self._wake_event.clear()

                try:
                    resp = await client.get(self.client_url)
                    if resp.status_code == 200:
                        raw_json = resp.json()
                        consecutive_failures = 0

                        game_data = raw_json.get("gameData", {}) or {}
                        game_mode = str(game_data.get("gameMode") or "").upper()
                        if game_mode in EXCLUDED_GAME_MODES:
                            if self.is_game_active:
                                logger.info(f"Match is in excluded mode '{game_mode}' (Arena). Discarding session.")
                                self.is_game_active = False
                                self._finalize_match_session()
                            await asyncio.sleep(self.idle_poll_interval)
                            continue

                        if not self.is_game_active:
                            logger.info(
                                f"Auto-poll detected active LoL match ({game_mode or 'CLASSIC'}). Starting recording session..."
                            )
                            self._ensure_match_session(raw_json)

                        try:
                            payload = self.calculate_metrics_and_features(raw_json)
                            self.latest_data = payload

                            # Dual storage: normalized + True Bronze (raw gzip)
                            self._persist_tick(payload, raw_json)

                            self.history.append(
                                {
                                    "time": payload["game_time_formatted"],
                                    "time_sec": payload["game_time_seconds"],
                                    "gold_diff": payload["metrics"]["gold_diff"],
                                    "prob_blue": payload["inference"]["win_probability_blue"],
                                }
                            )
                            if len(self.history) > 3600:
                                self.history.pop(0)

                            for queue in list(self.subscribers):
                                with suppress(asyncio.QueueFull):
                                    queue.put_nowait(payload)
                        except Exception as calc_err:
                            logger.error(f"Error calculating metrics or persisting tick: {calc_err}", exc_info=True)
                    else:
                        consecutive_failures = min(consecutive_failures + 1, 100)
                except Exception:
                    # Silent idle polling: client is unreachable when LoL is not running
                    consecutive_failures = min(consecutive_failures + 1, 100)

                if self.is_game_active and consecutive_failures >= 4:
                    logger.info("Match completion / game exit detected.")
                    self.is_game_active = False
                    self._finalize_match_session()
                    consecutive_failures = 0
                    if self.latest_data is not None:
                        self.latest_data["game_status"] = "MATCH_COMPLETED"
                        for queue in list(self.subscribers):
                            with suppress(asyncio.QueueFull):
                                queue.put_nowait(self.latest_data)

                await asyncio.sleep(self.active_poll_interval if self.is_game_active else self.idle_poll_interval)

    def stop(self):
        self.is_running = False
        self.is_game_active = False
        if self._current_file is not None or self._current_raw_file is not None:
            self._finalize_match_session()
