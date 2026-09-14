import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse

from services.engine.src.history import get_match_history
from services.engine.src.poller import TelemetryEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("rift-pulse.main")

engine = TelemetryEngine(model_path="services/engine/models/model.onnx", data_dir="data/matches", auto_poll_idle=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Check immediately if a game is in progress
    await engine.check_for_live_game()
    poll_task = asyncio.create_task(engine.poll_loop())
    logger.info("Telemetry and ONNX inference engine started (auto_poll_idle=%s).", engine.auto_poll_idle)
    yield
    engine.stop()
    poll_task.cancel()
    with suppress(asyncio.CancelledError):
        await poll_task
    logger.info("Engine stopped.")


app = FastAPI(title="Rift-Pulse Live Engine & Real-Time HUD API", version="1.3.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs("services/engine/static", exist_ok=True)
app.mount("/static", StaticFiles(directory="services/engine/static"), name="static")


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "game_active": engine.is_game_active,
        "game_status": engine.game_status,
        "session_id": engine.session_id,
        "ticks_processed": engine.tick_counter,
        "onnx_model_loaded": engine.session is not None,
        "active_sse_subscribers": len(engine.subscribers),
        "auto_poll_idle": engine.auto_poll_idle,
        "idle_poll_interval": engine.idle_poll_interval,
    }


@app.get("/api/v1/live/status")
async def get_live_status():
    return {
        "active": engine.is_game_active,
        "session_id": engine.session_id,
        "game_status": engine.game_status,
        "game_time": engine.latest_data.get("game_time_formatted") if engine.latest_data else None,
        "tick_id": engine.tick_counter,
        "auto_poll_idle": engine.auto_poll_idle,
        "idle_poll_interval": engine.idle_poll_interval,
    }


@app.post("/api/v1/live/auto-poll")
async def set_auto_poll_status(request: Request):
    """Enable, disable, or configure background idle polling at runtime."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    enabled = body.get("enabled")
    if enabled is None and "enabled" in request.query_params:
        enabled = request.query_params.get("enabled", "").lower() in ("true", "1", "yes")

    interval = body.get("interval")
    if interval is None and "interval" in request.query_params:
        try:
            interval = float(request.query_params.get("interval"))
        except ValueError:
            interval = None

    if enabled is not None or interval is not None:
        new_enabled = engine.auto_poll_idle if enabled is None else bool(enabled)
        engine.set_auto_poll(new_enabled, interval)

    return {
        "auto_poll_idle": engine.auto_poll_idle,
        "idle_poll_interval": engine.idle_poll_interval,
        "game_active": engine.is_game_active,
    }


@app.post("/api/v1/live/find")
async def find_live_game():
    result = await engine.check_for_live_game()
    return result


@app.get("/api/v1/telemetry/latest")
async def get_latest_telemetry():
    if engine.latest_data is None:
        return JSONResponse(status_code=204, content={"message": "Waiting for match to start"})
    return engine.latest_data


@app.get("/api/v1/telemetry/history")
async def get_telemetry_history():
    return {"session_id": engine.session_id, "history": engine.history, "count": len(engine.history)}


@app.get("/api/v1/matches")
async def list_recorded_matches():
    matches = get_match_history(data_dir="data/matches")
    return {"matches": matches, "total": len(matches)}


@app.get("/api/v1/matches/{session_id}")
async def get_match_details(session_id: str):
    matches = get_match_history(data_dir="data/matches")
    for m in matches:
        if m.get("session_id") == session_id:
            return m
    return JSONResponse(status_code=404, content={"message": "Match not found"})


@app.get("/api/v1/stream/live")
async def stream_live_telemetry(request: Request):
    queue = asyncio.Queue(maxsize=10)
    engine.subscribers.append(queue)

    async def event_generator():
        try:
            if engine.latest_data is not None:
                yield {
                    "event": "hud_telemetry_tick",
                    "id": str(engine.latest_data["tick_id"]),
                    "data": json.dumps(engine.latest_data),
                }

            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=12.0)
                    yield {"event": "hud_telemetry_tick", "id": str(payload["tick_id"]), "data": json.dumps(payload)}
                except asyncio.TimeoutError:
                    yield {"event": "ping", "data": "keep-alive"}
        finally:
            if queue in engine.subscribers:
                engine.subscribers.remove(queue)

    return EventSourceResponse(
        event_generator(), headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"}
    )


@app.get("/", response_class=HTMLResponse)
async def live_esports_broadcast_hud():
    """Official Esports Broadcast Overlay with ONNX Win Probability Inference."""
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>RIFT-PULSE // Official Esports Broadcast Overlay</title>
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Montserrat:ital,wght@0,400;0,600;0,700;0,800;0,900;1,700&display=swap');

            * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Montserrat', sans-serif; }
            body { 
                background: #06090e; 
                color: #ffffff; 
                padding: 16px 24px; 
                min-height: 100vh;
                background-image: radial-gradient(circle at 50% 0%, #121c2e 0%, #06090e 70%);
            }

            /* ==========================================================================
               TOP BROADCAST ROW & OBJECTIVE TIMERS (Wierna replika ekranu 2)
               ========================================================================== */
            .top-broadcast-row {
                max-width: 1320px;
                margin: 0 auto 20px auto;
                display: flex;
                gap: 12px;
                align-items: stretch;
            }

            .win-prob-panel {
                background: linear-gradient(180deg, rgba(14, 21, 33, 0.95) 0%, rgba(8, 12, 20, 0.95) 100%);
                border: 1px solid #1e293b;
                border-radius: 8px;
                padding: 6px 12px;
                display: flex;
                flex-direction: column;
                justify-content: space-between;
                box-shadow: 0 8px 24px rgba(0, 0, 0, 0.6);
                width: 260px;
                flex-shrink: 0;
            }

            .chart-mini-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 2px;
            }

            .chart-mini-title {
                font-size: 8.5px;
                font-weight: 800;
                color: #64748b;
                letter-spacing: 0.8px;
                text-transform: uppercase;
            }

            .chart-mini-curr {
                font-size: 11px;
                font-weight: 800;
            }

            .sparkline-wrapper {
                width: 100%;
                height: 42px;
                position: relative;
                cursor: crosshair;
            }

            .sparkline-svg {
                width: 100%;
                height: 100%;
                overflow: visible;
            }

            .chart-axis-labels {
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 8px;
                font-weight: 700;
                color: #475569;
                margin-top: 1px;
            }

            .sparkline-tooltip {
                position: absolute;
                top: -24px;
                left: 50%;
                transform: translateX(-50%);
                background: #090d16;
                border: 1px solid #38bdf8;
                border-radius: 4px;
                padding: 2px 6px;
                font-size: 9px;
                font-weight: 800;
                color: #f1f5f9;
                pointer-events: none;
                opacity: 0;
                transition: opacity 0.15s ease;
                white-space: nowrap;
                z-index: 20;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.6);
            }

            /* Sub-bar Objective Timers (Baron & Dragon in Top-Sub-Bar) */
            .sub-obj-timer {
                display: flex;
                align-items: center;
                gap: 6px;
                background: rgba(10, 15, 24, 0.75);
                border: 1px solid #1e293b;
                border-radius: 5px;
                padding: 2px 7px;
                height: 24px;
                transition: all 0.25s ease;
            }

            .sub-obj-timer.baron-sub {
                border-left: 3px solid #a855f7;
            }

            .sub-obj-timer.dragon-sub {
                border-left: 3px solid #00f2fe;
            }

            .sub-obj-timer.is-alive {
                border-color: #10b981;
                border-left-color: #10b981;
                background: rgba(16, 185, 129, 0.08);
                box-shadow: 0 0 8px rgba(16, 185, 129, 0.25);
            }

            .sub-obj-icon {
                width: 17px;
                height: 17px;
                border-radius: 3px;
                display: flex;
                align-items: center;
                justify-content: center;
                flex-shrink: 0;
            }

            .sub-obj-img {
                width: 13px;
                height: 13px;
                object-fit: contain;
            }

            .sub-obj-meta {
                display: flex;
                align-items: baseline;
                gap: 5px;
                white-space: nowrap;
            }

            .sub-obj-name {
                font-size: 8px;
                font-weight: 800;
                color: #94a3b8;
                letter-spacing: 0.5px;
                text-transform: uppercase;
            }

            .sub-obj-val {
                font-size: 11px;
                font-weight: 900;
                letter-spacing: 0.3px;
                color: #f1f5f9;
            }

            .sub-obj-timer.is-alive .sub-obj-val {
                color: #10b981;
            }

            .top-bar-container {
                flex: 1;
                margin: 0;
                background: linear-gradient(180deg, rgba(14, 21, 33, 0.95) 0%, rgba(8, 12, 20, 0.95) 100%);
                border: 1px solid #1e293b;
                border-radius: 8px;
                box-shadow: 0 8px 30px rgba(0, 0, 0, 0.7);
                overflow: hidden;
                display: flex;
                flex-direction: column;
            }

            .top-bar {
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 10px 24px;
                height: 56px;
                position: relative;
            }

            /* Blue Team (Left) */
            .team-side-blue {
                display: flex;
                align-items: center;
                gap: 12px;
                flex: 1;
            }
            .team-badge-blue {
                display: flex;
                align-items: center;
                gap: 8px;
                border-left: 4px solid #00f2fe;
                padding-left: 10px;
            }
            .team-meta {
                display: flex;
                flex-direction: column;
            }
            .team-name {
                font-size: 19px;
                font-weight: 900;
                letter-spacing: 0.5px;
            }
            .team-seed {
                font-size: 10px;
                font-weight: 700;
                color: #64748b;
                letter-spacing: 1px;
            }

            .tower-pill {
                display: flex;
                align-items: center;
                gap: 6px;
                background: #0f172a;
                border: 1px solid #1e293b;
                padding: 5px 10px;
                border-radius: 6px;
            }
            .tower-icon-img {
                width: 18px;
                height: 18px;
                object-fit: contain;
                filter: drop-shadow(0 0 2px rgba(255, 255, 255, 0.25));
            }
            .tower-val {
                font-size: 16px;
                font-weight: 900;
                color: #f1f5f9;
                min-width: 10px;
                text-align: center;
            }

            .gold-pill-blue {
                display: flex;
                align-items: center;
                gap: 8px;
                background: #0f172a;
                border: 1px solid #1e293b;
                padding: 5px 12px;
                border-radius: 6px;
            }
            .gold-display-wrap {
                display: flex;
                align-items: center;
                gap: 6px;
            }
            .gold-icon-img {
                width: 18px;
                height: 18px;
                object-fit: contain;
                filter: drop-shadow(0 0 4px rgba(245, 158, 11, 0.6));
            }
            .gold-val {
                font-size: 18px;
                font-weight: 900;
                letter-spacing: 0.5px;
                color: #fbbf24;
            }
            .gold-lead-badge {
                background: #00f2fe;
                color: #04101d;
                font-size: 11px;
                font-weight: 900;
                padding: 2px 6px;
                border-radius: 4px;
            }
            .gold-lead-badge.red-badge {
                background: #ef4444;
                color: #ffffff;
            }

            /* Center: Kill Score and Versus */
            .center-match-core {
                display: flex;
                align-items: center;
                gap: 16px;
                padding: 0 16px;
            }
            .team-score {
                font-size: 30px;
                font-weight: 900;
                letter-spacing: 1px;
                min-width: 36px;
                text-align: center;
            }
            .score-blue { color: #38bdf8; }
            .score-red { color: #f87171; }
            .versus-icon {
                font-size: 14px;
                color: #64748b;
            }

            /* Sub-bar pod statystykami (Smoki, Voidgruby, Zegar) */
            .top-sub-bar {
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 5px 24px 7px 24px;
                border-top: 1px solid rgba(30, 41, 59, 0.6);
                background: rgba(4, 7, 14, 0.5);
                min-height: 28px;
            }

            .team-sub-objectives {
                display: flex;
                align-items: center;
                gap: 12px;
                flex: 1;
            }

            .team-sub-objectives.red-sub {
                justify-content: flex-end;
            }

            .grubs-pill {
                display: flex;
                align-items: center;
                gap: 6px;
                background: rgba(15, 23, 42, 0.85);
                border: 1px solid #334155;
                padding: 2px 8px;
                border-radius: 4px;
                font-size: 11px;
                font-weight: 800;
                color: #cbd5e1;
            }

            .grub-icon-img {
                width: 20px;
                height: 20px;
                object-fit: contain;
                filter: drop-shadow(0 0 3px rgba(168, 85, 247, 0.6));
            }

            .grub-val {
                color: #c084fc;
                font-weight: 900;
                font-size: 12px;
            }

            .dragon-slots {
                display: flex;
                align-items: center;
                gap: 6px;
            }

            #red-dragon-slots {
                flex-direction: row-reverse;
            }

            .dragon-slot {
                width: 24px;
                height: 24px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                transition: all 0.3s ease;
                background: #090d16;
                overflow: hidden;
            }

            .dragon-slot.empty {
                border: 1px dashed #334155;
                background: rgba(15, 23, 42, 0.4);
            }

            .dragon-slot.filled {
                border: 1.5px solid #00f2fe;
                padding: 1px;
            }

            .dragon-slot-img {
                width: 100%;
                height: 100%;
                object-fit: contain;
                border-radius: 50%;
            }

            .sub-game-clock {
                display: flex;
                align-items: center;
                justify-content: center;
                min-width: 120px;
            }

            .clock-digits {
                font-size: 16px;
                font-weight: 900;
                color: #f1f5f9;
                letter-spacing: 1px;
            }

            .status-indicator {
                font-size: 9px;
                font-weight: 800;
                color: #10b981;
                letter-spacing: 1.5px;
                text-transform: uppercase;
                margin-left: 8px;
            }

            /* Red Team (Right) */
            .team-side-red {
                display: flex;
                align-items: center;
                justify-content: flex-end;
                gap: 12px;
                flex: 1;
            }
            .team-badge-red {
                display: flex;
                align-items: center;
                gap: 8px;
                border-right: 4px solid #ef4444;
                padding-right: 10px;
                text-align: right;
            }
            .gold-pill-red {
                display: flex;
                align-items: center;
                gap: 8px;
                background: #0f172a;
                border: 1px solid #1e293b;
                padding: 5px 12px;
                border-radius: 6px;
            }

            /* Integrated Win Probability Strip in Top Panel */
            .win-prob-strip {
                height: 6px;
                background: #0f172a;
                display: flex;
                position: relative;
                border-top: 1px solid #1e293b;
            }
            .win-prob-fill-blue {
                background: linear-gradient(90deg, #0284c7, #38bdf8);
                height: 100%;
                transition: width 0.5s cubic-bezier(0.4, 0, 0.2, 1);
            }
            .win-prob-fill-red {
                background: linear-gradient(90deg, #f87171, #dc2626);
                height: 100%;
                transition: width 0.5s cubic-bezier(0.4, 0, 0.2, 1);
            }

            /* ==========================================================================
               MAIN SCOREBOARD (5v5 Scoreboard)
               ========================================================================== */
            .scoreboard-container {
                max-width: 1320px;
                margin: 0 auto;
                background: rgba(10, 15, 26, 0.95);
                border: 1px solid #1e293b;
                border-radius: 8px;
                padding: 12px 16px;
                box-shadow: 0 10px 40px rgba(0, 0, 0, 0.8);
            }

            .scoreboard-row {
                display: flex;
                align-items: center;
                justify-content: space-between;
                background: linear-gradient(90deg, rgba(14, 116, 144, 0.15) 0%, rgba(15, 23, 42, 0.3) 35%, rgba(15, 23, 42, 0.3) 65%, rgba(194, 65, 12, 0.15) 100%);
                border: 1px solid #1e293b;
                margin-bottom: 6px;
                height: 60px;
                padding: 0 8px;
                border-radius: 4px;
                transition: transform 0.15s ease;
            }
            .scoreboard-row:hover {
                border-color: #334155;
            }

            /* Blue Player Side */
            .lane-player-blue {
                display: flex;
                align-items: center;
                gap: 8px;
                flex: 1;
            }
            /* Red Player Side */
            .lane-player-red {
                display: flex;
                align-items: center;
                justify-content: flex-end;
                gap: 8px;
                flex: 1;
            }

            /* Outer Role Badge */
            .role-badge {
                width: 32px;
                height: 32px;
                display: flex;
                align-items: center;
                justify-content: center;
                background: rgba(15, 23, 42, 0.75);
                border: 1px solid #334155;
                border-radius: 6px;
                flex-shrink: 0;
            }
            .role-badge.blue-role {
                border-color: rgba(2, 132, 199, 0.4);
            }
            .role-badge.red-role {
                border-color: rgba(234, 88, 12, 0.4);
            }
            .role-icon-img {
                width: 20px;
                height: 20px;
                object-fit: contain;
                filter: drop-shadow(0 0 2px rgba(255, 255, 255, 0.3));
            }

            /* Items (6 slots) */
            .items-grid {
                display: flex;
                gap: 3px;
            }
            .item-slot {
                width: 32px;
                height: 32px;
                background: #090d16;
                border: 1px solid #1e293b;
                border-radius: 4px;
                display: flex;
                align-items: center;
                justify-content: center;
                position: relative;
                overflow: hidden;
            }
            .item-img {
                width: 100%;
                height: 100%;
                object-fit: cover;
            }
            .trinket-slot {
                border-color: #0284c7;
            }

            /* Summoner Spells & Keystone */
            .spells-runes-group {
                display: flex;
                align-items: center;
                gap: 4px;
                flex-shrink: 0;
            }
            .keystone-box {
                width: 32px;
                height: 32px;
                border-radius: 50%;
                background: #090d16;
                border: 1.5px solid #475569;
                display: flex;
                align-items: center;
                justify-content: center;
                overflow: hidden;
                box-shadow: 0 0 6px rgba(0, 0, 0, 0.6);
                transition: border-color 0.2s;
            }
            .keystone-box:hover {
                border-color: #f59e0b;
            }
            .keystone-img {
                width: 26px;
                height: 26px;
                object-fit: cover;
                border-radius: 50%;
            }
            .spells-col {
                display: flex;
                flex-direction: column;
                gap: 2px;
            }
            .spell-box {
                width: 15px;
                height: 15px;
                background: #1e293b;
                border-radius: 2px;
                overflow: hidden;
            }
            .spell-img {
                width: 100%;
                height: 100%;
                object-fit: cover;
            }

            /* Champion Avatar & Level Badge */
            .champ-box {
                position: relative;
                width: 44px;
                height: 44px;
                border-radius: 50%;
                border: 2px solid #0284c7;
                overflow: visible; /* Fixed: allows level badge to float without clipping */
                background: #0f172a;
                flex-shrink: 0;
            }
            .champ-box.red-border {
                border-color: #ea580c;
            }
            .champ-img {
                width: 100%;
                height: 100%;
                border-radius: 50%;
                object-fit: cover;
                display: block;
            }
            .champ-level-badge {
                position: absolute;
                bottom: -2px;
                right: -2px;
                background: #0284c7;
                color: #fff;
                font-size: 10px;
                font-weight: 900;
                width: 16px;
                height: 16px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                border: 1px solid #082f49;
                z-index: 2;
                box-shadow: 0 2px 4px rgba(0,0,0,0.6);
            }
            .champ-level-badge.red-bg {
                background: #ea580c;
                border-color: #431407;
            }

            /* Player Info (Name & KDA) */
            .player-details {
                display: flex;
                flex-direction: column;
                justify-content: center;
                width: 95px;
                min-width: 95px;
                max-width: 95px;
                flex-shrink: 0;
                overflow: hidden;
            }
            .player-details.text-right {
                text-align: right;
                align-items: flex-end;
            }
            .player-details:not(.text-right) {
                text-align: left;
                align-items: flex-start;
            }
            .player-name {
                width: 100%;
                font-size: 13px;
                font-weight: 700;
                color: #f8fafc;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                line-height: 1.25;
            }
            .player-kda {
                font-size: 11px;
                font-weight: 700;
                color: #94a3b8;
                line-height: 1.2;
            }

            /* Player Stats (Gold Pill & CS) */
            .lane-player-blue .player-stats-group {
                margin-left: auto;
            }
            .lane-player-red .player-stats-group {
                margin-right: auto;
            }
            .player-stats-group {
                display: flex;
                align-items: center;
                gap: 12px;
                flex-shrink: 0;
            }
            .stat-col {
                position: relative;
                display: flex;
                flex-direction: column;
                align-items: center;
                justify-content: center;
            }
            .stat-header-icon {
                position: absolute;
                top: -14px;
                left: 50%;
                transform: translateX(-50%);
                display: flex;
                align-items: center;
                justify-content: center;
                pointer-events: none;
            }
            .stat-header-icon .col-icon {
                width: 14px;
                height: 14px;
                object-fit: contain;
                opacity: 0.9;
            }
            .stat-header-icon .col-icon-minion,
            .stat-header-icon .col-icon-coin {
                width: 14px;
                height: 14px;
                opacity: 0.9;
            }
            .gold-stat-pill {
                font-size: 14px;
                font-weight: 900;
                color: #fbbf24;
                min-width: 36px;
                text-align: center;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            .cs-stat-pill {
                font-size: 14px;
                font-weight: 900;
                color: #e2e8f0; /* Silver-white esports CS */
                min-width: 32px;
                text-align: center;
            }

            /* Center Lane Gold Diff Indicator */
            .lane-gold-center {
                display: flex;
                align-items: center;
                justify-content: center;
                padding: 0;
                margin: 0 12px;
                flex-shrink: 0;
            }
            .diff-pill {
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background: #090d16;
                padding: 3px 8px;
                border-radius: 4px;
                font-size: 12px;
                font-weight: 800;
                letter-spacing: 0.3px;
                border: 1px solid #1e293b;
                white-space: nowrap;
                min-width: 48px;
                text-align: center;
                box-sizing: border-box;
            }
            .diff-pill.blue-lead {
                color: #38bdf8;
                border-left: 3px solid #38bdf8;
                background: rgba(14, 116, 144, 0.12);
            }
            .diff-pill.red-lead {
                color: #ef4444;
                border-right: 3px solid #ef4444;
                background: rgba(239, 68, 68, 0.12);
            }
            .diff-pill.equal {
                color: #64748b;
                border-color: #1e293b;
            }

            /* Footer Utility Strip */
            .footer-strip {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-top: 16px;
                font-size: 11px;
                color: #64748b;
                padding-top: 12px;
                border-top: 1px solid #1e293b;
            }
            /* ==========================================================================
               TOP NAVIGATION BAR & CONTROLS
               ========================================================================== */
            .nav-header {
                max-width: 1320px;
                margin: 0 auto 16px auto;
                display: flex;
                justify-content: space-between;
                align-items: center;
                background: rgba(10, 15, 26, 0.9);
                backdrop-filter: blur(12px);
                border: 1px solid rgba(255, 255, 255, 0.08);
                border-radius: 10px;
                padding: 10px 18px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.5);
            }
            .nav-brand {
                display: flex;
                align-items: center;
                gap: 12px;
            }
            .brand-heart-wrap {
                display: flex;
                align-items: center;
                justify-content: center;
            }
            .heart-pulse-icon {
                width: 22px;
                height: 22px;
                animation: heartbeat 1.3s cubic-bezier(0.215, 0.61, 0.355, 1) infinite;
                transform-origin: center;
                display: block;
            }
            @keyframes heartbeat {
                0% {
                    transform: scale(1);
                    filter: drop-shadow(0 0 2px rgba(244, 63, 94, 0.4));
                }
                14% {
                    transform: scale(1.35);
                    filter: drop-shadow(0 0 12px rgba(244, 63, 94, 1));
                }
                28% {
                    transform: scale(1);
                    filter: drop-shadow(0 0 3px rgba(244, 63, 94, 0.4));
                }
                42% {
                    transform: scale(1.22);
                    filter: drop-shadow(0 0 9px rgba(244, 63, 94, 0.9));
                }
                70% {
                    transform: scale(1);
                    filter: drop-shadow(0 0 2px rgba(244, 63, 94, 0.3));
                }
                100% {
                    transform: scale(1);
                    filter: drop-shadow(0 0 2px rgba(244, 63, 94, 0.3));
                }
            }
            .live-dot {
                font-size: 13px;
                margin-right: 4px;
                display: inline-block;
                vertical-align: middle;
            }
            .nav-title {
                font-size: 15px;
                font-weight: 900;
                letter-spacing: 1.5px;
                background: linear-gradient(90deg, #00f2fe, #38bdf8, #818cf8);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
            }
            .nav-actions {
                display: flex;
                align-items: center;
                gap: 10px;
            }
            .btn-nav {
                background: #0f172a;
                color: #cbd5e1;
                border: 1px solid #334155;
                padding: 8px 18px;
                border-radius: 6px;
                font-size: 12px;
                font-weight: 700;
                cursor: pointer;
                display: inline-flex;
                align-items: center;
                gap: 6px;
                transition: all 0.2s ease;
            }
            .btn-nav:hover {
                background: #1e293b;
                color: #ffffff;
                border-color: #64748b;
            }
            .btn-nav.active {
                background: #0284c7;
                color: #ffffff;
                border-color: #38bdf8;
                box-shadow: 0 0 12px rgba(56, 189, 248, 0.4);
            }
            .btn-nav:disabled {
                opacity: 0.6;
                cursor: not-allowed;
            }

            /* ==========================================================================
               MATCH HISTORY VIEW STYLES
               ========================================================================== */
            .history-container {
                max-width: 1320px;
                margin: 0 auto;
            }
            .history-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
                padding: 0 4px;
            }
            .history-title {
                font-size: 20px;
                font-weight: 800;
                letter-spacing: 0.5px;
            }
            .history-grid {
                display: flex;
                flex-direction: column;
                gap: 14px;
            }
            .match-card {
                background: linear-gradient(180deg, rgba(15, 23, 42, 0.95) 0%, rgba(9, 13, 22, 0.95) 100%);
                border: 1px solid #1e293b;
                border-radius: 10px;
                padding: 16px 20px;
                display: grid;
                grid-template-columns: 180px 180px max-content 210px 1fr;
                gap: 16px;
                align-items: center;
                box-shadow: 0 6px 20px rgba(0, 0, 0, 0.4);
                position: relative;
                overflow: hidden;
                transition: border-color 0.2s, transform 0.2s;
            }
            .match-card:hover {
                border-color: #334155;
                transform: translateY(-2px);
            }
            .match-card.winner-blue {
                border-left: 5px solid #00f2fe;
            }
            .match-card.winner-red {
                border-left: 5px solid #ef4444;
            }
            .match-meta-col {
                display: flex;
                flex-direction: column;
                gap: 4px;
            }
            .match-result-badge {
                font-size: 14px;
                font-weight: 900;
                letter-spacing: 0.5px;
            }
            .match-result-badge.blue {
                color: #38bdf8;
            }
            .match-result-badge.red {
                color: #f87171;
            }
            .match-mode {
                font-size: 12px;
                color: #94a3b8;
                font-weight: 600;
            }
            .match-time-info {
                font-size: 11px;
                color: #64748b;
                display: flex;
                gap: 8px;
            }
            .match-scores-col {
                display: flex;
                flex-direction: column;
                gap: 6px;
            }
            .scores-main {
                display: flex;
                align-items: center;
                gap: 8px;
                font-size: 18px;
                font-weight: 900;
            }
            .scores-gold {
                display: flex;
                align-items: center;
                gap: 8px;
                font-size: 12px;
                font-weight: 700;
                color: #cbd5e1;
            }
            .scores-objectives {
                display: flex;
                align-items: center;
                gap: 6px;
                font-size: 11px;
                color: #94a3b8;
            }
            .match-teams-col {
                display: flex;
                flex-direction: column;
                gap: 8px;
            }
            .team-roster {
                display: flex;
                align-items: center;
                gap: 8px;
                flex-wrap: nowrap;
            }
            .team-roster-label {
                font-size: 11px;
                font-weight: 800;
                width: 42px;
                flex-shrink: 0;
            }
            .team-roster-label.blue { color: #38bdf8; }
            .team-roster-label.red { color: #f87171; }
            .roster-champs {
                display: flex;
                gap: 6px;
                flex-wrap: nowrap;
                align-items: center;
            }
            .roster-champ-unit {
                position: relative;
                width: 32px;
                height: 32px;
                border-radius: 6px;
                overflow: hidden;
                border: 1px solid #334155;
                background: #0f172a;
                flex-shrink: 0;
            }
            .roster-champ-unit img {
                width: 100%;
                height: 100%;
                object-fit: cover;
            }
            .match-chart-col {
                display: flex;
                flex-direction: column;
                justify-content: center;
                background: rgba(10, 15, 26, 0.75);
                border: 1px solid #1e293b;
                border-radius: 6px;
                padding: 6px 10px;
                height: 70px;
                box-sizing: border-box;
            }
            .match-chart-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 2px;
            }
            .match-chart-title {
                font-size: 8px;
                font-weight: 800;
                color: #64748b;
                letter-spacing: 0.5px;
                text-transform: uppercase;
            }
            .match-chart-val {
                font-size: 9px;
                font-weight: 800;
            }
            .match-sparkline-wrap {
                width: 100%;
                height: 30px;
                position: relative;
            }
            .match-sparkline-svg {
                width: 100%;
                height: 100%;
                overflow: visible;
            }
            .match-chart-axis {
                display: flex;
                justify-content: space-between;
                align-items: center;
                font-size: 7.5px;
                font-weight: 700;
                color: #475569;
                margin-top: 1px;
            }
            .match-action-col {
                display: flex;
                flex-direction: row;
                justify-content: flex-end;
                align-items: center;
                width: 100%;
            }
            .btn-details {
                background: #1e293b;
                color: #38bdf8;
                border: 1px solid #38bdf850;
                padding: 8px 18px;
                border-radius: 6px;
                font-size: 12px;
                font-weight: 700;
                cursor: pointer;
                transition: all 0.2s;
                white-space: nowrap;
            }
            .btn-details:hover {
                background: #0284c7;
                color: #ffffff;
            }

            /* MODAL STYLES */
            .modal-backdrop {
                position: fixed;
                top: 0;
                left: 0;
                width: 100vw;
                height: 100vh;
                background: rgba(0, 0, 0, 0.85);
                backdrop-filter: blur(8px);
                display: flex;
                align-items: center;
                justify-content: center;
                z-index: 1000;
                padding: 16px;
            }
            .modal-card {
                background: #0b111e;
                border: 1px solid #334155;
                border-radius: 10px;
                max-width: 1320px;
                width: 96vw;
                max-height: 85vh;
                overflow-y: auto;
                padding: 16px 22px;
                box-shadow: 0 16px 50px rgba(0, 0, 0, 0.85);
            }
            .modal-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 12px;
                border-bottom: 1px solid #1e293b;
                padding-bottom: 10px;
            }
            .modal-close {
                background: transparent;
                border: none;
                color: #94a3b8;
                font-size: 20px;
                cursor: pointer;
                line-height: 1;
                padding: 4px;
            }
            .modal-close:hover { color: #fff; }

            /* Modal Scoreboard overrides - spacious and clear */
            .modal-card .scoreboard-container {
                padding: 10px 14px;
                border-radius: 8px;
                box-shadow: none;
                width: 100%;
                overflow-x: auto;
            }
            .modal-card .scoreboard-row {
                height: 54px;
                margin-bottom: 5px;
                padding: 0 10px;
                gap: 8px;
            }
            .modal-card .lane-player-blue,
            .modal-card .lane-player-red {
                gap: 8px;
                flex: 1;
            }
            .modal-card .role-badge {
                width: 30px;
                height: 30px;
                border-radius: 6px;
                flex-shrink: 0;
            }
            .modal-card .role-icon-img {
                width: 18px;
                height: 18px;
            }
            .modal-card .items-grid {
                gap: 3px;
                flex-shrink: 0;
            }
            .modal-card .item-slot {
                width: 28px;
                height: 28px;
                border-radius: 4px;
                flex-shrink: 0;
            }
            .modal-card .spells-runes-group {
                gap: 4px;
                flex-shrink: 0;
            }
            .modal-card .keystone-box {
                width: 28px;
                height: 28px;
                flex-shrink: 0;
            }
            .modal-card .keystone-img {
                width: 22px;
                height: 22px;
            }
            .modal-card .spells-col {
                gap: 2px;
                flex-shrink: 0;
            }
            .modal-card .spell-box {
                width: 13px;
                height: 13px;
                flex-shrink: 0;
            }
            .modal-card .champ-box {
                width: 38px;
                height: 38px;
                border-width: 2px;
                flex-shrink: 0;
            }
            .modal-card .champ-level-badge {
                width: 15px;
                height: 15px;
                font-size: 9px;
                bottom: -2px;
                right: -2px;
            }
            .modal-card .player-details {
                width: 90px;
                min-width: 90px;
                max-width: 90px;
                flex-shrink: 0;
            }
            .modal-card .player-name {
                font-size: 12px;
                line-height: 1.2;
            }
            .modal-card .player-kda {
                font-size: 10px;
                line-height: 1.2;
            }
            .modal-card .player-stats-group {
                gap: 6px;
                flex-shrink: 0;
            }
            .modal-card .stat-header-icon {
                top: -14px;
            }
            .modal-card .col-icon {
                width: 12px;
                height: 12px;
            }
            .modal-card .cs-stat-pill,
            .modal-card .gold-stat-pill {
                font-size: 11px;
            }
            .modal-card .diff-pill {
                font-size: 11px;
                padding: 3px 8px;
                min-width: 42px;
            }

            /* NO LIVE GAME STATE VIEW */
            .no-game-container {
                max-width: 820px;
                margin: 40px auto;
                background: linear-gradient(180deg, rgba(15, 23, 42, 0.95) 0%, rgba(9, 13, 22, 0.95) 100%);
                border: 1px solid #1e293b;
                border-radius: 12px;
                padding: 48px 32px;
                text-align: center;
                box-shadow: 0 12px 40px rgba(0, 0, 0, 0.6);
            }
            .no-game-icon-pulse {
                width: 72px;
                height: 72px;
                border-radius: 50%;
                background: rgba(56, 189, 248, 0.08);
                border: 1px solid rgba(56, 189, 248, 0.25);
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0 auto 20px auto;
                box-shadow: 0 0 24px rgba(56, 189, 248, 0.15);
                animation: pulse-ring 2.5s infinite;
            }
            @keyframes pulse-ring {
                0% { box-shadow: 0 0 0 0 rgba(56, 189, 248, 0.4); }
                70% { box-shadow: 0 0 0 16px rgba(56, 189, 248, 0); }
                100% { box-shadow: 0 0 0 0 rgba(56, 189, 248, 0); }
            }
            .no-game-title {
                font-size: 22px;
                font-weight: 800;
                color: #f8fafc;
                margin-bottom: 10px;
            }
            .no-game-desc {
                font-size: 13px;
                color: #94a3b8;
                line-height: 1.6;
                max-width: 520px;
                margin: 0 auto 24px auto;
            }
            .no-game-actions {
                display: flex;
                justify-content: center;
                gap: 12px;
                margin-bottom: 28px;
            }
            .no-game-status-box {
                display: inline-flex;
                flex-direction: column;
                gap: 6px;
                background: rgba(10, 15, 26, 0.8);
                border: 1px solid #1e293b;
                border-radius: 8px;
                padding: 12px 20px;
                font-size: 11px;
                color: #64748b;
                text-align: left;
            }
            .no-game-status-line {
                display: flex;
                align-items: center;
                gap: 8px;
            }

            /* TOAST NOTIFICATION */
            .toast {
                position: fixed;
                bottom: 24px;
                right: 24px;
                background: #0f172a;
                border: 1px solid #38bdf8;
                color: #fff;
                padding: 12px 20px;
                border-radius: 8px;
                font-size: 13px;
                font-weight: 700;
                box-shadow: 0 10px 30px rgba(0, 0, 0, 0.8);
                display: none;
                z-index: 2000;
                animation: toast-in 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            }
            @keyframes toast-in {
                from { transform: translateY(20px); opacity: 0; }
                to { transform: translateY(0); opacity: 1; }
            }

            /* SPINNER */
            .spinner {
                display: inline-block;
                width: 14px;
                height: 14px;
                border: 2px solid rgba(255,255,255,0.3);
                border-radius: 50%;
                border-top-color: #fff;
                animation: spin 0.8s linear infinite;
            }
            @keyframes spin {
                to { transform: rotate(360deg); }
            }
        </style>
    </head>
    <body>

        <!-- ==========================================================================
             MAIN NAVIGATION BAR & CONTROLS
             ========================================================================== -->
        <div class="nav-header">
            <div class="nav-brand">
                <div class="brand-heart-wrap">
                    <svg class="heart-pulse-icon" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                        <path d="M12 21.35l-1.45-1.32C5.4 15.36 2 12.28 2 8.5 2 5.42 4.42 3 7.5 3c1.74 0 3.41.81 4.5 2.09C13.09 3.81 14.76 3 16.5 3 19.58 3 22 5.42 22 8.5c0 3.78-3.4 6.86-8.55 11.54L12 21.35z" fill="url(#heart-grad)"/>
                        <defs>
                            <linearGradient id="heart-grad" x1="2" y1="3" x2="22" y2="21.35" gradientUnits="userSpaceOnUse">
                                <stop stop-color="#ff2e63"/>
                                <stop offset="1" stop-color="#e11d48"/>
                            </linearGradient>
                        </defs>
                    </svg>
                </div>
                <div class="nav-title">RIFT-PULSE</div>
            </div>
            <div class="nav-actions">
                <button id="btn-find-live" class="btn-nav" onclick="findLiveGame()">
                    <span id="find-spinner-wrap"></span><span id="find-text">Find Live Game</span>
                </button>
                <button id="tab-history" class="btn-nav active" onclick="switchView('history')">
                    Match History
                </button>
            </div>
        </div>

        <!-- ==========================================================================
             LIVE BROADCAST OVERLAY
             ========================================================================== -->
        <div id="view-live" style="display: none;">
            
            <div id="live-active-content" style="display: none;">
                <div class="top-broadcast-row">
                
                <!-- DEDICATED WIN PROBABILITY TIMELINE WIDGET -->
                <div class="win-prob-panel">
                    <div class="chart-mini-header">
                        <div class="chart-mini-title">WIN PROB / MIN</div>
                        <div class="chart-mini-curr" id="chart-curr-prob"><span style="color: #94a3b8;">50% Even</span></div>
                    </div>
                    <div class="sparkline-wrapper" id="sparkline-container" title="Win Probability timeline per minute">
                        <svg id="win-prob-svg" viewBox="0 0 250 42" preserveAspectRatio="none" class="sparkline-svg">
                            <defs>
                                <linearGradient id="blueAreaGrad" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stop-color="#38bdf8" stop-opacity="0.4"/>
                                    <stop offset="100%" stop-color="#38bdf8" stop-opacity="0.0"/>
                                </linearGradient>
                                <linearGradient id="redAreaGrad" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stop-color="#f87171" stop-opacity="0.4"/>
                                    <stop offset="100%" stop-color="#f87171" stop-opacity="0.0"/>
                                </linearGradient>
                            </defs>
                            <!-- 50% Baseline on X-Axis with quarter ticks -->
                            <line id="sparkline-baseline" x1="0" y1="38" x2="250" y2="38" stroke="#334155" stroke-width="1.2"/>
                            <line x1="62.5" y1="38" x2="62.5" y2="41" stroke="#334155" stroke-width="1"/>
                            <line x1="125" y1="38" x2="125" y2="41" stroke="#334155" stroke-width="1"/>
                            <line x1="187.5" y1="38" x2="187.5" y2="41" stroke="#334155" stroke-width="1"/>
                            <!-- Segmented Area fills -->
                            <path id="sparkline-area-blue" d="" fill="url(#blueAreaGrad)"/>
                            <path id="sparkline-area-red" d="" fill="url(#redAreaGrad)"/>
                            <!-- Segmented Path lines -->
                            <path id="sparkline-line-blue" d="" fill="none" stroke="#38bdf8" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
                            <path id="sparkline-line-red" d="" fill="none" stroke="#f87171" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>
                            <!-- Current dot -->
                            <circle id="sparkline-dot" cx="-10" cy="-10" r="3.5" fill="#38bdf8" stroke="#ffffff" stroke-width="1.5"/>
                        </svg>
                        <div id="sparkline-tooltip" class="sparkline-tooltip"></div>
                    </div>
                    <div class="chart-axis-labels">
                        <span>0m</span>
                        <span id="chart-axis-q1"></span>
                        <span id="chart-axis-mid"></span>
                        <span id="chart-axis-q3"></span>
                        <span id="chart-axis-end">--m</span>
                    </div>
                </div>

                <!-- MAIN BROADCAST PANEL WITH TEAMS AND SCORE -->
                <div class="top-bar-container">
                    <!-- Top bar with team stats -->
                    <div class="top-bar">
                        
                        <!-- BLUE TEAM (Left) -->
                        <div class="team-side-blue">
                            <div class="team-badge-blue">
                                <div class="team-meta">
                                    <div class="team-name" id="blue-team-name">BLUE TEAM</div>
                                    <div class="team-seed">WIN PROB: <span id="win-prob-blue-val" style="color: #38bdf8;">--%</span></div>
                                </div>
                            </div>

                            <div class="tower-pill" title="Blue Towers Destroyed">
                                <img src="/static/icons/tower-100.png" class="tower-icon-img" alt="Turret">
                                <span class="tower-val" id="blue-towers">0</span>
                            </div>
                            
                            <div class="gold-pill-blue" title="Blue Team Gold">
                                <div class="gold-display-wrap">
                                    <img src="/static/icons/gold_coin.png" class="gold-icon-img" alt="Gold" title="Total Blue Gold">
                                    <span class="gold-val" id="blue-gold">0.0K</span>
                                </div>
                                <div class="gold-lead-badge" id="gold-lead-badge" style="display: none;">+0.0K</div>
                            </div>
                        </div>

                        <!-- CENTER: KILL SCORE -->
                        <div class="center-match-core">
                            <div class="team-score score-blue" id="blue-kills">0</div>
                            <div class="versus-icon">⚔</div>
                            <div class="team-score score-red" id="red-kills">0</div>
                        </div>

                        <!-- RED TEAM (Right) -->
                        <div class="team-side-red">
                            <div class="gold-pill-red" title="Red Team Gold">
                                <div class="gold-lead-badge red-badge" id="gold-lead-badge-red" style="display: none;">+0.0K</div>
                                <div class="gold-display-wrap">
                                    <span class="gold-val" id="red-gold">0.0K</span>
                                    <img src="/static/icons/gold_coin.png" class="gold-icon-img" alt="Gold" title="Total Red Gold">
                                </div>
                            </div>

                            <div class="tower-pill" title="Red Towers Destroyed">
                                <span class="tower-val" id="red-towers">0</span>
                                <img src="/static/icons/tower-200.png" class="tower-icon-img" alt="Turret">
                            </div>

                            <div class="team-badge-red">
                                <div class="team-meta">
                                    <div class="team-name" id="red-team-name">RED TEAM</div>
                                    <div class="team-seed">WIN PROB: <span id="win-prob-red-val" style="color: #f87171;">--%</span></div>
                                </div>
                            </div>
                        </div>

                    </div>

                    <!-- Bottom sub-bar: Objectives (Dragons, Voidgrubs, Baron & Dragon Timers) and Match Clock -->
                    <div class="top-sub-bar">
                        <!-- Blue objectives + Baron timer -->
                        <div class="team-sub-objectives blue-sub">
                            <div class="grubs-pill" title="Voidgrubs">
                                <img src="/static/icons/voidgrub_icon.png" class="grub-icon-img" alt="Grubs">
                                <span id="blue-voidgrubs" class="grub-val">0</span>
                            </div>
                            <div class="dragon-slots" id="blue-dragon-slots">
                                <div class="dragon-slot empty"></div>
                                <div class="dragon-slot empty"></div>
                                <div class="dragon-slot empty"></div>
                                <div class="dragon-slot empty"></div>
                            </div>
                            <!-- Baron / Herald Timer Chip -->
                            <div class="sub-obj-timer baron-sub" id="baron-timer-card" title="Baron / Herald Timer">
                                <div class="sub-obj-icon void-icon-wrap" id="baron-icon-wrap">
                                    <img id="baron-icon-img" src="/static/icons/baron.png" class="sub-obj-img" alt="Baron / Herald">
                                </div>
                                <div class="sub-obj-meta">
                                    <span class="sub-obj-name" id="baron-name">BARON</span>
                                    <span class="sub-obj-val" id="baron-time">ALIVE</span>
                                </div>
                            </div>
                        </div>

                        <!-- Center: Match clock -->
                        <div class="sub-game-clock">
                            <span class="clock-digits" id="game-time">00:00</span>
                        </div>

                        <!-- Dragon Timer + Red objectives -->
                        <div class="team-sub-objectives red-sub">
                            <!-- Dragon Timer Chip -->
                            <div class="sub-obj-timer dragon-sub" id="dragon-timer-card" title="Dragon Timer">
                                <div class="sub-obj-icon dragon-icon-wrap" id="dragon-icon-wrap">
                                    <img id="dragon-icon-img" src="/static/icons/dragon_default.png" class="sub-obj-img" alt="Dragon">
                                </div>
                                <div class="sub-obj-meta">
                                    <span class="sub-obj-name" id="dragon-name">DRAGON</span>
                                    <span class="sub-obj-val" id="dragon-time">--:--</span>
                                </div>
                            </div>
                            <div class="dragon-slots" id="red-dragon-slots">
                                <div class="dragon-slot empty"></div>
                                <div class="dragon-slot empty"></div>
                                <div class="dragon-slot empty"></div>
                                <div class="dragon-slot empty"></div>
                            </div>
                            <div class="grubs-pill" title="Voidgrubs">
                                <img src="/static/icons/voidgrub_icon.png" class="grub-icon-img" alt="Grubs">
                                <span id="red-voidgrubs" class="grub-val">0</span>
                            </div>
                        </div>
                    </div>

                    <!-- Integrated Win Probability Strip (ONNX Runtime) -->
                    <div class="win-prob-strip">
                        <div id="prob-bar-blue" class="win-prob-fill-blue" style="width: 50%;"></div>
                        <div id="prob-bar-red" class="win-prob-fill-red" style="width: 50%;"></div>
                    </div>

                </div>

            </div>

            <!-- MAIN 5v5 SCOREBOARD -->
            <div class="scoreboard-container">
                <div id="matchup-rows">
                    <!-- 5 matchup rows generated dynamically by JavaScript -->
                    <div style="text-align: center; padding: 40px; color: #64748b;">
                        Loading real-time match telemetry...
                    </div>
                </div>

                <div class="footer-strip">
                    <div>
                        <span>ONNX Inference Latency: <strong id="onnx-lat" style="color: #10b981;">0.14 ms</strong></span>
                        &bull; <span id="session-info">Session: --</span>
                    </div>
                </div>
            </div>
            </div> <!-- End #live-active-content -->

            <!-- NO ACTIVE GAME SCREEN -->
            <div id="live-no-game" style="display: none;">
                <div class="no-game-container">
                    <div class="no-game-icon-pulse">
                        <svg width="34" height="34" viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <circle cx="12" cy="12" r="10"></circle>
                            <line x1="2" y1="12" x2="22" y2="12"></line>
                            <path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"></path>
                        </svg>
                    </div>
                    <div class="no-game-title">No live game active</div>
                    <div id="no-game-message" class="no-game-desc">
                        Background auto-detection is active. Launch League of Legends and the match will automatically begin recording from second 0:00 without clicking anything.
                    </div>
                    <div class="no-game-actions">
                        <button class="btn-nav active" onclick="findLiveGame()">
                            <span>Check Now (Force)</span>
                        </button>
                    </div>
                    <div class="no-game-status-box">
                        <div class="no-game-status-line">
                            <span>Background Detection: <strong style="color: #10b981;">AUTO-POLL ACTIVE (checks every 2.5s)</strong></span>
                        </div>
                        <div class="no-game-status-line">
                            <span>Live Client Data API (127.0.0.1:2999): <strong id="live-api-status-text" style="color: #cbd5e1;">Listening for game launch...</strong></span>
                        </div>
                        <div class="no-game-status-line">
                            <span>Supported modes: <strong>Summoner's Rift, ARAM, Practice Tool</strong></span>
                        </div>
                    </div>
                </div>
            </div>

        </div> <!-- End #view-live -->

        <!-- ==========================================================================
             MATCH HISTORY VIEW
             ========================================================================== -->
        <div id="view-history" style="display: block;">
            <div class="history-container">
                <div class="history-header">
                    <div>
                        <div class="history-title">Match History</div>
                        <div style="font-size: 12px; color: #94a3b8; margin-top: 4px;">Recorded telemetry sessions and real-time AI win predictions</div>
                    </div>
                </div>
                <div id="history-list" class="history-grid">
                    <div style="text-align: center; padding: 40px; color: #64748b;">Loading match history...</div>
                </div>
            </div>
        </div>

        <!-- MATCH DETAILS MODAL -->
        <div id="match-modal" class="modal-backdrop" style="display: none;" onclick="closeModal(event)">
            <div class="modal-card" onclick="event.stopPropagation()">
                <div class="modal-header">
                    <div>
                        <h2 id="modal-title" style="font-size: 15px; font-weight: 800; margin: 0;">Match Details</h2>
                        <div id="modal-meta" style="font-size: 11px; color: #94a3b8; margin-top: 2px;"></div>
                    </div>
                    <button class="modal-close" onclick="closeModalDirect()">✕</button>
                </div>
                <div id="modal-content"></div>
            </div>
        </div>

        <!-- TOAST NOTIFICATIONS -->
        <div id="toast" class="toast"></div>

        <script>
            const D_DRAGON = "https://ddragon.leagueoflegends.com/cdn/14.20.1/img";

            // Summoner spells mapping
            const SPELL_MAP = {
                "Flash": "SummonerFlash.png",
                "Unleashed Teleport": "SummonerTeleport.png",
                "Teleport": "SummonerTeleport.png",
                "Ignite": "SummonerDot.png",
                "Smite": "SummonerSmite.png",
                "Heal": "SummonerHeal.png",
                "Cleanse": "SummonerBoost.png",
                "Ghost": "SummonerHaste.png",
                "Barrier": "SummonerBarrier.png",
                "Exhaust": "SummonerExhaust.png"
            };

            function getSpellIcon(name) {
                const file = SPELL_MAP[name] || "SummonerFlash.png";
                return `${D_DRAGON}/spell/${file}`;
            }

            function getItemIcon(itemId) {
                if (!itemId || itemId === 0) return null;
                return `${D_DRAGON}/item/${itemId}.png`;
            }

            // Non-standard champion names mapping to DataDragon
            const CHAMP_NAME_FIXES = {
                "Renata Glasc": "Renata",
                "Kha'Zix": "Khazix",
                "Cho'Gath": "Chogath",
                "Kai'Sa": "Kaisa",
                "LeBlanc": "Leblanc",
                "Nunu & Willump": "Nunu",
                "Nunu": "Nunu",
                "Wukong": "MonkeyKing",
                "Bel'Veth": "Belveth",
                "Vel'Koz": "Velkoz",
                "Kog'Maw": "KogMaw",
                "K'Sante": "KSante",
                "Dr. Mundo": "DrMundo",
                "Dr Mundo": "DrMundo",
                "Aurelion Sol": "AurelionSol",
                "Jarvan IV": "JarvanIV",
                "Lee Sin": "LeeSin",
                "Master Yi": "MasterYi",
                "Miss Fortune": "MissFortune",
                "Rek'Sai": "RekSai",
                "Tahm Kench": "TahmKench",
                "Twisted Fate": "TwistedFate",
                "Xin Zhao": "XinZhao",
                "FiddleSticks": "Fiddlesticks"
            };

            function getChampIcon(champName) {
                if (!champName) return `${D_DRAGON}/profileicon/29.png`;
                const cleanName = champName.trim();
                const fixed = CHAMP_NAME_FIXES[cleanName] || cleanName.replace(/[^a-zA-Z0-9]/g, '');
                return `${D_DRAGON}/champion/${fixed}.png`;
            }

            function formatGold(val) {
                const num = Number(val) || 0;
                if (num >= 1000) {
                    return (num / 1000).toFixed(1) + 'k';
                }
                return num.toString();
            }

            function getPlayerNameStyle(name) {
                if (!name) return 'font-size: 13px;';
                let units = 0;
                for (let i = 0; i < name.length; i++) {
                    const c = name[i];
                    if (c >= 'A' && c <= 'Z') units += 1.15;
                    else if (c === 'W' || c === 'M' || c === 'w' || c === 'm') units += 1.35;
                    else if (c === 'i' || c === 'l' || c === ' ' || c === '.' || c === '!') units += 0.45;
                    else units += 0.85;
                }
                if (units > 14) return 'font-size: 9.5px;';
                if (units > 12) return 'font-size: 10.5px;';
                if (units > 10) return 'font-size: 11.5px;';
                return 'font-size: 13px;';
            }

            function fitPlayerNames(root = document) {
                root.querySelectorAll('.player-name').forEach(el => {
                    let size = parseFloat(window.getComputedStyle(el).fontSize) || 13;
                    while (el.scrollWidth > el.clientWidth && size > 8.5) {
                        size -= 0.5;
                        el.style.fontSize = size + 'px';
                    }
                });
            }

            function getKeystoneIcon(keystoneId) {
                if (!keystoneId) return '/static/icons/crystal_rune.svg';
                return `/static/icons/runes/keystone_${keystoneId}.png`;
            }

            function getRoleIcon(role) {
                const r = (role || '').toUpperCase();
                if (r === 'TOP') return '/static/icons/role_top.png';
                if (r === 'JUNGLE' || r === 'JUG') return '/static/icons/role_jungle.png';
                if (r === 'MIDDLE' || r === 'MID') return '/static/icons/role_mid.png';
                if (r === 'BOTTOM' || r === 'BOT' || r === 'ADC') return '/static/icons/role_bot.png';
                if (r === 'UTILITY' || r === 'SUPPORT' || r === 'SUP') return '/static/icons/role_support.png';
                return '/static/icons/role_fill.png';
            }

            function renderItems(items, trinket, isRed = false) {
                const slots = [];
                // 6 regular item slots
                for (let i = 0; i < 6; i++) {
                    const item = items[i];
                    if (item && item.itemID) {
                        slots.push(`<div class="item-slot"><img class="item-img" src="${getItemIcon(item.itemID)}" onerror="this.style.display='none'"></div>`);
                    } else {
                        slots.push(`<div class="item-slot"></div>`);
                    }
                }
                // Trinket slot (7)
                const trinketHtml = trinket && trinket.itemID
                    ? `<div class="item-slot trinket-slot"><img class="item-img" src="${getItemIcon(trinket.itemID)}" onerror="this.style.display='none'"></div>`
                    : `<div class="item-slot trinket-slot"></div>`;

                return isRed ? slots.join('') + trinketHtml : trinketHtml + slots.join('');
            }

            function renderRow(matchup, isFirstRow = false) {
                const bp = matchup.blue_player;
                const rp = matchup.red_player;

                if (!bp && !rp) return '';

                const isFirst = isFirstRow || matchup.lane_index === 0;
                const role = matchup.role || (bp && bp.position) || (rp && rp.position) || '';
                const roleLabel = matchup.role_label || role || 'Fill';

                // Center lane gold diff pill
                const diff = matchup.gold_diff_abs || 0;
                const diffFormatted = diff >= 1000 ? (diff / 1000).toFixed(1) + 'k' : diff;
                let diffHtml = '';
                if (matchup.leader === 'BLUE') {
                    diffHtml = `<div class="diff-pill blue-lead" title="Blue leads by ${diff} gold">${diffFormatted}</div>`;
                } else if (matchup.leader === 'RED') {
                    diffHtml = `<div class="diff-pill red-lead" title="Red leads by ${diff} gold">${diffFormatted}</div>`;
                } else {
                    diffHtml = `<div class="diff-pill equal" title="Even gold">0</div>`;
                }

                // Blue team side (Outer: Role -> Trinket -> Items -> Keystone + Spells -> Champ -> Player Info -> Gold -> CS)
                const blueHtml = bp ? `
                    <div class="lane-player-blue">
                        <div class="role-badge blue-role" title="${roleLabel} Lane">
                            <img class="role-icon-img" src="${getRoleIcon(role)}" alt="${roleLabel}">
                        </div>

                        <div class="items-grid">${renderItems(bp.items || [], bp.trinket, false)}</div>
                        
                        <div class="spells-runes-group">
                            <div class="keystone-box" title="${bp.keystone_name || 'Keystone Rune'}">
                                <img class="keystone-img" src="${getKeystoneIcon(bp.keystone_id)}" onerror="this.src='/static/icons/crystal_rune.svg'" alt="Rune">
                            </div>
                            <div class="spells-col">
                                <div class="spell-box"><img class="spell-img" src="${getSpellIcon(bp.spells[0])}"></div>
                                <div class="spell-box"><img class="spell-img" src="${getSpellIcon(bp.spells[1])}"></div>
                            </div>
                        </div>

                        <div class="champ-box">
                            <img class="champ-img" src="${getChampIcon(bp.champion)}" onerror="this.src='https://ddragon.leagueoflegends.com/cdn/14.20.1/img/profileicon/29.png'">
                            <div class="champ-level-badge">${bp.level}</div>
                        </div>

                        <div class="player-details">
                            <div class="player-name" style="${getPlayerNameStyle(bp.summoner_name)}" title="${bp.summoner_name}">${bp.summoner_name}</div>
                            <div class="player-kda">${bp.kills}/${bp.deaths}/${bp.assists}</div>
                        </div>

                        <div class="player-stats-group">
                            <div class="stat-col">
                                ${isFirst ? '<div class="stat-header-icon" title="Creep Score"><img src="/static/icons/cs_minion.svg" class="col-icon col-icon-minion" alt="CS"></div>' : ''}
                                <div class="cs-stat-pill" title="Creep Score">${bp.cs}</div>
                            </div>
                            <div class="stat-col">
                                ${isFirst ? '<div class="stat-header-icon" title="Player Gold"><img src="/static/icons/cs_coin.svg" class="col-icon col-icon-coin" alt="Gold"></div>' : ''}
                                <div class="gold-stat-pill" title="Total Item Gold">
                                    <span>${formatGold(bp.items_gold)}</span>
                                </div>
                            </div>
                        </div>
                    </div>
                ` : '<div class="lane-player-blue"></div>';

                // Red team side (Inner: Gold -> CS -> Player Info -> Champ -> Spells + Keystone -> Items -> Trinket -> Role :Outer)
                const redHtml = rp ? `
                    <div class="lane-player-red">
                        <div class="player-stats-group">
                            <div class="stat-col">
                                ${isFirst ? '<div class="stat-header-icon" title="Player Gold"><img src="/static/icons/cs_coin.svg" class="col-icon col-icon-coin" alt="Gold"></div>' : ''}
                                <div class="gold-stat-pill" title="Total Item Gold">
                                    <span>${formatGold(rp.items_gold)}</span>
                                </div>
                            </div>
                            <div class="stat-col">
                                ${isFirst ? '<div class="stat-header-icon" title="Creep Score"><img src="/static/icons/cs_minion.svg" class="col-icon col-icon-minion" alt="CS"></div>' : ''}
                                <div class="cs-stat-pill" title="Creep Score">${rp.cs}</div>
                            </div>
                        </div>

                        <div class="player-details text-right">
                            <div class="player-name" style="${getPlayerNameStyle(rp.summoner_name)}" title="${rp.summoner_name}">${rp.summoner_name}</div>
                            <div class="player-kda">${rp.kills}/${rp.deaths}/${rp.assists}</div>
                        </div>

                        <div class="champ-box red-border">
                            <img class="champ-img" src="${getChampIcon(rp.champion)}" onerror="this.src='https://ddragon.leagueoflegends.com/cdn/14.20.1/img/profileicon/29.png'">
                            <div class="champ-level-badge red-bg">${rp.level}</div>
                        </div>

                        <div class="spells-runes-group">
                            <div class="spells-col">
                                <div class="spell-box"><img class="spell-img" src="${getSpellIcon(rp.spells[0])}"></div>
                                <div class="spell-box"><img class="spell-img" src="${getSpellIcon(rp.spells[1])}"></div>
                            </div>
                            <div class="keystone-box" title="${rp.keystone_name || 'Keystone Rune'}">
                                <img class="keystone-img" src="${getKeystoneIcon(rp.keystone_id)}" onerror="this.src='/static/icons/crystal_rune.svg'" alt="Rune">
                            </div>
                        </div>

                        <div class="items-grid">${renderItems(rp.items || [], rp.trinket, true)}</div>

                        <div class="role-badge red-role" title="${roleLabel} Lane">
                            <img class="role-icon-img" src="${getRoleIcon(role)}" alt="${roleLabel}">
                        </div>
                    </div>
                ` : '<div class="lane-player-red"></div>';

                return `
                    <div class="scoreboard-row">
                        ${blueHtml}
                        <div class="lane-gold-center">${diffHtml}</div>
                        ${redHtml}
                    </div>
                `;
            }

            // Dragon configuration with official Riot Games assets
            const DRAGON_CONFIG = {
                "Ocean": { url: "/static/icons/dragon_ocean.png", color: "#0ea5e9", glow: "rgba(14, 165, 233, 0.7)", label: "Ocean Dragon" },
                "Infernal": { url: "/static/icons/dragon_infernal.png", color: "#f97316", glow: "rgba(249, 115, 22, 0.7)", label: "Infernal Dragon" },
                "Mountain": { url: "/static/icons/dragon_mountain.png", color: "#d97706", glow: "rgba(217, 119, 6, 0.7)", label: "Mountain Dragon" },
                "Cloud": { url: "/static/icons/dragon_cloud.png", color: "#94a3b8", glow: "rgba(148, 163, 184, 0.7)", label: "Cloud Dragon" },
                "Hextech": { url: "/static/icons/dragon_hextech.png", color: "#06b6d4", glow: "rgba(6, 182, 212, 0.7)", label: "Hextech Dragon" },
                "Chemtech": { url: "/static/icons/dragon_chemtech.png", color: "#84cc16", glow: "rgba(132, 204, 22, 0.7)", label: "Chemtech Dragon" },
                "Elder": { url: "/static/icons/dragon_elder.png", color: "#eab308", glow: "rgba(234, 179, 8, 0.9)", label: "Elder Dragon" }
            };

            function renderDragonSlots(dragonsList) {
                let html = '';
                const totalSlots = Math.max(4, dragonsList.length);
                for (let i = 0; i < totalSlots; i++) {
                    if (i < dragonsList.length) {
                        const dType = dragonsList[i];
                        const cfg = DRAGON_CONFIG[dType] || { url: "/static/icons/dragon_default.png", color: "#00f2fe", glow: "rgba(0, 242, 254, 0.5)", label: dType };
                        html += `<div class="dragon-slot filled" style="border-color: ${cfg.color}; box-shadow: 0 0 10px ${cfg.glow};" title="${cfg.label}">
                            <img src="${cfg.url}" class="dragon-slot-img" alt="${cfg.label}">
                        </div>`;
                    } else {
                        html += `<div class="dragon-slot empty"></div>`;
                    }
                }
                return html;
            }

            // Application State
            let currentView = 'history';
            let activeEventSource = null;
            let isLiveGameActive = false;

            // UI Elements
            const btnFindLive = document.getElementById("btn-find-live");
            const tabHistory = document.getElementById("tab-history");
            const viewLive = document.getElementById("view-live");
            const viewHistory = document.getElementById("view-history");
            const historyList = document.getElementById("history-list");
            const matchModal = document.getElementById("match-modal");
            const toastEl = document.getElementById("toast");

            // Live HUD Elements
            const blueGoldEl = document.getElementById("blue-gold");
            const redGoldEl = document.getElementById("red-gold");
            const blueTowersEl = document.getElementById("blue-towers");
            const redTowersEl = document.getElementById("red-towers");
            const blueKillsEl = document.getElementById("blue-kills");
            const redKillsEl = document.getElementById("red-kills");
            const gameTimeEl = document.getElementById("game-time");
            const goldLeadBadge = document.getElementById("gold-lead-badge");
            const goldLeadBadgeRed = document.getElementById("gold-lead-badge-red");
            const probBarBlue = document.getElementById("prob-bar-blue");
            const probBarRed = document.getElementById("prob-bar-red");
            const winProbBlueVal = document.getElementById("win-prob-blue-val");
            const winProbRedVal = document.getElementById("win-prob-red-val");
            const matchupRows = document.getElementById("matchup-rows");
            const onnxLatEl = document.getElementById("onnx-lat");
            const sessionInfoEl = document.getElementById("session-info");

            function showToast(msg, isSuccess = true) {
                toastEl.innerText = msg;
                toastEl.style.borderColor = isSuccess ? "#10b981" : "#ef4444";
                toastEl.style.color = isSuccess ? "#6ee7b7" : "#fca5a5";
                toastEl.style.display = "block";
                setTimeout(() => { toastEl.style.display = "none"; }, 3800);
            }

            function switchView(viewName, liveActive = null) {
                currentView = viewName;
                if (viewName === 'live') {
                    viewLive.style.display = 'block';
                    viewHistory.style.display = 'none';
                    if (btnFindLive) btnFindLive.classList.add('active');
                    if (tabHistory) tabHistory.classList.remove('active');

                    if (liveActive !== null) {
                        isLiveGameActive = liveActive;
                    }

                    const liveActiveEl = document.getElementById("live-active-content");
                    const liveNoGameEl = document.getElementById("live-no-game");

                    if (isLiveGameActive) {
                        if (liveActiveEl) liveActiveEl.style.display = 'block';
                        if (liveNoGameEl) liveNoGameEl.style.display = 'none';
                        connectLiveStream();
                    } else {
                        if (liveActiveEl) liveActiveEl.style.display = 'none';
                        if (liveNoGameEl) liveNoGameEl.style.display = 'block';
                        if (activeEventSource) {
                            activeEventSource.close();
                            activeEventSource = null;
                        }
                    }
                } else {
                    viewLive.style.display = 'none';
                    viewHistory.style.display = 'block';
                    if (btnFindLive) btnFindLive.classList.remove('active');
                    if (tabHistory) tabHistory.classList.add('active');
                    loadMatchHistory();
                }
            }

            async function findLiveGame() {
                const iconSpan = document.getElementById("find-spinner-wrap");
                const textSpan = document.getElementById("find-text");
                if (btnFindLive) btnFindLive.disabled = true;
                if (iconSpan) iconSpan.innerHTML = '<span class="spinner"></span> ';
                if (textSpan) textSpan.innerText = 'Searching...';

                try {
                    const resp = await fetch('/api/v1/live/find', { method: 'POST' });
                    const data = await resp.json();

                    if (data.found && data.active) {
                        isLiveGameActive = true;
                        switchView('live', true);
                        showToast(`Active match detected (${data.game_time || '00:00'})! Connected to Live Game.`, true);
                    } else {
                        isLiveGameActive = false;
                        switchView('live', false);
                        const msgEl = document.getElementById("no-game-message");
                        const statusTxt = document.getElementById("live-api-status-text");
                        if (msgEl) {
                            if (data.ignored) {
                                msgEl.innerText = data.message || "Detected game mode is excluded from telemetry tracking.";
                            } else {
                                msgEl.innerText = `Jump into the Summoner's Rift and press "Refresh" button to see live data`;
                            }
                        }
                        if (statusTxt) {
                            statusTxt.innerText = "API unreachable";
                        }
                        showToast(data.message || "No active League of Legends match detected.", false);
                    }
                } catch (err) {
                    isLiveGameActive = false;
                    switchView('live', false);
                    showToast("Error checking game status: " + err, false);
                } finally {
                    if (btnFindLive) btnFindLive.disabled = false;
                    if (iconSpan) iconSpan.innerHTML = '';
                    if (textSpan) textSpan.innerText = 'Find Live Game';
                }
            }

            let localWinProbTimeline = [];
            let lastRecordedMinute = -1;

            function updateWinProbSparkline(serverHistory, currentProbBlue, gameTimeSec, gameTimeFormatted) {
                let timeline = [];
                if (serverHistory && serverHistory.length > 0) {
                    timeline = serverHistory.slice();
                    localWinProbTimeline = serverHistory.slice();
                } else {
                    const currMin = Math.floor((gameTimeSec || 0) / 60);
                    if (currMin !== lastRecordedMinute) {
                        lastRecordedMinute = currMin;
                        localWinProbTimeline.push({
                            minute: currMin,
                            time: gameTimeFormatted || `${currMin}:00`,
                            prob_blue: currentProbBlue != null ? currentProbBlue : 50.0
                        });
                    }
                    timeline = localWinProbTimeline.slice();
                }

                if (!timeline || timeline.length === 0) {
                    timeline = [{ minute: 0, time: "00:00", prob_blue: currentProbBlue != null ? currentProbBlue : 50.0 }];
                }

                // Ensure timeline always starts with minute 0 at 50.0% baseline
                if (timeline.length > 0 && timeline[0].minute > 0) {
                    timeline.unshift({ minute: 0, time: "00:00", prob_blue: 50.0 });
                }

                const svgWidth = 250;
                const svgHeight = 42;
                const baselineY = 38; // 50% baseline on the X axis
                const topY = 4;       // Dynamic peak top padding
                const n = timeline.length;
                const maxMin = timeline[timeline.length - 1].minute;

                // Dynamic Y max based on the highest lead winrate that occurred
                const curLead = currentProbBlue != null ? Math.max(currentProbBlue, 100.0 - currentProbBlue) : 50.0;
                const allLeadValues = timeline.map(pt => Math.max(pt.prob_blue, 100.0 - pt.prob_blue));
                allLeadValues.push(curLead);
                const maxObserved = Math.max(50.0, ...allLeadValues);
                const yMax = Math.max(55.0, Math.min(100.0, maxObserved));

                function getY(leadProb) {
                    const p = Math.max(50.0, Math.min(yMax, leadProb));
                    const ratio = (p - 50.0) / (yMax - 50.0);
                    return baselineY - ratio * (baselineY - topY);
                }

                // SVG coordinates mapping proportional to game minutes
                const points = timeline.map((pt, i) => {
                    let x;
                    if (n <= 1) {
                        x = svgWidth / 2;
                    } else if (maxMin > 0) {
                        x = (pt.minute / maxMin) * (svgWidth - 8) + 4;
                    } else {
                        x = (i / (n - 1)) * (svgWidth - 8) + 4;
                    }
                    const clampedBlue = Math.max(0.0, Math.min(100.0, pt.prob_blue));
                    const isBlue = clampedBlue >= 50.0;
                    const leadProb = isBlue ? clampedBlue : (100.0 - clampedBlue);
                    const y = getY(leadProb);
                    return {
                        x,
                        y,
                        prob: clampedBlue,
                        leadProb,
                        isBlue,
                        minute: pt.minute,
                        time: pt.time
                    };
                });

                // Generate segmented Blue and Red paths with bounce at 50% (X axis)
                const blueLines = [];
                const redLines = [];
                const blueAreas = [];
                const redAreas = [];
                const by = baselineY.toFixed(1);

                for (let i = 0; i < points.length - 1; i++) {
                    const p1 = points[i];
                    const p2 = points[i + 1];
                    const x1 = p1.x.toFixed(1);
                    const y1 = p1.y.toFixed(1);
                    const x2 = p2.x.toFixed(1);
                    const y2 = p2.y.toFixed(1);

                    if (p1.prob >= 50.0 && p2.prob >= 50.0) {
                        // Blue leads throughout segment
                        blueLines.push(`M ${x1} ${y1} L ${x2} ${y2}`);
                        blueAreas.push(`M ${x1} ${by} L ${x1} ${y1} L ${x2} ${y2} L ${x2} ${by} Z`);
                    } else if (p1.prob <= 50.0 && p2.prob <= 50.0) {
                        // Red leads throughout segment
                        redLines.push(`M ${x1} ${y1} L ${x2} ${y2}`);
                        redAreas.push(`M ${x1} ${by} L ${x1} ${y1} L ${x2} ${y2} L ${x2} ${by} Z`);
                    } else {
                        // Crosses 50% baseline -> touches X axis before changing color!
                        const t = (50.0 - p1.prob) / (p2.prob - p1.prob);
                        const xMid = (p1.x + t * (p2.x - p1.x)).toFixed(1);
                        const yMid = by;

                        if (p1.prob > 50.0) {
                            // Blue drops to Red: touches X axis in blue, bounces up in red
                            blueLines.push(`M ${x1} ${y1} L ${xMid} ${yMid}`);
                            blueAreas.push(`M ${x1} ${by} L ${x1} ${y1} L ${xMid} ${yMid} Z`);

                            redLines.push(`M ${xMid} ${yMid} L ${x2} ${y2}`);
                            redAreas.push(`M ${xMid} ${by} L ${x2} ${y2} L ${x2} ${by} Z`);
                        } else {
                            // Red drops to Blue: touches X axis in red, bounces up in blue
                            redLines.push(`M ${x1} ${y1} L ${xMid} ${yMid}`);
                            redAreas.push(`M ${x1} ${by} L ${x1} ${y1} L ${xMid} ${yMid} Z`);

                            blueLines.push(`M ${xMid} ${yMid} L ${x2} ${y2}`);
                            blueAreas.push(`M ${xMid} ${by} L ${x2} ${y2} L ${x2} ${by} Z`);
                        }
                    }
                }

                const lineBlueEl = document.getElementById("sparkline-line-blue");
                const lineRedEl = document.getElementById("sparkline-line-red");
                const areaBlueEl = document.getElementById("sparkline-area-blue");
                const areaRedEl = document.getElementById("sparkline-area-red");
                const dotEl = document.getElementById("sparkline-dot");
                const currProbEl = document.getElementById("chart-curr-prob");
                const axisEndEl = document.getElementById("chart-axis-end");
                const axisQ1El = document.getElementById("chart-axis-q1");
                const axisMidEl = document.getElementById("chart-axis-mid");
                const axisQ3El = document.getElementById("chart-axis-q3");

                if (lineBlueEl) lineBlueEl.setAttribute("d", blueLines.join(" "));
                if (lineRedEl) lineRedEl.setAttribute("d", redLines.join(" "));
                if (areaBlueEl) areaBlueEl.setAttribute("d", blueAreas.join(" "));
                if (areaRedEl) areaRedEl.setAttribute("d", redAreas.join(" "));

                const lastPt = points[points.length - 1];
                const isBlueLead = lastPt.prob >= 50.0;
                const leadProb = isBlueLead ? lastPt.prob : (100.0 - lastPt.prob);
                const isTie = Math.abs(lastPt.prob - 50.0) < 0.1;
                const teamName = isTie ? "Even" : (isBlueLead ? "Blue" : "Red");
                const themeColor = isTie ? "#94a3b8" : (isBlueLead ? "#38bdf8" : "#f87171");
                const leadFormatted = Number(leadProb.toFixed(1));

                if (dotEl) {
                    dotEl.setAttribute("cx", lastPt.x.toFixed(1));
                    dotEl.setAttribute("cy", lastPt.y.toFixed(1));
                    dotEl.setAttribute("fill", themeColor);
                }
                if (currProbEl) {
                    if (isTie) {
                        currProbEl.innerHTML = `<span style="color: #94a3b8; font-weight: 800;">50% Even</span>`;
                    } else {
                        currProbEl.innerHTML = `<span style="color: ${themeColor}; font-weight: 800;">${leadFormatted}% ${teamName}</span>`;
                    }
                }

                if (axisEndEl) {
                    axisEndEl.innerText = `${maxMin}m`;
                }
                if (axisQ1El && axisMidEl && axisQ3El) {
                    if (maxMin >= 4) {
                        axisQ1El.innerText = `${Math.floor(maxMin * 0.25)}m`;
                        axisMidEl.innerText = `${Math.floor(maxMin * 0.50)}m`;
                        axisQ3El.innerText = `${Math.floor(maxMin * 0.75)}m`;
                    } else {
                        axisQ1El.innerText = '';
                        axisMidEl.innerText = '';
                        axisQ3El.innerText = '';
                    }
                }

                // Attach hover listener once
                const container = document.getElementById("sparkline-container");
                const tooltip = document.getElementById("sparkline-tooltip");
                if (container && tooltip && !container._hoverBound) {
                    container._hoverBound = true;
                    container.addEventListener("mousemove", (e) => {
                        if (!container._points || container._points.length === 0) return;
                        const rect = container.getBoundingClientRect();
                        const mouseX = e.clientX - rect.left;
                        let closestPt = container._points[0];
                        let closestDist = Math.abs(mouseX - closestPt.x);
                        for (let k = 1; k < container._points.length; k++) {
                            const dist = Math.abs(mouseX - container._points[k].x);
                            if (dist < closestDist) {
                                closestDist = dist;
                                closestPt = container._points[k];
                            }
                        }
                        const pt = closestPt;
                        if (pt) {
                            const isPtTie = Math.abs(pt.prob - 50.0) < 0.1;
                            const isPtBlue = pt.prob >= 50.0;
                            const ptTeam = isPtTie ? "Even" : (isPtBlue ? "Blue" : "Red");
                            const color = isPtTie ? "#94a3b8" : (isPtBlue ? "#38bdf8" : "#f87171");
                            const ptLeadVal = isPtBlue ? pt.prob : (100.0 - pt.prob);
                            const ptLeadFormatted = Number(ptLeadVal.toFixed(1));
                            tooltip.style.opacity = "1";
                            tooltip.style.left = `${mouseX}px`;
                            tooltip.style.borderColor = color;
                            tooltip.innerHTML = `<span style="color: #94a3b8;">${pt.time || pt.minute + 'm'}:</span> <strong style="color: ${color}">${ptLeadFormatted}% ${ptTeam}</strong>`;
                        }
                    });
                    container.addEventListener("mouseleave", () => {
                        tooltip.style.opacity = "0";
                    });
                }
                if (container) {
                    container._points = points;
                }
            }

            function connectLiveStream() {
                if (activeEventSource) return;

                activeEventSource = new EventSource("/api/v1/stream/live");

                activeEventSource.addEventListener("hud_telemetry_tick", function(e) {
                    const data = JSON.parse(e.data);

                    if (data.game_status === "MATCH_COMPLETED") {
                        showToast("Match completed! Finalizing summary and returning to idle auto-detect...", true);
                        isLiveGameActive = false;
                        switchView('live', false);
                        loadMatchHistory();
                        startIdlePolling();
                        return;
                    }

                    // Top Bar Stats
                    blueGoldEl.innerText = data.metrics.blue_gold_k;
                    redGoldEl.innerText = data.metrics.red_gold_k;
                    blueTowersEl.innerText = data.metrics.blue_towers;
                    redTowersEl.innerText = data.metrics.red_towers;
                    blueKillsEl.innerText = data.metrics.blue_kills;
                    redKillsEl.innerText = data.metrics.red_kills;
                    gameTimeEl.innerText = data.game_time_formatted;
                    sessionInfoEl.innerText = "Session: " + data.session_id.substring(0, 8) + "...";
                    onnxLatEl.innerText = data.inference.inference_latency_ms + " ms";

                    // Objective Timers (Baron / Herald + Dragon)
                    if (data.objectives && data.objectives.timers) {
                        const timers = data.objectives.timers;
                        
                        if (timers.baron_herald) {
                            const bh = timers.baron_herald;
                            const isAlive = bh.status === "ALIVE";
                            const bNameEl = document.getElementById("baron-name");
                            if (bNameEl) bNameEl.innerText = bh.name || "BARON";
                            const bTimeEl = document.getElementById("baron-time");
                            if (bTimeEl) {
                                bTimeEl.innerText = bh.label || (isAlive ? "ALIVE" : "--:--");
                                bTimeEl.style.color = isAlive ? "#10b981" : "#f1f5f9";
                            }
                            const bCard = document.getElementById("baron-timer-card");
                            if (bCard) {
                                if (isAlive) bCard.classList.add("is-alive"); else bCard.classList.remove("is-alive");
                            }
                            const bImg = document.getElementById("baron-icon-img");
                            if (bImg) {
                                bImg.src = bh.type === "HERALD" ? "/static/icons/herald.png" : "/static/icons/baron.png";
                            }
                        }

                        if (timers.dragon) {
                            const dt = timers.dragon;
                            const isAlive = dt.status === "ALIVE";
                            const dNameEl = document.getElementById("dragon-name");
                            if (dNameEl) dNameEl.innerText = dt.name || "DRAGON";
                            const dTimeEl = document.getElementById("dragon-time");
                            if (dTimeEl) {
                                dTimeEl.innerText = dt.label || (isAlive ? "ALIVE" : "--:--");
                                dTimeEl.style.color = isAlive ? "#10b981" : "#f1f5f9";
                            }
                            const dCard = document.getElementById("dragon-timer-card");
                            if (dCard) {
                                if (isAlive) dCard.classList.add("is-alive"); else dCard.classList.remove("is-alive");
                            }
                            const dImg = document.getElementById("dragon-icon-img");
                            if (dImg) {
                                dImg.src = dt.is_elder ? "/static/icons/dragon_elder.png" : "/static/icons/dragon_default.png";
                            }
                        }
                    }

                    // Secured Objectives (Voidgrubs and Dragons)
                    if (data.objectives) {
                        if (data.objectives.blue) {
                            const bGrubs = document.getElementById("blue-voidgrubs");
                            if (bGrubs) bGrubs.innerText = data.objectives.blue.voidgrubs || 0;
                            const bDragons = document.getElementById("blue-dragon-slots");
                            if (bDragons) bDragons.innerHTML = renderDragonSlots(data.objectives.blue.dragons || []);
                        }
                        if (data.objectives.red) {
                            const rGrubs = document.getElementById("red-voidgrubs");
                            if (rGrubs) rGrubs.innerText = data.objectives.red.voidgrubs || 0;
                            const rDragons = document.getElementById("red-dragon-slots");
                            if (rDragons) rDragons.innerHTML = renderDragonSlots(data.objectives.red.dragons || []);
                        }
                    }

                    // Gold Lead Badges (Shown on the team with the advantage)
                    const gDiff = data.metrics.gold_diff;
                    const diffAbsK = data.metrics.gold_diff_k;
                    if (gDiff > 300) {
                        if (goldLeadBadge) {
                            goldLeadBadge.style.display = 'inline-block';
                            goldLeadBadge.innerText = "+" + diffAbsK;
                            goldLeadBadge.style.background = "#00f2fe";
                            goldLeadBadge.style.color = "#04101d";
                        }
                        if (goldLeadBadgeRed) goldLeadBadgeRed.style.display = 'none';
                    } else if (gDiff < -300) {
                        if (goldLeadBadge) goldLeadBadge.style.display = 'none';
                        if (goldLeadBadgeRed) {
                            goldLeadBadgeRed.style.display = 'inline-block';
                            goldLeadBadgeRed.innerText = "+" + diffAbsK;
                            goldLeadBadgeRed.style.background = "#ef4444";
                            goldLeadBadgeRed.style.color = "#ffffff";
                        }
                    } else {
                        if (goldLeadBadge) goldLeadBadge.style.display = 'none';
                        if (goldLeadBadgeRed) goldLeadBadgeRed.style.display = 'none';
                    }

                    // Win Probability Bar
                    const pBlue = data.inference.win_prob_blue_pct;
                    const pRed = data.inference.win_prob_red_pct;
                    probBarBlue.style.width = pBlue + "%";
                    probBarRed.style.width = pRed + "%";
                    winProbBlueVal.innerText = pBlue + "%";
                    winProbRedVal.innerText = pRed + "%";

                    // Compact Win Probability Sparkline Timeline
                    updateWinProbSparkline(data.win_prob_history, pBlue, data.game_time_seconds, data.game_time_formatted);

                    // Main Scoreboard Rows
                    if (data.matchups && data.matchups.length > 0) {
                        matchupRows.innerHTML = data.matchups.map((m, idx) => renderRow(m, idx === 0)).join('');
                        fitPlayerNames(matchupRows);
                    }
                });

                activeEventSource.onerror = function() {
                    // Stream disconnected
                };
            }

            async function loadMatchHistory() {
                try {
                    const resp = await fetch('/api/v1/matches');
                    const data = await resp.json();
                    const matches = data.matches || [];

                    if (matches.length === 0) {
                        historyList.innerHTML = `
                            <div style="text-align: center; padding: 50px 20px; background: #0b111e; border: 1px dashed #334155; border-radius: 10px;">
                                <div style="font-size: 16px; font-weight: 700; color: #f8fafc;">No Recorded Matches Found</div>
                                <div style="font-size: 12px; color: #64748b; margin-top: 6px;">Play a match in League of Legends and click 'Find Live Game' to record your first game session.</div>
                            </div>
                        `;
                        return;
                    }

                    historyList.innerHTML = matches.map(m => renderMatchCard(m)).join('');
                } catch (err) {
                    historyList.innerHTML = `<div style="color: #ef4444; padding: 20px;">Failed to load match history: ${err}</div>`;
                }
            }

            function renderMatchSparkline(history, durationStr, winner) {
                let timeline = history ? history.slice() : [];
                if (!timeline || timeline.length === 0) {
                    const defaultProb = winner === "BLUE" ? 75.0 : 25.0;
                    timeline = [
                        { minute: 0, time: "00:00", prob_blue: 50.0 },
                        { minute: 20, time: durationStr || "20:00", prob_blue: defaultProb }
                    ];
                } else if (timeline.length === 1) {
                    timeline = [
                        { minute: 0, time: "00:00", prob_blue: 50.0 },
                        timeline[0]
                    ];
                }

                // Ensure timeline always starts with minute 0 at 50.0% baseline
                if (timeline.length > 0 && timeline[0].minute > 0) {
                    timeline.unshift({ minute: 0, time: "00:00", prob_blue: 50.0 });
                }

                const svgW = 170;
                const svgH = 30;
                const baselineY = 26; // 50% baseline on the X axis
                const topY = 4;
                const n = timeline.length;
                const maxMin = timeline[timeline.length - 1].minute;

                // Dynamic Y max based on the highest lead winrate that occurred
                const allLeadValues = timeline.map(pt => Math.max(pt.prob_blue, 100.0 - pt.prob_blue));
                const maxObserved = Math.max(50.0, ...allLeadValues);
                const yMax = Math.max(55.0, Math.min(100.0, maxObserved));

                function getY(leadProb) {
                    const p = Math.max(50.0, Math.min(yMax, leadProb));
                    const ratio = (p - 50.0) / (yMax - 50.0);
                    return baselineY - ratio * (baselineY - topY);
                }

                const points = timeline.map((pt, i) => {
                    let x;
                    if (n <= 1) {
                        x = svgW / 2;
                    } else if (maxMin > 0) {
                        x = (pt.minute / maxMin) * (svgW - 8) + 4;
                    } else {
                        x = (i / (n - 1)) * (svgW - 8) + 4;
                    }
                    const clampedBlue = Math.max(0.0, Math.min(100.0, pt.prob_blue));
                    const isBlue = clampedBlue >= 50.0;
                    const leadProb = isBlue ? clampedBlue : (100.0 - clampedBlue);
                    const y = getY(leadProb);
                    return {
                        x,
                        y,
                        prob: clampedBlue,
                        leadProb,
                        isBlue,
                        minute: pt.minute,
                        time: pt.time
                    };
                });

                const blueLines = [];
                const redLines = [];
                const blueAreas = [];
                const redAreas = [];
                const by = baselineY.toFixed(1);

                for (let i = 0; i < points.length - 1; i++) {
                    const p1 = points[i];
                    const p2 = points[i + 1];
                    const x1 = p1.x.toFixed(1);
                    const y1 = p1.y.toFixed(1);
                    const x2 = p2.x.toFixed(1);
                    const y2 = p2.y.toFixed(1);

                    if (p1.prob >= 50.0 && p2.prob >= 50.0) {
                        // Blue leads throughout segment
                        blueLines.push(`M ${x1} ${y1} L ${x2} ${y2}`);
                        blueAreas.push(`M ${x1} ${by} L ${x1} ${y1} L ${x2} ${y2} L ${x2} ${by} Z`);
                    } else if (p1.prob <= 50.0 && p2.prob <= 50.0) {
                        // Red leads throughout segment
                        redLines.push(`M ${x1} ${y1} L ${x2} ${y2}`);
                        redAreas.push(`M ${x1} ${by} L ${x1} ${y1} L ${x2} ${y2} L ${x2} ${by} Z`);
                    } else {
                        // Crosses 50% baseline -> touches X axis before changing color!
                        const t = (50.0 - p1.prob) / (p2.prob - p1.prob);
                        const xMid = (p1.x + t * (p2.x - p1.x)).toFixed(1);
                        const yMid = by;

                        if (p1.prob > 50.0) {
                            // Blue drops to Red: touches X axis in blue, bounces up in red
                            blueLines.push(`M ${x1} ${y1} L ${xMid} ${yMid}`);
                            blueAreas.push(`M ${x1} ${by} L ${x1} ${y1} L ${xMid} ${by} Z`);

                            redLines.push(`M ${xMid} ${yMid} L ${x2} ${y2}`);
                            redAreas.push(`M ${xMid} ${by} L ${x2} ${y2} L ${x2} ${by} Z`);
                        } else {
                            // Red drops to Blue: touches X axis in red, bounces up in blue
                            redLines.push(`M ${x1} ${y1} L ${xMid} ${yMid}`);
                            redAreas.push(`M ${x1} ${by} L ${x1} ${y1} L ${xMid} ${by} Z`);

                            blueLines.push(`M ${xMid} ${yMid} L ${x2} ${y2}`);
                            blueAreas.push(`M ${xMid} ${by} L ${x2} ${y2} L ${x2} ${by} Z`);
                        }
                    }
                }

                const lastPt = points[points.length - 1];
                const isBlueLead = lastPt.prob >= 50.0;
                const isTie = Math.abs(lastPt.prob - 50.0) < 0.1;
                const leadProb = isBlueLead ? lastPt.prob : (100.0 - lastPt.prob);
                const teamName = isTie ? "Even" : (isBlueLead ? "Blue" : "Red");
                const lastColor = isTie ? "#94a3b8" : (isBlueLead ? "#38bdf8" : "#f87171");
                const leadText = isTie ? "50% Even" : `${leadProb.toFixed(0)}% ${teamName}`;
                let totalMinutes = typeof lastPt.minute === 'number' && lastPt.minute > 0 ? lastPt.minute : 0;
                if (!totalMinutes && durationStr) {
                    const parts = String(durationStr).split(':');
                    if (parts.length >= 2) {
                        totalMinutes = parseInt(parts[0], 10) || 0;
                    } else {
                        totalMinutes = parseInt(durationStr, 10) || 0;
                    }
                }

                let q1Text = '';
                let q2Text = '';
                let q3Text = '';
                if (totalMinutes >= 4) {
                    q1Text = `${Math.floor(totalMinutes * 0.25)}m`;
                    q2Text = `${Math.floor(totalMinutes * 0.50)}m`;
                    q3Text = `${Math.floor(totalMinutes * 0.75)}m`;
                }
                const endText = totalMinutes ? `${totalMinutes}m` : (durationStr ? `${durationStr}m` : '');

                const tick1X = (svgW * 0.25).toFixed(1);
                const tick2X = (svgW * 0.50).toFixed(1);
                const tick3X = (svgW * 0.75).toFixed(1);

                return `
                    <div class="match-chart-col">
                        <div class="match-chart-header">
                            <span class="match-chart-title">Win Probability</span>
                            <span class="match-chart-val" style="color: ${lastColor}; font-weight: 800;">${leadText}</span>
                        </div>
                        <div class="match-sparkline-wrap" title="Win Probability Timeline">
                            <svg viewBox="0 0 ${svgW} ${svgH}" preserveAspectRatio="none" class="match-sparkline-svg">
                                <line x1="0" y1="${baselineY}" x2="${svgW}" y2="${baselineY}" stroke="#334155" stroke-width="1.2"/>
                                <line x1="${tick1X}" y1="${baselineY}" x2="${tick1X}" y2="${baselineY + 2.5}" stroke="#334155" stroke-width="1"/>
                                <line x1="${tick2X}" y1="${baselineY}" x2="${tick2X}" y2="${baselineY + 2.5}" stroke="#334155" stroke-width="1"/>
                                <line x1="${tick3X}" y1="${baselineY}" x2="${tick3X}" y2="${baselineY + 2.5}" stroke="#334155" stroke-width="1"/>
                                <path d="${blueAreas.join(' ')}" fill="rgba(56, 189, 248, 0.25)"/>
                                <path d="${redAreas.join(' ')}" fill="rgba(248, 113, 113, 0.25)"/>
                                <path d="${blueLines.join(' ')}" fill="none" stroke="#38bdf8" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
                                <path d="${redLines.join(' ')}" fill="none" stroke="#f87171" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
                                <circle cx="${lastPt.x.toFixed(1)}" cy="${lastPt.y.toFixed(1)}" r="2.5" fill="${lastColor}"/>
                            </svg>
                        </div>
                        <div class="match-chart-axis">
                            <span>0m</span>
                            <span>${q1Text}</span>
                            <span>${q2Text}</span>
                            <span>${q3Text}</span>
                            <span>${endText}</span>
                        </div>
                    </div>
                `;
            }

            function renderMatchCard(m) {
                const isBlueWin = m.winner === "BLUE";
                const winnerClass = isBlueWin ? "winner-blue" : "winner-red";
                const badgeColor = isBlueWin ? "blue" : "red";
                const resultText = isBlueWin ? "BLUE VICTORY" : "RED VICTORY";

                const blueChamps = (m.blue_team || []).map(p => `
                    <div class="roster-champ-unit" title="${p.summoner_name} (${p.champion}) - KDA: ${p.kills}/${p.deaths}/${p.assists}">
                        <img src="${getChampIcon(p.champion)}" onerror="this.src='https://ddragon.leagueoflegends.com/cdn/14.20.1/img/profileicon/29.png'">
                    </div>
                `).join('');

                const redChamps = (m.red_team || []).map(p => `
                    <div class="roster-champ-unit" title="${p.summoner_name} (${p.champion}) - KDA: ${p.kills}/${p.deaths}/${p.assists}">
                        <img src="${getChampIcon(p.champion)}" onerror="this.src='https://ddragon.leagueoflegends.com/cdn/14.20.1/img/profileicon/29.png'">
                    </div>
                `).join('');

                const bKills = m.metrics ? (m.metrics.blue_kills || 0) : 0;
                const rKills = m.metrics ? (m.metrics.red_kills || 0) : 0;
                const bGold = m.metrics ? (m.metrics.blue_gold_k || '0K') : '0K';
                const rGold = m.metrics ? (m.metrics.red_gold_k || '0K') : '0K';
                const bTowers = m.metrics ? (m.metrics.blue_towers || 0) : 0;
                const rTowers = m.metrics ? (m.metrics.red_towers || 0) : 0;
                const bDrakes = m.metrics ? (m.metrics.blue_dragons || 0) : 0;
                const rDrakes = m.metrics ? (m.metrics.red_dragons || 0) : 0;

                return `
                    <div class="match-card ${winnerClass}">
                        <div class="match-meta-col">
                            <div class="match-result-badge ${badgeColor}">${resultText}</div>
                            <div class="match-mode">${m.game_mode_label || m.game_mode}</div>
                            <div class="match-time-info">
                                <span>${m.date_formatted}</span>
                                <span>Duration: ${m.duration}</span>
                            </div>
                        </div>

                        <div class="match-scores-col">
                            <div class="scores-main">
                                <span style="color: #38bdf8;">${bKills}</span>
                                <span style="color: #64748b; font-size: 14px;">VS</span>
                                <span style="color: #f87171;">${rKills}</span>
                            </div>
                            <div class="scores-gold">
                                <img src="/static/icons/gold_coin.png" style="width: 13px; height: 15px; object-fit: contain;" alt="Gold">
                                <span>${bGold} - ${rGold}</span>
                            </div>
                            <div class="scores-objectives">
                                <span>Towers: ${bTowers} - ${rTowers}</span>
                                &bull;
                                <span>Dragons: ${bDrakes} - ${rDrakes}</span>
                            </div>
                        </div>

                        <div class="match-teams-col">
                            <div class="team-roster">
                                <div class="team-roster-label blue">BLUE</div>
                                <div class="roster-champs">${blueChamps}</div>
                            </div>
                            <div class="team-roster">
                                <div class="team-roster-label red">RED</div>
                                <div class="roster-champs">${redChamps}</div>
                            </div>
                        </div>

                        ${renderMatchSparkline(m.win_prob_history, m.duration, m.winner)}

                        <div class="match-action-col">
                            <button class="btn-details" onclick="openMatchDetails('${m.session_id}')">
                                Details
                            </button>
                        </div>
                    </div>
                `;
            }

            async function openMatchDetails(sessionId) {
                try {
                    const resp = await fetch('/api/v1/matches/' + sessionId);
                    if (!resp.ok) throw new Error("Match details not found");
                    const m = await resp.json();

                    document.getElementById("modal-title").innerText = `${m.game_mode_label || m.game_mode} - ${m.winner === 'BLUE' ? 'Blue Victory' : 'Red Victory'}`;
                    document.getElementById("modal-meta").innerHTML = `
                        <span>${m.date_formatted}</span> &bull; Duration: <strong>${m.duration}</strong> &bull; 
                        Gold: <strong style="color: #38bdf8;">${m.metrics.blue_gold_k}</strong> vs <strong style="color: #f87171;">${m.metrics.red_gold_k}</strong> &bull; 
                        Kills: <strong style="color: #38bdf8;">${m.metrics.blue_kills}</strong> vs <strong style="color: #f87171;">${m.metrics.red_kills}</strong>
                    `;

                    const contentEl = document.getElementById("modal-content");
                    if (m.matchups && m.matchups.length > 0) {
                        contentEl.innerHTML = `
                            <div class="scoreboard-container" style="max-width: 100%; margin-top: 10px;">
                                ${m.matchups.map((row, idx) => renderRow(row, idx === 0)).join('')}
                            </div>
                        `;
                        fitPlayerNames(contentEl);
                    } else {
                        contentEl.innerHTML = `<div style="text-align: center; padding: 40px; color: #94a3b8;">No detailed matchup breakdown available for this session.</div>`;
                    }

                    matchModal.style.display = "flex";
                } catch (err) {
                    showToast("Failed to load match details: " + err, false);
                }
            }

            function closeModalDirect() {
                matchModal.style.display = "none";
            }

            function closeModal(e) {
                if (e.target === matchModal) {
                    matchModal.style.display = "none";
                }
            }

            // Background idle auto-detection poller for browser
            let idlePollTimer = null;

            async function checkIdleStatus() {
                if (isLiveGameActive) return;
                try {
                    const resp = await fetch('/api/v1/live/status');
                    if (!resp.ok) return;
                    const data = await resp.json();

                    if (data && data.active) {
                        isLiveGameActive = true;
                        showToast(`Auto-detected live LoL match (${data.game_time || '00:00'})! Connecting to live HUD...`, true);
                        switchView('live', true);
                    }
                } catch (e) {
                    // Silently wait for next poll
                }
            }

            function startIdlePolling() {
                if (idlePollTimer) clearInterval(idlePollTimer);
                idlePollTimer = setInterval(checkIdleStatus, 2500);
            }

            // Initialize on page load
            async function initPage() {
                try {
                    const resp = await fetch('/api/v1/live/status');
                    const data = await resp.json();

                    if (data && data.active) {
                        switchView('live', true);
                    } else {
                        switchView('history');
                    }
                } catch (e) {
                    switchView('history');
                }
                startIdlePolling();
            }

            if (document.readyState === 'loading') {
                window.addEventListener("DOMContentLoaded", initPage);
            } else {
                initPage();
            }
        </script>
    </body>
    </html>
    """


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("services.engine.src.main:app", host="0.0.0.0", port=8000, reload=True)
