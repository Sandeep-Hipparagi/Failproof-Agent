"""FastAPI websocket application for the voice-only restaurant concierge. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from .agent_harness import AgentHarness
from .failproof_enclave import Enclave
from .graph_memory import GraphMemory
from .sarvam_client import SarvamClient
from .control_plane import ControlPlane
from .tools.restaurant_tools import release_tool_session, set_degraded_callback, set_http_client

ROOT = Path(__file__).resolve().parent.parent
memory = GraphMemory()
enclave = Enclave()
sarvam = SarvamClient()
harness = AgentHarness(memory, enclave, sarvam)
control = ControlPlane()
enclave.control_plane = control
control.graph_connected = bool(memory.client)
control.voice_models.append(sarvam.tts_model)
memory.on_degraded = lambda code, message: control.alert("WARNING", code, "system", message)
set_degraded_callback(lambda code, message: control.alert("WARNING", code, "system", message))
session_registry: dict[str, dict[str, Any]] = {}
session_registry_lock = asyncio.Lock()

@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient(timeout=httpx.Timeout(4.0), limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)) as shared_client:
        sarvam.http_client = shared_client
        set_http_client(shared_client)
        try:
            if sarvam.api_key:
                try:
                    await sarvam.synthesize("Checking the menus and latest reviews around you...")
                except Exception as exc:
                    control.alert("WARNING", "SARVAM_FILLER_PREWARM_FAILED", "system", type(exc).__name__)
            yield
        finally:
            sarvam.http_client = None
            set_http_client(None)
            await memory.close()

app = FastAPI(title="Failproof Restaurant Concierge", lifespan=lifespan)

@app.get("/")
async def index():
    return FileResponse(ROOT / "frontend" / "index.html")

@app.get("/health")
async def health():
    return {"ok": True, "sarvam_configured": bool(sarvam.api_key), "gemini_configured": bool(harness.client), "graphiti_configured": bool(memory.client)}

@app.get("/api/home")
@app.get("/api/home/overview")
@app.get("/api/trace/dashboards")
async def dashboard_state():
    control.graph_connected = bool(memory.client)
    return control.metrics()

@app.get("/api/fix/overview-policies")
async def policy_inventory():
    return {"policies": enclave.inspect_policies(), "parameters": control.policy_parameters}

@app.post("/api/fix/policies/deploy")
async def deploy_policies(payload: dict[str, Any]):
    try:
        result = control.deploy(payload)
    except (TypeError, ValueError) as exc:
        control.event("system", "POLICY_DEPLOYMENT_FAILED", error=str(exc))
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    control.event("system", "POLICY_DEPLOYED", deployment=result)
    return result

@app.get("/api/fix/activity")
async def activity_log():
    return {"activity": control.activity}

@app.get("/api/fix/activity/verify")
async def verify_activity_ledger():
    return control.verify_ledger_integrity()

@app.get("/api/trace/overview")
async def trace_overview():
    return {"sessions": list(control.sessions.values()), "events": control.events, "snapshots": control.snapshots, "queries": control.queries}

@app.get("/api/trace/sessions")
async def trace_sessions():
    return {"sessions": list(control.sessions.values())}

@app.get("/api/trace/live-events")
async def trace_live_events():
    return {"events": control.events}

@app.get("/api/trace/snapshots")
async def trace_snapshots():
    return {"snapshots": control.snapshots}

@app.get("/api/trace/queries")
async def trace_queries():
    return {"queries": control.queries}

@app.get("/api/trace/sentiment")
async def trace_sentiment():
    return {"sentiments": [item for s in control.sessions.values() for item in s["sentiments"]]}

@app.get("/api/analyse/overview")
@app.get("/api/analyse/insights")
@app.get("/api/analyse/checks")
@app.get("/api/analyse/alerts")
async def analysis_state():
    return control.analysis()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    session_id = uuid.uuid4().hex
    user_id = session_id
    state: dict[str, Any] = {"session_id": session_id, "user_id": user_id, "user_location": {"lat": 12.9716, "lng": 77.5946}, "candidate_restaurants": [], "pending_reservation": None, "pending_offer": None, "voice_approval_verified": False, "last_user_transcript": "", "last_user_transcript_server_ts": None, "transcript_received_server_ts": None, "reservation_offer": {}, "messages": [], "emit": lambda item: ws.send_json(item)}
    control.start_session(session_id, state)
    now = time.monotonic()
    async with session_registry_lock:
        expired = [key for key, value in session_registry.items() if now - value["connected_at"] > 1800]
        for key in expired:
            session_registry.pop(key, None)
            enclave.release_session(key)
            release_tool_session(key)
        session_registry[session_id] = {"state": state, "connected_at": now}
    await ws.send_json({"type": "home_metrics", "metrics": control.metrics()})
    async def emit(item):
        kinds = {"status": None, "transcript": "STT_FINAL_TRANSCRIPT", "tool_requested": "TOOL_CALL_REQUESTED", "audio": "TTS_CHUNK_STREAMED", "enclave": "ENCLAVE_VERDICT"}
        if kinds.get(item.get("type")):
            live = control.event(session_id, kinds[item["type"]], **{k:v for k,v in item.items() if k not in {"type", "audio"}})
            await ws.send_json({"type": "live_event", "event": live})
        if item.get("type") != "tool_requested":
            await ws.send_json(item)
        await ws.send_json({"type": "home_metrics", "metrics": control.metrics()})
    state["emit"] = emit
    audio_chunks: list[bytes] = []
    turn_lock = asyncio.Lock()
    task: asyncio.Task | None = None

    async def process_audio():
        nonlocal audio_chunks
        pcm = b"".join(audio_chunks)
        audio_chunks = []
        if not pcm: return
        await ws.send_json({"type": "status", "state": "Verifying Tools"})
        try:
            started = time.perf_counter()
            control.audio_formats.append((16000, 1))
            transcript = await sarvam.transcribe(pcm, 16000)
            control.stage(session_id, "sarvam_stt", started, model="saaras:v3", sample_rate=16000, channels=1)
        except Exception as exc:
            control.alert("WARNING", "SARVAM_STT_UNAVAILABLE", session_id, type(exc).__name__)
            await ws.send_json({"type": "telemetry", "message": f"Sarvam transcription unavailable: {exc}"})
            await ws.send_json({"type": "status", "state": "Idle"})
            return
        state["last_user_transcript"] = transcript
        state["transcript_received_server_ts"] = time.time()
        state["last_user_transcript_server_ts"] = state["transcript_received_server_ts"]
        state["voice_approval_verified"] = False
        # A spoken affirmative is accepted only for the exact offer already played to this session.
        offer = state.get("reservation_offer", {})
        if offer.get("presented") and _affirmative(transcript):
            state["voice_approval_verified"] = True
        live = control.event(session_id, "STT_FINAL_TRANSCRIPT", text=transcript)
        await ws.send_json({"type": "live_event", "event": live})
        await ws.send_json({"type": "transcript", "text": transcript})
        if not transcript.strip():
            await ws.send_json({"type": "status", "state": "Idle"}); return
        async with turn_lock:
            turn_started = time.perf_counter()
            await harness.run_turn(state)
            control.sessions[session_id]["turn_durations"].append(time.perf_counter() - turn_started)
        await ws.send_json({"type": "status", "state": "Idle"})

    try:
        while True:
            message = await ws.receive()
            if message.get("type") == "websocket.disconnect":
                if task and not task.done(): task.cancel()
                break
            if message.get("bytes") is not None:
                audio_chunks.append(message["bytes"])
                continue
            raw = message.get("text")
            if not raw: continue
            event = json.loads(raw)
            kind = event.get("type")
            if kind == "USER_SPEAKING_START":
                live = control.event(session_id, "USER_SPEAKING_START", interruption=bool(event.get("interruption")))
                await ws.send_json({"type": "live_event", "event": live})
                if event.get("interruption"):
                    control.sessions[session_id]["barge_ins"] += 1
                if task and not task.done(): task.cancel()
                audio_chunks.clear()
                await ws.send_json({"type": "barge_in_ack"})
            elif kind == "AUDIO_END":
                task = asyncio.create_task(process_audio())
            elif kind == "LOCATION":
                state["user_location"] = {"lat": float(event.get("lat", 12.9716)), "lng": float(event.get("lng", 77.5946))}
                control.sessions[session_id].update(state["user_location"])
            elif kind == "TTS_PLAYBACK_COMPLETED":
                offer = state.get("pending_offer")
                if offer and event.get("offer_id") == offer.get("offer_id"):
                    offer["offer_playback_verified"] = True
                    offer["playback_completed_server_ts"] = control.timestamp()
                    control.playback_ack(offer["offer_id"], offer["playback_completed_server_ts"])
                    control.sessions[session_id]["playback_acks"] += 1
                    control.sessions[session_id]["playback_offers"] += 1
                    control.event(session_id, "TTS_PLAYBACK_COMPLETED", offer_id=offer["offer_id"])
                    await ws.send_json({"type": "live_event", "event": control.events[-1]})
                    await ws.send_json({"type": "playback_verification", "status": "verified", "offer_id": offer["offer_id"]})
            elif kind == "TTS_PLAYBACK_INTERRUPTED":
                offer = state.get("pending_offer")
                if offer and event.get("offer_id") == offer.get("offer_id"):
                    offer["offer_playback_verified"] = False
                    offer["playback_completed_server_ts"] = None
                    control.sessions[session_id]["playback_offers"] += 1
                    control.event(session_id, "TTS_PLAYBACK_INTERRUPTED", offer_id=offer["offer_id"])
                    await ws.send_json({"type": "live_event", "event": control.events[-1]})
                    await ws.send_json({"type": "playback_verification", "status": "interrupted", "offer_id": offer["offer_id"]})
            elif kind == "SET_CONTEXT":
                state["user_id"] = str(event.get("user_id") or user_id)
            await ws.send_json({"type": "home_metrics", "metrics": control.metrics()})
    except WebSocketDisconnect:
        if task and not task.done(): task.cancel()
    finally:
        if task and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        control.sessions[session_id]["ended_at"] = control.timestamp()
        async with session_registry_lock:
            session_registry.pop(session_id, None)
        enclave.release_session(session_id)
        release_tool_session(session_id)
        audio_chunks.clear()
        state.clear()

def _affirmative(text: str) -> bool:
    import re
    return bool(re.search(r"\b(yes|yeah|yep|confirm|confirmed|book it|lock it in|go ahead|make the reservation)\b", text, re.I))
