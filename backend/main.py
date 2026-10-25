"""FastAPI websocket application for the voice-only restaurant concierge. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from .agent_harness import AgentHarness
from .failproof_enclave import Enclave
from .graph_memory import GraphMemory
from .sarvam_client import SarvamClient

ROOT = Path(__file__).resolve().parent.parent
memory = GraphMemory()
enclave = Enclave()
sarvam = SarvamClient()
harness = AgentHarness(memory, enclave, sarvam)

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await memory.close()

app = FastAPI(title="Failproof Restaurant Concierge", lifespan=lifespan)

@app.get("/")
async def index():
    return FileResponse(ROOT / "frontend" / "index.html")

@app.get("/health")
async def health():
    return {"ok": True, "sarvam_configured": bool(sarvam.api_key), "gemini_configured": bool(harness.client), "graphiti_configured": bool(memory.client)}

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    session_id = uuid.uuid4().hex
    user_id = session_id
    state: dict[str, Any] = {"session_id": session_id, "user_id": user_id, "user_location": {"lat": 12.9716, "lng": 77.5946}, "candidate_restaurants": [], "pending_reservation": None, "pending_offer": None, "voice_approval_verified": False, "last_user_transcript": "", "last_user_transcript_server_ts": None, "transcript_received_server_ts": None, "reservation_offer": {}, "messages": [], "emit": lambda item: ws.send_json(item)}
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
            transcript = await sarvam.transcribe(pcm, 16000)
        except Exception as exc:
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
        await ws.send_json({"type": "transcript", "text": transcript})
        if not transcript.strip():
            await ws.send_json({"type": "status", "state": "Idle"}); return
        async with turn_lock:
            await harness.run_turn(state)
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
                if task and not task.done(): task.cancel()
                audio_chunks.clear()
                await ws.send_json({"type": "barge_in_ack"})
            elif kind == "AUDIO_END":
                task = asyncio.create_task(process_audio())
            elif kind == "LOCATION":
                state["user_location"] = {"lat": float(event.get("lat", 12.9716)), "lng": float(event.get("lng", 77.5946))}
            elif kind == "OFFER_PRESENTED":
                # Kept for protocol compatibility only. Client claims cannot prove playback.
                pass
            elif kind == "TTS_PLAYBACK_COMPLETED":
                offer = state.get("pending_offer")
                if offer and event.get("offer_id") == offer.get("offer_id"):
                    offer["offer_playback_verified"] = True
                    offer["playback_completed_server_ts"] = time.time()
                    await ws.send_json({"type": "playback_verification", "status": "verified", "offer_id": offer["offer_id"]})
            elif kind == "TTS_PLAYBACK_INTERRUPTED":
                offer = state.get("pending_offer")
                if offer and event.get("offer_id") == offer.get("offer_id"):
                    offer["offer_playback_verified"] = False
                    offer["playback_completed_server_ts"] = None
                    await ws.send_json({"type": "playback_verification", "status": "interrupted", "offer_id": offer["offer_id"]})
            elif kind == "SET_CONTEXT":
                state["user_id"] = str(event.get("user_id") or user_id)
    except WebSocketDisconnect:
        if task and not task.done(): task.cancel()

def _affirmative(text: str) -> bool:
    import re
    return bool(re.search(r"\b(yes|yeah|yep|confirm|confirmed|book it|lock it in|go ahead|make the reservation)\b", text, re.I))
