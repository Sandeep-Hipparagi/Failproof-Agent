"""Two-act local WebSocket rehearsal for playback interception and recovery. Developed by Sandeep Hipparagi."""
from __future__ import annotations

import sys
import secrets
import time
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

from backend import main
from backend.agent_harness import _make_offer
from backend.failproof_enclave import Enclave
from backend.tools.restaurant_tools import check_table_availability, execute_reservation


class RehearsalHarness:
    """Deterministic model/STT stand-in; endpoint, event gate, enclave, tool, and memory are real."""

    def __init__(self, memory: object) -> None:
        self.enclave = Enclave()
        self.memory = memory
        self.booking_args: dict[str, object] = {}

    async def run_turn(self, state: dict) -> dict:
        transcript = state["last_user_transcript"].casefold()
        if state.get("pending_offer") is None:
            hold = await check_table_availability("seed_mtr_lalbagh", "8:00 PM", 2)
            details = {"place_id": "seed_mtr_lalbagh", "time_slot": "8:00 PM", "party_size": 2, **hold,
                       "restaurant_name": "MTR (Lalbagh Road)"}
            state["pending_reservation"] = details
            state["pending_offer"] = _make_offer(state, details)
            _prepare_offer_audio(state["pending_offer"])
            await state["emit"]({"type": "audio", "text": "MTR (Lalbagh Road), party of 2, at 8:00 PM. Shall I book it?", "audio": "", "offer_id": state["pending_offer"]["offer_id"], "requires_playback_ack": True, "playback_ack_token": state["pending_offer"]["playback_ack_token"]})
            return state

        args = {"place_id": "seed_mtr_lalbagh", "restaurant_name": "MTR (Lalbagh Road)",
                "time_slot": "8:00 PM", "party_size": 2,
                "hold_token": state["pending_offer"]["hold_token"],
                "user_confirmation_quote": state["last_user_transcript"]}
        verdict = self.enclave.intercept(session_id=state["session_id"], tool_name="execute_reservation", args=args, state=state)
        await state["emit"]({"type": "enclave", **verdict})
        if verdict["verdict"] != "allow":
            # Re-state the same terms and preserve the offer ID; only playback completion can unlock it.
            _prepare_offer_audio(state["pending_offer"])
            await state["emit"]({"type": "audio", "text": "MTR (Lalbagh Road), party of 2, at 8:00 PM. Shall I book it?", "audio": "", "offer_id": state["pending_offer"]["offer_id"], "requires_playback_ack": True, "playback_ack_token": state["pending_offer"]["playback_ack_token"]})
            return state
        result = await execute_reservation(**args)
        assert result["confirmed"], result
        self.enclave.committed(args["hold_token"])
        await self.memory.record_booking(state["user_id"], args["restaurant_name"], result["booking_reference"])
        self.booking_args = args
        await state["emit"]({"type": "tool", "name": "execute_reservation", "result": result})
        return state


def _until(ws, predicate, *, limit: int = 20) -> list[dict]:
    messages = []
    for _ in range(limit):
        message = ws.receive_json()
        messages.append(message)
        if predicate(message):
            return messages
    raise AssertionError(f"Expected WebSocket event not received; saw {messages!r}")


def _prepare_offer_audio(offer: dict) -> None:
    offer["offer_playback_verified"] = False
    offer["playback_completed_server_ts"] = None
    offer["playback_ack_received"] = False
    offer["playback_ack_pending"] = True
    offer["playback_ack_token"] = secrets.token_urlsafe(24)


def run_rehearsal() -> None:
    transcripts = iter(("Find a South Indian restaurant in Bengaluru", "Yes, book it right now", "Yes, confirm the table for two at 8 PM"))

    async def fake_transcribe(_pcm: bytes, _sample_rate: int = 16000) -> str:
        return next(transcripts)

    rehearsal = RehearsalHarness(main.memory)
    with patch.object(main.sarvam, "transcribe", fake_transcribe), patch.object(main, "harness", rehearsal):
        client = TestClient(main.app)
        with client.websocket_connect("/ws") as ws:
            ws.send_json({"type": "SET_CONTEXT", "user_id": "smoke-browser-user"})
            ws.receive_json()
            # Act 1: request discovery, then interrupt the offer and speak a premature yes.
            ws.send_json({"type": "USER_SPEAKING_START"})
            ws.send_bytes(b"simulated pcm")
            ws.send_json({"type": "AUDIO_END"})
            first = _until(ws, lambda item: item.get("type") == "audio" and item.get("offer_id"))
            offer_id = next(item["offer_id"] for item in first if item.get("type") == "audio")
            ack_token = next(item["playback_ack_token"] for item in first if item.get("type") == "audio")

            ws.send_json({"type": "TTS_PLAYBACK_INTERRUPTED", "offer_id": offer_id, "playback_ack_token": ack_token, "client_ts": int(time.time() * 1000)})
            _until(ws, lambda item: item.get("type") == "playback_verification" and item.get("status") == "interrupted")
            ws.send_json({"type": "USER_SPEAKING_START", "interruption": True})
            ws.send_bytes(b"simulated pcm")
            ws.send_json({"type": "AUDIO_END"})
            act1 = _until(ws, lambda item: item.get("type") == "audio" and item.get("offer_id") == offer_id)
            ack_token = next(item["playback_ack_token"] for item in act1 if item.get("type") == "audio")
            steers = [item for item in act1 if item.get("type") == "enclave" and item.get("verdict") == "steer"]
            assert steers and steers[0]["reason"].startswith("AUDIO_PLAYBACK_UNVERIFIED"), act1
            assert not any(item.get("type") == "tool" and item.get("name") == "execute_reservation" for item in act1), act1

            # Act 2: full replay acknowledgment, then a later affirmative transcript.
            ws.send_json({"type": "TTS_PLAYBACK_COMPLETED", "offer_id": offer_id, "playback_ack_token": ack_token, "client_ts": int(time.time() * 1000)})
            _until(ws, lambda item: item.get("type") == "playback_verification" and item.get("status") == "verified")
            time.sleep(0.05)
            ws.send_json({"type": "USER_SPEAKING_START"})
            ws.send_bytes(b"simulated pcm")
            ws.send_json({"type": "AUDIO_END"})
            act2 = _until(ws, lambda item: item.get("type") == "tool" and item.get("name") == "execute_reservation")
            assert any(item.get("type") == "enclave" and item.get("verdict") == "allow" for item in act2), act2
            booking = next(item["result"] for item in act2 if item.get("type") == "tool")
            assert booking.get("confirmed") and booking.get("booking_reference"), booking
            memory_facts = main.memory.local.get(rehearsal.booking_args.get("user_id", ""), [])
            # Memory uses the websocket session ID; inspect all active local episodes for the BOOKED_AT edge.
            all_facts = [fact for episodes in main.memory.local.values() for fact in episodes]
            assert any("BOOKED_AT" in fact and "MTR (Lalbagh Road)" in fact for fact in all_facts), memory_facts

    print("PASS: interrupted offer steered; completed playback plus later affirmative allowed booking; memory recorded BOOKED_AT")


def test_smoke_live_loop() -> None:
    run_rehearsal()


if __name__ == "__main__":
    run_rehearsal()
