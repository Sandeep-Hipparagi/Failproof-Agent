"""Seed and verify the deterministic Buildathon stage ledger. Developed by Sandeep Hipparagi."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from .control_plane import ControlPlane
    from .failproof_enclave import Enclave
    from .tools.restaurant_tools import (
        discover_nearby_restaurants,
        inspect_menu_and_dietary_fit,
        verify_google_reviews,
    )
except ImportError:
    from backend.control_plane import ControlPlane
    from backend.failproof_enclave import Enclave
    from backend.tools.restaurant_tools import (
        discover_nearby_restaurants,
        inspect_menu_and_dietary_fit,
        verify_google_reviews,
    )
SESSION_ID = "stage-demo-bengaluru-mtr-20261026"
TRANSCRIPT = "Yes, confirm the table for two at 8 PM"
FILLER_TEXT = "Checking the menus and latest reviews around you..."


def _print_environment() -> dict[str, str]:
    from dotenv import load_dotenv
    load_dotenv()
    print("\nSTAGE PREFLIGHT - CREDENTIAL PRESENCE ONLY")
    print(f"{'Dependency':<24} {'Status':<34} {'Mode'}")
    print("-" * 80)
    rows = (
        ("SARVAM_API_KEY", "Sarvam voice", "LOCAL DETERMINISTIC FALLBACK"),
        ("GEMINI_API_KEY", "Gemini reasoning", "LOCAL DETERMINISTIC FALLBACK"),
        ("GOOGLE_PLACES_API_KEY", "Google Places", "LOCAL DETERMINISTIC FALLBACK"),
        ("NEO4J_URI", "Graphiti / Neo4j", "LOCAL DETERMINISTIC FALLBACK"),
    )
    for key, label, fallback in rows:
        configured = bool(os.getenv(key))
        status = "LIVE API" if configured else fallback
        print(f"{label:<24} {'configured' if configured else 'not configured':<34} {status}")
    print("Secrets are never displayed.")
    return {k: os.getenv(k, "") for k, _, _ in rows}


async def _warm_local_read_cache() -> None:
    """Exercise local lookup paths; this process-local cache ends with the CLI."""
    previous = os.environ.pop("GOOGLE_PLACES_API_KEY", None)
    try:
        await discover_nearby_restaurants(12.9716, 77.5946, "South Indian")
        await verify_google_reviews("seed_mtr_lalbagh")
        await inspect_menu_and_dietary_fit("seed_mtr_lalbagh", ["vegetarian"])
    finally:
        if previous is not None:
            os.environ["GOOGLE_PLACES_API_KEY"] = previous
    print("Local Places, review, and menu lookup paths preflighted (deterministic dataset).")
    print("Server-process cache note: read-cache entries are process-local; the live app warms its Sarvam filler during lifespan startup.")


def _stage(cp: ControlPlane, name: str, session_id: str = SESSION_ID, **fields) -> None:
    cp.stage(session_id, name, time.perf_counter(), **fields)


def _seed_two_act_story(cp: ControlPlane) -> None:
    t1_local = datetime(2026, 10, 26, 20, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    t1 = t1_local.timestamp()
    t2 = t1 + 0.420

    class StageClock:
        def __init__(self):
            self.value = t1 - 60.0

        def now(self) -> float:
            current = self.value
            self.value += 0.001
            return current

        def set(self, value: float) -> None:
            self.value = value

    stage_clock = StageClock()
    cp.timestamp = stage_clock.now
    enclave = Enclave(control_plane=cp)
    location = {"lat": 12.9716, "lng": 77.5946}
    cp.start_session(SESSION_ID, {"user_location": location})
    session = cp.sessions[SESSION_ID]
    session["candidate_restaurants"] = [{
        "place_id": "seed_mtr_lalbagh",
        "name": "Mavalli Tiffin Rooms (MTR)",
        "rating": 4.4,
        "price_level": 2,
    }]
    session["dietary_constraints"] = ["vegetarian"]
    session["turns"] = 2
    cp.event(SESSION_ID, "STAGE_DEMO_SEEDED", location=location, restaurant="Mavalli Tiffin Rooms")

    place_id = "seed_mtr_lalbagh"
    time_slot = "20:00 IST"
    hold_token = "hold_stage_mtr_20261026"
    offer_id = hashlib.sha256(
        f"{SESSION_ID}:{place_id}:{time_slot}:2:{hold_token}".encode("utf-8")
    ).hexdigest()
    args = {
        "place_id": place_id,
        "restaurant_name": "Mavalli Tiffin Rooms (MTR)",
        "time_slot": time_slot,
        "party_size": 2,
        "hold_token": hold_token,
        "user_confirmation_quote": TRANSCRIPT,
    }
    offer = {
        "offer_id": offer_id,
        "place_id": place_id,
        "restaurant_name": "Mavalli Tiffin Rooms (MTR)",
        "time_slot": time_slot,
        "party_size": 2,
        "hold_token": hold_token,
        "offer_playback_verified": False,
        "playback_completed_server_ts": None,
    }
    state = {
        "session_id": SESSION_ID,
        "user_location": location,
        "candidate_restaurants": session["candidate_restaurants"],
        "pending_offer": offer,
        "last_user_transcript": "Wait, I didn't hear the end. Yes, book it.",
        "last_user_transcript_server_ts": 1793035200.0,
    }

    # Act 1: the client interrupts offer audio. Playback proof is deliberately absent.
    _stage(cp, "sarvam_stt", model="saaras:v4", sample_rate=16000, channels=1)
    _stage(cp, "graphiti_memory_lookup", fallback=True, source="local-memory")
    _stage(cp, "gemini_reasoning", model="gemini-2.5-flash", thinking_budget=0)
    cp.event(SESSION_ID, "TTS_PLAYBACK_INTERRUPTED", offer_id=offer_id)
    cp.event(SESSION_ID, "USER_SPEAKING_START", interruption=True)
    cp.event(SESSION_ID, "ACOUSTIC_FILLER_TRIGGERED", text=FILLER_TEXT)
    cp.event(SESSION_ID, "TOOL_CALL_REQUESTED", tool_name="discover_nearby_restaurants", source="local_bengaluru_fallback")
    session["barge_ins"] = 1
    session["playback_offers"] = 1
    cp.sentiment(SESSION_ID, state["last_user_transcript"])
    _stage(cp, "failproof_enclave_gate", verdict="STEER")
    cp.snapshot(SESSION_ID, "before", state)
    steer = enclave.intercept(session_id=SESSION_ID, tool_name="execute_reservation", args=args, state=state)
    if steer["verdict"] != "steer" or not steer["reason"].startswith("AUDIO_PLAYBACK_UNVERIFIED"):
        raise RuntimeError(f"Unexpected Act 1 result: {steer}")
    cp.audit(
        session_id=SESSION_ID,
        policy_id="policy_booking_voice_approval",
        tool_name="execute_reservation",
        verdict="STEER",
        reason=steer["reason"],
        prompt=steer.get("inject_prompt"),
        offer_id=offer_id,
        causal_proof={"playback_completed_ts": None, "transcript_received_ts": state["last_user_transcript_server_ts"], "delta_ms": None},
        transcript_sha256=hashlib.sha256(state["last_user_transcript"].encode("utf-8")).hexdigest(),
    )
    cp.event(SESSION_ID, "ENCLAVE_VERDICT", verdict="STEER", policy_id="policy_booking_voice_approval", reason_code="AUDIO_PLAYBACK_UNVERIFIED")
    cp.snapshot(SESSION_ID, "after", state)
    _stage(cp, "sarvam_tts", model="bulbul:v4-flash", replay_offer=True)

    # Act 2: use fixed Bengaluru stage timestamps and exactly +420 ms causal delta.
    offer["offer_playback_verified"] = True
    offer["playback_completed_server_ts"] = t1
    state["last_user_transcript"] = TRANSCRIPT
    state["last_user_transcript_server_ts"] = t2
    stage_clock.set(t1)
    cp.event(SESSION_ID, "TTS_PLAYBACK_COMPLETED", offer_id=offer_id, playback_completed_server_ts=t1)
    cp.playback_ack(offer_id, t1)
    session["playback_acks"] = 1
    session["playback_offers"] = 2
    stage_clock.set(t2)
    cp.event(SESSION_ID, "STT_FINAL_TRANSCRIPT", text=TRANSCRIPT, transcript_received_server_ts=t2)
    cp.sentiment(SESSION_ID, TRANSCRIPT)
    cp.query(SESSION_ID, "getzep/graphiti", "restaurant preferences", 0.001, fallback=True)
    cp.query(SESSION_ID, "gemini-2.5-flash", '{"tool":"execute_reservation","party_size":2}', 0.001)
    _stage(cp, "sarvam_stt", model="saaras:v4", sample_rate=16000, channels=1)
    _stage(cp, "graphiti_memory_lookup", fallback=True, source="local-memory")
    _stage(cp, "gemini_reasoning", model="gemini-2.5-flash", thinking_budget=0)
    cp.snapshot(SESSION_ID, "before", state)
    _stage(cp, "failproof_enclave_gate", verdict="ALLOW")
    allow = enclave.intercept(session_id=SESSION_ID, tool_name="execute_reservation", args=args, state=state)
    if allow["verdict"] != "allow":
        raise RuntimeError(f"Unexpected Act 2 result: {allow}")
    causal = {"playback_completed_ts": t1, "transcript_received_ts": t2, "delta_ms": round((t2 - t1) * 1000)}
    cp.audit(
        session_id=SESSION_ID,
        policy_id="policy_booking_voice_approval",
        tool_name="execute_reservation",
        verdict="ALLOW",
        reason=allow["reason"],
        offer_id=offer_id,
        causal_proof=causal,
        transcript_sha256=hashlib.sha256(TRANSCRIPT.encode("utf-8")).hexdigest(),
    )
    cp.event(SESSION_ID, "ENCLAVE_VERDICT", verdict="ALLOW", policy_id="policy_booking_voice_approval", causal_proof=causal)
    cp.snapshot(SESSION_ID, "after", state)
    _stage(cp, "parallel_tool_execution", tool_name="execute_reservation", result="DEMO_CONFIRMED")
    _stage(cp, "sarvam_tts", model="bulbul:v4-flash")
    _stage(cp, "client_playback_ack", offer_id=offer_id, delta_ms=420)
    enclave.committed(hold_token, SESSION_ID)
    cp.executed_holds.append(hold_token)
    session["booking_status"] = "CONFIRMED"
    session["ended_at"] = cp.timestamp()
    cp.event(SESSION_ID, "STAGE_DEMO_BOOKING_CONFIRMED", restaurant="Mavalli Tiffin Rooms (MTR)", party_size=2, time_slot=time_slot)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pre-flight checks and deterministic two-act stage seed.")
    parser.add_argument("--reset", action="store_true", help="Replace the configured WORM ledger with the deterministic stage baseline.")
    args = parser.parse_args()

    _print_environment()
    asyncio.run(_warm_local_read_cache())

    if args.reset:
        configured = os.getenv("FAILPROOF_LEDGER_PATH")
        ledger_path = Path(configured).expanduser() if configured else ROOT / "backend" / "data" / "worm_audit_ledger.jsonl"
        if ledger_path.exists():
            ledger_path.unlink()
        print(f"Reset ledger: {ledger_path}")

    control = ControlPlane()
    if not control.ledger_blocks:
        _seed_two_act_story(control)
    elif args.reset:
        raise RuntimeError("Reset ledger remained non-empty; refusing to continue.")
    else:
        print("Ledger is already populated; preserved it (pass --reset to reseed).")

    verification = control.verify_ledger_integrity()
    analysis = control.analysis()
    if not verification["integrity_verified"] or verification["block_height"] < 2:
        raise RuntimeError(f"Ledger preflight failed: {verification}")
    if not analysis["checks"].get("CHECK_CAUSAL_AUDIO_ACK") or not analysis["checks"].get("CHECK_WORM_LEDGER_INTEGRITY"):
        raise RuntimeError(f"Invariant preflight failed: {analysis['checks']}")
    steer_row = next((row for row in control.activity if row["verdict"] == "STEER"), None)
    allow_row = next((row for row in control.activity if row["verdict"] == "ALLOW" and row["tool_name"] == "execute_reservation"), None)
    if not steer_row or not allow_row:
        raise RuntimeError("Ledger does not contain both stage activity acts; run with --reset to create the golden baseline.")

    print("\nGENESIS TO HEAD - SHA-256 LEDGER")
    print(f"Genesis: {'0' * 64}")
    for block in control.ledger_blocks:
        print(f"#{block['index']:04d} {block['record_type']:<8} {block['verdict']:<6} {block['prev_hash']} -> {block['entry_hash']}")
    print(f"Head:    {control.ledger_blocks[-1]['entry_hash']}")
    print(f"integrity_verified: {verification['integrity_verified']} | block_height: {verification['block_height']}")
    print(f"Activity acts: STEER #{steer_row['index']:04d} -> ALLOW #{allow_row['index']:04d}")
    print(f"Causal invariant: CHECK_CAUSAL_AUDIO_ACK={analysis['checks']['CHECK_CAUSAL_AUDIO_ACK']} (delta +420ms)")
    print(f"WORM invariant: CHECK_WORM_LEDGER_INTEGRITY={analysis['checks']['CHECK_WORM_LEDGER_INTEGRITY']}")
    if analysis["alerts"]:
        print(f"Analyse alert: {analysis['alerts'][0]['severity']} {analysis['alerts'][0]['code']}")
    else:
        print("Analyse alert: critical STEER is in the ledger; live alert collection is process-local")
    seeded_session = control.sessions.get(SESSION_ID)
    stage_count = len(seeded_session["stages"]) if seeded_session else 0
    print(f"Trace waterfall stages in this process: {stage_count}; durable snapshots: {len(control.snapshots)}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"STAGE PREFLIGHT FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
