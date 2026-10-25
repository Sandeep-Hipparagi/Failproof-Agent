"""Focused async tests for the client playback proof and causal booking gate. Developed by Sandeep Hipparagi."""
import asyncio
import time

from backend.failproof_enclave import Enclave


def _fixture():
    enclave = Enclave()
    offer = {
        "offer_id": "a1b2c3d4e5f67890",
        "place_id": "place-1",
        "restaurant_name": "MTR",
        "time_slot": "8:00 PM",
        "party_size": 2,
        "hold_token": "hold-1",
        "offer_playback_verified": False,
        "playback_completed_server_ts": None,
    }
    state = {
        "pending_offer": offer,
        "last_user_transcript": "Yes, lock it in",
        "last_user_transcript_server_ts": time.time(),
    }
    args = {
        "place_id": "place-1",
        "restaurant_name": "MTR",
        "time_slot": "8:00 PM",
        "party_size": 2,
        "hold_token": "hold-1",
        "user_confirmation_quote": "Yes, lock it in",
    }
    return enclave, state, args


def test_booking_before_playback_ack_is_steered():
    enclave, state, args = _fixture()
    verdict = enclave.intercept(session_id="s1", tool_name="execute_reservation", args=args, state=state)
    assert verdict["verdict"] == "steer"
    assert verdict["reason"].startswith("AUDIO_PLAYBACK_UNVERIFIED")


def test_interrupted_offer_is_steered_even_with_affirmative():
    enclave, state, args = _fixture()
    state["pending_offer"]["offer_playback_verified"] = True
    state["pending_offer"]["playback_completed_server_ts"] = time.time() - 5
    # TTS_PLAYBACK_INTERRUPTED handler resets these exact fields.
    state["pending_offer"]["offer_playback_verified"] = False
    state["pending_offer"]["playback_completed_server_ts"] = None
    state["last_user_transcript"] = "Yes book it"
    verdict = enclave.intercept(session_id="s2", tool_name="execute_reservation", args=args, state=state)
    assert verdict["verdict"] == "steer"
    assert verdict["reason"].startswith("AUDIO_PLAYBACK_UNVERIFIED")


def test_post_playback_affirmative_is_allowed():
    enclave, state, args = _fixture()
    completed_at = time.time()
    state["pending_offer"]["offer_playback_verified"] = True
    state["pending_offer"]["playback_completed_server_ts"] = completed_at
    state["last_user_transcript"] = "Yes, lock it in"
    state["last_user_transcript_server_ts"] = completed_at + 0.01
    verdict = enclave.intercept(session_id="s3", tool_name="execute_reservation", args=args, state=state)
    assert verdict["verdict"] == "allow"


if __name__ == "__main__":
    for test in (test_booking_before_playback_ack_is_steered,
                 test_interrupted_offer_is_steered_even_with_affirmative,
                 test_post_playback_affirmative_is_allowed):
        test()
    asyncio.run(asyncio.sleep(0))
