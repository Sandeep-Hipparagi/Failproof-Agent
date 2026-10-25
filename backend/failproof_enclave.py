"""Deterministic runtime interception for high-risk tools and loops. Developed by Sandeep Hipparagi."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any

AFFIRMATIVE = re.compile(r"\b(yes|yeah|yep|confirm|confirmed|book it|lock it in|go ahead|make the reservation)\b", re.I)
NEGATED = re.compile(r"\b(no|not|don't|do not|never|cancel|wait|stop)\b", re.I)


@dataclass
class Enclave:
    """Session-scoped policies. Never infers consent from silence or model output."""
    calls: dict[str, dict[tuple[str, str], int]] = field(default_factory=dict)
    booked_holds: set[str] = field(default_factory=set)
    last_verdict: str = "allow"

    @staticmethod
    def signature(tool_name: str, args: dict[str, Any]) -> tuple[str, str]:
        raw = json.dumps(args, sort_keys=True, separators=(",", ":"), default=str)
        return tool_name, hashlib.sha256(raw.encode()).hexdigest()

    def intercept(self, *, session_id: str, tool_name: str, args: dict[str, Any],
                  state: dict[str, Any]) -> dict[str, Any]:
        sig = self.signature(tool_name, args)
        session_calls = self.calls.setdefault(session_id, {})
        session_calls[sig] = session_calls.get(sig, 0) + 1
        if session_calls[sig] > 2:
            self.last_verdict = "break-loop"
            return {"verdict": "break-loop", "reason": "Repeated identical tool call limit exceeded"}
        if tool_name == "execute_reservation":
            hold = str(args.get("hold_token", ""))
            if hold and hold in self.booked_holds:
                self.last_verdict = "break-loop"
                return {"verdict": "break-loop", "reason": "Reservation hold token was already committed"}
            offer = state.get("pending_offer") or {}
            terms = ("place_id", "time_slot", "party_size", "hold_token")
            if not offer or any(str(offer.get(k, "")) != str(args.get(k, "")) for k in terms):
                return self._steer("OFFER_MISMATCH: Reservation arguments do not match the active offer.",
                                   "The requested reservation does not match the active offer. Recheck availability and present the exact terms again.")
            if offer.get("offer_playback_verified") is not True or offer.get("playback_completed_server_ts") is None:
                return self._steer("AUDIO_PLAYBACK_UNVERIFIED: User interrupted or has not heard the complete reservation terms.",
                                   "The user has not heard the complete reservation offer audio yet (playback was incomplete or interrupted). Do NOT call execute_reservation. Repeat the restaurant name, party size, and time slot clearly and wait for the user to confirm after you finish speaking.")
            completed_ts = offer["playback_completed_server_ts"]
            transcript_ts = state.get("last_user_transcript_server_ts")
            if transcript_ts is None or transcript_ts <= completed_ts:
                return self._steer("STALE_CONFIRMATION_TIMESTAMP: Confirmation transcript did not arrive after full offer playback.",
                                   "The confirmation arrived before verified offer playback completed. Repeat the complete offer and wait for a new spoken confirmation after playback.")
            transcript = str(state.get("last_user_transcript", ""))
            if not AFFIRMATIVE.search(transcript) or NEGATED.search(transcript):
                return self._steer("EXPLICIT_AFFIRMATION_REQUIRED: No unambiguous affirmative confirmation found.",
                                   "The user has not clearly confirmed. Ask for an unambiguous verbal yes after stating the exact reservation terms.")
        self.last_verdict = "allow"
        return {"verdict": "allow", "reason": "Policy checks passed"}

    def _steer(self, reason: str, prompt: str) -> dict[str, Any]:
        self.last_verdict = "steer"
        return {"verdict": "steer", "policy": "policy_booking_voice_approval", "reason": reason,
                "inject_prompt": f"SYSTEM ENCLAVE INTERCEPTION: {prompt}"}

    def committed(self, hold_token: str) -> None:
        self.booked_holds.add(hold_token)
        self.last_verdict = "allow"


def validate_tool_args(name: str, args: dict[str, Any]) -> tuple[bool, str]:
    """Small fail-closed schema gate before policy evaluation/execution."""
    schemas = {
        "discover_nearby_restaurants": {"lat": (int, float), "lng": (int, float), "cuisine": str},
        "verify_google_reviews": {"place_id": str},
        "inspect_menu_and_dietary_fit": {"place_id": str, "dietary_constraints": list},
        "check_table_availability": {"place_id": str, "time_slot": str, "party_size": int},
        "execute_reservation": {"place_id": str, "restaurant_name": str, "time_slot": str, "party_size": int, "hold_token": str, "user_confirmation_quote": str},
    }
    schema = schemas.get(name)
    if schema is None:
        return False, "Unknown tool"
    for key, kind in schema.items():
        value = args.get(key)
        if not isinstance(value, kind) or (kind is str and not value.strip()):
            return False, f"Invalid or missing argument: {key}"
    return True, ""
