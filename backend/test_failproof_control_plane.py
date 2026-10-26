"""Control plane policy, telemetry, and invariant tests. Developed by Sandeep Hipparagi."""
import asyncio

from fastapi.testclient import TestClient

from backend.control_plane import ControlPlane
from backend.failproof_enclave import Enclave
from backend.main import app, control, enclave
from backend.agent_harness import AgentHarness


def test_policy_inventory_deploy_and_fail_closed_validation():
    client = TestClient(app)
    inventory = client.get("/api/fix/overview-policies").json()
    assert {p["id"] for p in inventory["policies"]} == {
        "policy_booking_voice_approval", "policy_loop_and_idempotency", "policy_schema_and_pii_guard"}
    assert client.post("/api/fix/policies/deploy", json={"policies": {"policy_loop_and_idempotency": {"state": "SHADOW_MODE"}}, "parameters": {"max_loop_retries": 2}}).status_code == 200
    bad = client.post("/api/fix/policies/deploy", json={"policies": {"missing": {"state": "DISABLED"}}})
    assert bad.status_code == 422
    assert enclave.inspect_policies()[1]["state"] == "SHADOW_MODE"
    assert any(e["type"] == "POLICY_DEPLOYMENT_FAILED" for e in control.events)
    # Restore global deployment state for the rest of the suite.
    control.policies = {k: {**v, "state": "ENABLED"} for k, v in control.policies.items()}


def test_worm_audit_hash_chain_and_alerts(tmp_path, monkeypatch):
    monkeypatch.setenv("FAILPROOF_LEDGER_PATH", str(tmp_path / "audit.jsonl"))
    cp = ControlPlane()
    first = cp.audit(session_id="s", policy_id="policy_booking_voice_approval", tool_name="execute_reservation", verdict="steer", reason="AUDIO_PLAYBACK_UNVERIFIED: no ACK")
    second = cp.audit(session_id="s", policy_id="policy_loop_and_idempotency", tool_name="search", verdict="break-loop", reason="repeated")
    assert second["previous_hash"] == first["hash"]
    assert first["hash"] != second["hash"]
    assert [x["severity"] for x in cp.alerts] == ["CRITICAL", "WARNING"]


def test_trace_snapshots_events_sentiment_queries_and_invariants(tmp_path, monkeypatch):
    monkeypatch.setenv("FAILPROOF_LEDGER_PATH", str(tmp_path / "trace.jsonl"))
    async def run():
        cp = ControlPlane()
        cp.start_session("s", {"user_location": {"lat": 1.0, "lng": 2.0}})
        cp.event("s", "USER_SPEAKING_START")
        cp.event("s", "STT_FINAL_TRANSCRIPT", text="hurry, I am waiting")
        assert [e["server_ts"] for e in cp.events] == sorted(e["server_ts"] for e in cp.events)
        assert cp.sentiment("s", "hurry, I am waiting")["urgency"]
        cp.query("s", "gemini-2.5-flash", '{"tool":"search"}', .02)
        assert cp.queries[0]["provider"] == "gemini-2.5-flash"
        class Memory:
            client = None
            async def retrieve_preferences(self, _): return {}
            async def ingest_episode(self, *_): pass
        class Voice:
            async def synthesize(self, _): return b""
        enc = Enclave(control_plane=cp)
        harness = AgentHarness(Memory(), enc, Voice())
        state = {"session_id": "s", "pending_offer": {"offer_id": "o", "offer_playback_verified": True,
                 "playback_completed_server_ts": 10}, "last_user_transcript_server_ts": 11, "candidate_restaurants": [], "pending_calls": []}
        cp.snapshot("s", "before", state)
        cp.snapshot("s", "after", state)
        assert cp.snapshots[0]["offer_id"] == "o"
        assert set(cp.analysis()["checks"]) == {"CHECK_NO_DEFAULT_VOICE", "CHECK_CAUSAL_AUDIO_ACK", "CHECK_IDEMPOTENT_HOLD_TOKEN", "CHECK_AUDIO_SAMPLE_RATE", "CHECK_WORM_LEDGER_INTEGRITY"}
    asyncio.run(run())


def test_audio_and_tool_policy_invariants():
    cp = ControlPlane()
    cp.start_session("s", {"user_location": {}})
    enc = Enclave(control_plane=cp)
    state = {"pending_offer": {"place_id": "p", "time_slot": "8", "party_size": 2, "hold_token": "h", "offer_playback_verified": True, "playback_completed_server_ts": 4},
             "last_user_transcript_server_ts": 5, "last_user_transcript": "Yes"}
    args = {"place_id": "p", "restaurant_name": "Cafe", "time_slot": "8", "party_size": 2, "hold_token": "h", "user_confirmation_quote": "Yes"}
    assert enc.intercept(session_id="s", tool_name="execute_reservation", args=args, state=state)["verdict"] == "allow"
    enc.committed("h")
    assert enc.intercept(session_id="s", tool_name="execute_reservation", args=args, state=state)["verdict"] == "break-loop"

