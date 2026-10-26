"""Offline tests for optional authenticated hosted telemetry delivery."""
import asyncio
from types import SimpleNamespace

from backend.platform_bridge import PlatformBridge


def _mtls_config(tmp_path):
    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("test certificate", encoding="utf-8")
    key.write_text("test key", encoding="utf-8")
    return {"FAILPROOF_PLATFORM_CLIENT_CERT": str(cert), "FAILPROOF_PLATFORM_CLIENT_KEY": str(key)}


class Response:
    def __init__(self, status=202):
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError("remote rejected event batch")


class Client:
    def __init__(self, *, status=202):
        self.status = status
        self.calls = []

    async def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response(self.status)


def test_bridge_is_silent_and_disabled_without_credentials():
    bridge = PlatformBridge(SimpleNamespace(), environ={})
    assert bridge.enabled is False
    assert bridge.health()["disabled_reason"] == "missing_api_key"
    asyncio.run(bridge.start())
    assert bridge.client is None


def test_bridge_sends_redacted_session_events_and_signed_blocks(tmp_path):
    block = {"index": 0, "entry_hash": "abc123", "payload": {"record_type": "activity"}}
    control = SimpleNamespace(
        ledger_blocks=[block],
        events=[{"type": "STT_FINAL_TRANSCRIPT", "session_id": "private-session", "server_ts": 100,
                 "text": "private transcript must not leave this machine"},
                {"type": "ENCLAVE_VERDICT", "session_id": "private-session", "server_ts": 101,
                 "verdict": "STEER", "reason_code": "AUDIO_PLAYBACK_UNVERIFIED"}],
        sessions={"private-session": {"turns": 1, "booking_status": "NONE", "barge_ins": 1,
                                      "playback_acks": 0, "playback_offers": 1, "started_at": 100}},
        metrics=lambda: {"active_sessions": 1, "voice_turns": 1, "ledger_integrity_verified": True,
                         "ledger_block_height": 1})
    client = Client()
    config = {"FAILPROOF_PLATFORM_API_KEY": "test-token",
              "FAILPROOF_PLATFORM_INGEST_URL": "https://collector.example/events",
              "FAILPROOF_MACHINE_ID": "qa-machine", **_mtls_config(tmp_path)}
    bridge = PlatformBridge(control, environ=config, client=client)

    assert asyncio.run(bridge.publish_once(heartbeat=True)) is True
    url, request = client.calls[0]
    assert url == "https://collector.example/events"
    assert request["headers"]["Authorization"] == "Bearer test-token"
    assert request["headers"]["Idempotency-Key"]
    body = request["json"]
    assert body["events"][0]["data"] == block
    serialized = str(body)
    assert "private transcript must not leave this machine" not in serialized
    assert "text" not in str(next(item for item in body["events"] if item["type"] == "session_event")["data"])
    assert any(item["type"] == "machine_heartbeat" for item in body["events"])
    assert bridge.delivery_state == "connected"


def test_failed_delivery_keeps_cursors_for_retry(tmp_path):
    control = SimpleNamespace(ledger_blocks=[{"index": 0, "entry_hash": "hash"}], events=[], sessions={},
                              metrics=lambda: {})
    client = Client(status=503)
    config = {"FAILPROOF_PLATFORM_API_KEY": "test-token",
              "FAILPROOF_PLATFORM_INGEST_URL": "https://collector.example/events", **_mtls_config(tmp_path)}
    bridge = PlatformBridge(control, environ=config, client=client)

    assert asyncio.run(bridge.publish_once()) is False
    assert bridge._block_cursor == 0
    assert bridge.delivery_state == "retrying"


def test_bridge_rejects_dashboard_host_and_requires_failproof_mtls():
    dashboard = PlatformBridge(SimpleNamespace(), environ={"FAILPROOF_PLATFORM_API_KEY": "key",
        "FAILPROOF_PLATFORM_INGEST_URL": "https://app.befailproof.ai/blazingfast/events"})
    hosted_without_mtls = PlatformBridge(SimpleNamespace(), environ={"FAILPROOF_PLATFORM_API_KEY": "key",
        "FAILPROOF_PLATFORM_INGEST_URL": "https://ingest.befailproof.ai/events"})
    assert dashboard.health()["disabled_reason"] == "dashboard_host_is_not_ingest_host"
    assert hosted_without_mtls.health()["disabled_reason"] == "failproof_ingest_requires_mtls"
