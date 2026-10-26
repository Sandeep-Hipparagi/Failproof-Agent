"""Fast offline pre-stage integrity check. Developed by Sandeep Hipparagi."""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient

from backend.control_plane import ControlPlane
from backend.platform_bridge import PlatformBridge
from backend import main


def verify() -> None:
    prior_ledger_path = os.environ.get("FAILPROOF_LEDGER_PATH")
    with tempfile.TemporaryDirectory(prefix="failproof-stage-") as directory:
        ledger_path = Path(directory) / "stage-ledger.jsonl"
        os.environ["FAILPROOF_LEDGER_PATH"] = str(ledger_path)
        try:
            control = ControlPlane()
            control.audit(session_id="stage-session", policy_id="policy_booking_voice_approval",
                          tool_name="execute_reservation", verdict="STEER",
                          reason="AUDIO_PLAYBACK_UNVERIFIED: simulated interruption")
            control.audit(session_id="stage-session", policy_id="policy_booking_voice_approval",
                          tool_name="execute_reservation", verdict="ALLOW",
                          reason="simulated post-playback confirmation")
            assert control.verify_ledger_integrity() == {
                "integrity_verified": True, "block_height": 2, "corrupted_block_index": None}

            no_key = PlatformBridge(control, environ={})
            no_mtls = PlatformBridge(control, environ={
                "FAILPROOF_PLATFORM_API_KEY": "offline-test-key",
                "FAILPROOF_PLATFORM_INGEST_URL": "https://ingest.befailproof.ai/events",
            })
            assert no_key.health()["disabled_reason"] == "missing_api_key"
            assert no_mtls.health()["disabled_reason"] == "failproof_ingest_requires_mtls"

            async def bridge_preflight() -> None:
                await no_key.start()
                await no_mtls.start()
                assert no_key.client is None and no_mtls.client is None
                await no_key.stop()
                await no_mtls.stop()

            asyncio.run(bridge_preflight())

            with patch.object(main, "control", control), patch.object(main, "platform_bridge", no_mtls):
                with TestClient(main.app) as client:
                    overview = client.get("/api/home/overview")
                    verification = client.get("/api/fix/activity/verify")
                    health = client.get("/health")
                    assert overview.status_code == 200, overview.text
                    assert overview.json()["ledger_block_height"] == 2
                    assert verification.status_code == 200, verification.text
                    assert verification.json()["integrity_verified"] is True
                    assert verification.json()["block_height"] == 2
                    bridge_health = health.json()["platform_bridge"]
                    assert bridge_health["enabled"] is False
                    assert bridge_health["disabled_reason"] == "failproof_ingest_requires_mtls"
        finally:
            if prior_ledger_path is None:
                os.environ.pop("FAILPROOF_LEDGER_PATH", None)
            else:
                os.environ["FAILPROOF_LEDGER_PATH"] = prior_ledger_path
    print("PASS: two-act ledger, integrity endpoints, and fail-silent no-mTLS bridge preflight")


if __name__ == "__main__":
    verify()
