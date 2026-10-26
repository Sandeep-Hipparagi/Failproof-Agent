"""Durable WORM ledger recovery and tamper detection. Developed by Sandeep Hipparagi."""
from backend.control_plane import ControlPlane


def _record_two_events(cp: ControlPlane):
    cp.audit(session_id="session-1", policy_id="policy_booking_voice_approval",
             tool_name="execute_reservation", verdict="STEER",
             reason="AUDIO_PLAYBACK_UNVERIFIED: playback has not completed")
    cp.audit(session_id="session-1", policy_id="policy_booking_voice_approval",
             tool_name="execute_reservation", verdict="ALLOW", reason="Policy checks passed")


def test_cold_restart_restores_activity_hashes_and_counters(tmp_path, monkeypatch):
    ledger_path = tmp_path / "worm_audit_ledger.jsonl"
    monkeypatch.setenv("FAILPROOF_LEDGER_PATH", str(ledger_path))
    original = ControlPlane()
    _record_two_events(original)
    expected_hashes = [row["entry_hash"] for row in original.activity]

    restored = ControlPlane()
    assert len(restored.activity) == 2
    assert [row["entry_hash"] for row in restored.activity] == expected_hashes
    assert restored.metrics()["interceptions"] == {"STEER": 1, "ALLOW": 1}
    assert restored.policy_hits["policy_booking_voice_approval"] == 2
    assert restored._audit_hash == expected_hashes[-1]


def test_chain_verification_after_reload(tmp_path, monkeypatch):
    ledger_path = tmp_path / "worm_audit_ledger.jsonl"
    monkeypatch.setenv("FAILPROOF_LEDGER_PATH", str(ledger_path))
    _record_two_events(ControlPlane())
    restored = ControlPlane()
    result = restored.verify_ledger_integrity()
    assert result == {"integrity_verified": True, "block_height": 2, "corrupted_block_index": None}


def test_tamper_detection_marks_failed_and_locks_writes(tmp_path, monkeypatch):
    ledger_path = tmp_path / "worm_audit_ledger.jsonl"
    monkeypatch.setenv("FAILPROOF_LEDGER_PATH", str(ledger_path))
    _record_two_events(ControlPlane())
    text = ledger_path.read_text(encoding="utf-8")
    assert '"verdict":"STEER"' in text
    ledger_path.write_text(text.replace('"verdict":"STEER"', '"verdict":"ALLOW"', 1), encoding="utf-8")

    tampered = ControlPlane()
    result = tampered.verify_ledger_integrity()
    assert result["integrity_verified"] is False
    assert result["corrupted_block_index"] == 0
    assert tampered.analysis()["checks"]["CHECK_WORM_LEDGER_INTEGRITY"] is False
    assert any(alert["code"] == "WORM_LEDGER_TAMPER_DETECTED" for alert in tampered.alerts)

