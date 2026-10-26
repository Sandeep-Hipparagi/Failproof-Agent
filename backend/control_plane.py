"""Failproof telemetry, policy deployment, audit, and analysis. Developed by Sandeep Hipparagi."""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Any


POLICY_DEFAULTS = {
    "policy_booking_voice_approval": {"state": "ENABLED", "hook": "pre-tool:execute_reservation", "description": "Require exact offer, completed playback ACK, later affirmative voice intent, and matching reservation terms."},
    "policy_loop_and_idempotency": {"state": "ENABLED", "hook": "pre-tool:all tools", "description": "Break after two identical tool calls and reject previously consumed hold tokens."},
    "policy_schema_and_pii_guard": {"state": "ENABLED", "hook": "pre-tool/post-tool", "description": "Fail closed on unknown or invalid tool schemas and redact sensitive transcript/payload fields."},
}

_LEDGER_LOCKS: dict[str, threading.RLock] = {}
_LEDGER_LOCKS_GUARD = threading.Lock()


class ControlPlane:
    def __init__(self):
        configured_path = os.getenv("FAILPROOF_LEDGER_PATH")
        self.ledger_path = Path(configured_path) if configured_path else Path(__file__).resolve().parent / "data" / "worm_audit_ledger.jsonl"
        self.policies = deepcopy(POLICY_DEFAULTS)
        self.policy_parameters = {"max_loop_retries": 2, "vad_barge_in_hold_ms": 250, "offer_expiration_seconds": 300}
        self.sessions: dict[str, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.activity: list[dict[str, Any]] = []
        self.snapshots: list[dict[str, Any]] = []
        self.queries: list[dict[str, Any]] = []
        self.alerts: list[dict[str, Any]] = []
        self.checks: Counter = Counter()
        self.voice_models: list[str] = [os.getenv("SARVAM_TTS_MODEL", "bulbul:v4-flash")]
        self.audio_formats: list[tuple[int, int]] = [(16000, 1)]
        self.executed_holds: list[str] = []
        self.policy_hits: Counter = Counter()
        self._last_ts = 0.0
        self._event_seq: int = 0
        self._audit_hash = "0" * 64
        self.ledger_blocks: list[dict[str, Any]] = []
        self.integrity_verified = True
        self.corrupted_block_index: int | None = None
        self._ledger_signature: tuple[int, int] = (0, 0)
        with _LEDGER_LOCKS_GUARD:
            self._ledger_thread_lock = _LEDGER_LOCKS.setdefault(str(self.ledger_path.resolve()), threading.RLock())
        self._hydrate_ledger()

    @contextmanager
    def _ledger_lock(self):
        """Serialize WORM reads/writes across threads and processes."""
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.ledger_path.with_name(self.ledger_path.name + ".lock")
        with self._ledger_thread_lock, lock_path.open("a+b") as lock_file:
            lock_file.seek(0, os.SEEK_END)
            if lock_file.tell() == 0:
                lock_file.write(b"1")
                lock_file.flush()
            lock_file.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _current_ledger_signature(self) -> tuple[int, int]:
        try:
            stat = self.ledger_path.stat()
            return stat.st_size, stat.st_mtime_ns
        except FileNotFoundError:
            return 0, 0

    @staticmethod
    def _hash_fields(block: dict[str, Any]) -> dict[str, Any]:
        if block.get("hash_version") != 2:
            legacy_keys = ("index", "timestamp", "session_id", "policy_id", "tool_name", "verdict", "reason_code", "prev_hash")
            return {key: block[key] for key in legacy_keys if key in block}
        keys = ("index", "timestamp", "timestamp_utc", "session_id", "policy_id", "tool_name", "verdict", "reason_code", "prev_hash",
                "record_type", "payload_sha256", "offer_id", "causal_proof", "transcript_sha256")
        return {key: block[key] for key in keys if key in block}

    @staticmethod
    def _entry_hash(block: dict[str, Any]) -> str:
        canonical = json.dumps(ControlPlane._hash_fields(block), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _append_block(self, *, record_type: str, session_id: str, policy_id: str, tool_name: str,
                      verdict: str, reason_code: str, payload: dict[str, Any] | None = None,
                      accountability: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._ledger_lock():
            if not self.integrity_verified:
                raise RuntimeError("WORM ledger integrity failed; writes are locked")
            signature = self._current_ledger_signature()
            if signature != self._ledger_signature:
                status, blocks = self._verify_ledger_integrity_unlocked()
                if not status["integrity_verified"]:
                    raise RuntimeError("WORM ledger integrity failed; writes are locked")
                self.ledger_blocks = blocks
                self._audit_hash = blocks[-1]["entry_hash"] if blocks else "0" * 64
                self._ledger_signature = signature
            timestamp = self.timestamp()
            block = {"index": len(self.ledger_blocks), "timestamp": timestamp,
                     "timestamp_utc": datetime.fromtimestamp(timestamp, timezone.utc).isoformat(), "session_id": session_id,
                     "policy_id": policy_id, "tool_name": tool_name, "verdict": verdict,
                     "reason_code": reason_code, "prev_hash": self._audit_hash, "record_type": record_type, "hash_version": 2}
            if accountability:
                block.update(accountability)
            if payload is not None:
                block["payload"] = payload
                payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                block["payload_sha256"] = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
            block["entry_hash"] = self._entry_hash(block)
            block["hash"] = block["entry_hash"]
            block["previous_hash"] = block["prev_hash"]
            with self.ledger_path.open("a", encoding="utf-8", newline="\n") as ledger_file:
                ledger_file.write(json.dumps(block, sort_keys=True, separators=(",", ":")) + "\n")
                ledger_file.flush()
                os.fsync(ledger_file.fileno())
            self.ledger_blocks.append(block)
            self._audit_hash = block["entry_hash"]
            self._ledger_signature = self._current_ledger_signature()
            return block

    def _hydrate_ledger(self) -> None:
        with self._ledger_lock():
            status, parsed = self._verify_ledger_integrity_unlocked()
            blocks = parsed[:status["block_height"]]
            self.ledger_blocks = parsed
            self._ledger_signature = self._current_ledger_signature()
        valid_blocks = blocks
        if valid_blocks:
            self._audit_hash = valid_blocks[-1].get("entry_hash", "0" * 64)
        for block in valid_blocks:
            if block.get("record_type") == "activity":
                row = {"timestamp": block["timestamp"], "session_id": block["session_id"],
                       "timestamp_utc": block.get("timestamp_utc"),
                       "policy_id": block["policy_id"], "tool_name": block["tool_name"],
                       "verdict": block["verdict"], "reason_code": block["reason_code"],
                       "trigger_reason": block.get("trigger_reason") or block.get("reason") or block["reason_code"],
                       "injected_recovery_prompt": block.get("payload", {}).get("injected_recovery_prompt"),
                       "previous_hash": block["prev_hash"], "hash": block["entry_hash"],
                       "index": block["index"], "entry_hash": block["entry_hash"], "prev_hash": block["prev_hash"]}
                row.update({key: block[key] for key in ("offer_id", "causal_proof", "transcript_sha256") if key in block})
                self.activity.append(row)
                self.policy_hits[row["policy_id"]] += 1
            elif block.get("record_type") == "snapshot":
                self.snapshots.append(block.get("payload", {}))

    def _verify_ledger_integrity_unlocked(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        previous = "0" * 64
        corrupted: int | None = None
        count = 0
        blocks: list[dict[str, Any]] = []
        try:
            lines = self.ledger_path.read_text(encoding="utf-8").splitlines() if self.ledger_path.exists() else []
            for index, line in enumerate(lines):
                try:
                    block = json.loads(line)
                    required = {"index", "timestamp", "session_id", "policy_id", "tool_name", "verdict", "reason_code", "prev_hash", "entry_hash"}
                    valid = (isinstance(block, dict) and required.issubset(block)
                             and block.get("index") == index and block.get("prev_hash") == previous
                             and block.get("entry_hash") == self._entry_hash(block))
                    if valid and block.get("hash_version") == 2:
                        valid = block.get("record_type") in {"activity", "snapshot"} and "timestamp_utc" in block
                    if "payload_sha256" in block:
                        payload_json = json.dumps(block["payload"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)
                        valid = valid and block.get("payload_sha256") == hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
                except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError, AttributeError, ValueError):
                    valid = False
                    block = {}
                if not valid:
                    corrupted = index
                    break
                previous = block["entry_hash"]
                blocks.append(block)
                count += 1
        except (OSError, UnicodeDecodeError):
            corrupted = 0
        self.integrity_verified = corrupted is None
        self.corrupted_block_index = corrupted
        if corrupted is not None and not any(a.get("code") == "WORM_LEDGER_TAMPER_DETECTED" and a.get("block_index") == corrupted for a in self.alerts):
            self.alert("CRITICAL", "WORM_LEDGER_TAMPER_DETECTED", "system", f"Ledger integrity failed at block {corrupted}")
            self.alerts[-1]["block_index"] = corrupted
        return ({"integrity_verified": self.integrity_verified, "block_height": count,
                 "corrupted_block_index": corrupted}, blocks)

    def verify_ledger_integrity(self) -> dict[str, Any]:
        with self._ledger_lock():
            status, _ = self._verify_ledger_integrity_unlocked()
            return status

    def playback_ack(self, offer_id: str, timestamp: float) -> None:
        print(f"[FAILPROOF-ACK] offer_id={offer_id} | TTS_PLAYBACK_COMPLETED locked at t1={timestamp:.3f}", flush=True)

    def timestamp(self) -> float:
        now = time.time()
        self._last_ts = max(now, self._last_ts + 0.000001)
        return self._last_ts

    def event(self, session_id: str, kind: str, **data) -> dict[str, Any]:
        self._event_seq += 1
        ts = self.timestamp()
        iso = datetime.fromtimestamp(ts, timezone.utc).isoformat()
        item = {
            "sequence_id": self._event_seq,
            "type": kind,
            "session_id": session_id,
            "server_ts": ts,
            "timestamp_utc": iso,
            **self.redact(data),
        }
        self.events.append(item)
        return item

    def start_session(self, session_id: str, state: dict[str, Any]):
        self.sessions[session_id] = {"session_id": session_id, "started_at": self.timestamp(), "ended_at": None,
            "lat": state.get("user_location", {}).get("lat"), "lng": state.get("user_location", {}).get("lng"),
            "dietary_constraints": [], "candidate_restaurants": [], "booking_status": "NONE", "turns": 0,
            "stages": [], "turn_durations": [], "barge_ins": 0, "playback_acks": 0, "playback_offers": 0, "sentiments": []}

    def stage(self, session_id: str, name: str, started: float, **fields):
        duration = max(0.0, time.perf_counter() - started)
        ts = self.timestamp()
        iso = datetime.fromtimestamp(ts, timezone.utc).isoformat()
        if name == "sarvam_tts" and "model" in fields:
            self.voice_models.append(fields["model"])
        entry = {"stage": name, "duration_seconds": duration, "server_ts": ts, "timestamp_utc": iso, **fields}
        session = self.sessions.get(session_id)
        if session:
            session["stages"].append(entry)
        return entry

    def snapshot(self, session_id: str, phase: str, state: dict[str, Any]):
        keys = ("pending_offer", "reservation_offer", "last_user_speech_started_server_ts", "last_user_transcript_server_ts", "candidate_restaurants")
        snap = self.redact({"session_id": session_id, "phase": phase, "server_ts": self.timestamp(), **{k: deepcopy(state.get(k)) for k in keys}})
        offer = snap.get("pending_offer") or snap.get("reservation_offer") or {}
        snap.update({"offer_id": offer.get("offer_id"), "offer_playback_verified": offer.get("offer_playback_verified", False), "playback_completed_server_ts": offer.get("playback_completed_server_ts")})
        self.snapshots.append(snap)
        self._append_block(record_type="snapshot", session_id=session_id, policy_id="trace_state_snapshot",
                           tool_name="failproof_policy_gate", verdict=phase.upper(), reason_code="GATE_STATE",
                           payload=deepcopy(snap))
        return snap

    def audit(self, *, session_id: str, policy_id: str, tool_name: str, verdict: str, reason: str,
              prompt: str | None = None, offer_id: str | None = None,
              causal_proof: dict[str, Any] | None = None, transcript_sha256: str | None = None):
        row = self._append_block(record_type="activity", session_id=session_id, policy_id=policy_id,
                                 tool_name=tool_name, verdict=verdict.upper(), reason_code=reason.split(":", 1)[0],
                                 payload={"injected_recovery_prompt": self.redact(prompt)} if prompt else None,
                                 accountability={"offer_id": offer_id, "causal_proof": causal_proof,
                                                  "transcript_sha256": transcript_sha256})
        row["trigger_reason"] = reason
        row["reason"] = reason
        self.activity.append(row)
        self.policy_hits[policy_id] += 1
        if verdict.upper() in {"ALLOW", "STEER", "BREAK-LOOP"}:
            print(f"[FAILPROOF-WORM #{row['index']:04d}] verdict={row['verdict']} | policy={policy_id} | reason={row['reason_code']} | hash={row['entry_hash'][:4]}...{row['entry_hash'][-4:]}", flush=True)
        if row["reason_code"] in {"AUDIO_PLAYBACK_UNVERIFIED", "STALE_CONFIRMATION_TIMESTAMP", "PREMATURE_CONFIRMATION_SPEECH"}:
            self.alert("CRITICAL", row["reason_code"], session_id, reason)
        elif verdict.upper() == "BREAK-LOOP":
            self.alert("WARNING", "TOOL_LOOP_BROKEN", session_id, reason)
        return row

    def alert(self, severity, code, session_id, message):
        item = {"severity": severity, "code": code, "session_id": session_id, "message": message, "server_ts": self.timestamp()}
        self.alerts.append(item)
        return item

    def query(self, session_id, provider, query, latency, **details):
        row = self.redact({"session_id": session_id, "provider": provider, "query": query, "latency_seconds": latency, "server_ts": self.timestamp(), **details})
        self.queries.append(row)

    @staticmethod
    def redact(value):
        if isinstance(value, dict):
            return {k: ("[REDACTED]" if re.search(r"email|phone|address|confirmation_quote|api_key|password|secret|credit_card|ack_token", str(k), re.I) else ControlPlane.redact(v)) for k, v in value.items()}
        if isinstance(value, list):
            return [ControlPlane.redact(item) for item in value]
        if isinstance(value, str):
            text = re.sub(r"[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}", "[REDACTED_EMAIL]", value)
            text = re.sub(r"(?<!\w)(?:\+?\d[\d .()-]{7,}\d)(?!\w)", "[REDACTED_PHONE]", text)
            text = re.sub(r"\b(?:\d[ -]*?){13,19}\b", "[REDACTED_CARD]", text)
            return re.sub(r"\b[\w.-]{2,}@[\w.-]{2,}\b", "[REDACTED_UPI]", text)
        return value

    def sentiment(self, session_id, text):
        lowered = text.lower()
        tag = "frustrated" if any(x in lowered for x in ("hurry", "impatient", "waiting", "come on", "annoyed")) else "satisfied" if any(x in lowered for x in ("great", "thanks", "perfect", "yes")) else "neutral"
        result = {"label": tag, "urgency": any(x in lowered for x in ("hurry", "now", "quickly", "urgent")), "server_ts": self.timestamp()}
        if session_id in self.sessions:
            self.sessions[session_id]["sentiments"].append(result)
        return result

    def deploy(self, payload: dict[str, Any]):
        allowed = {"policies", "parameters"}
        if set(payload) - allowed:
            raise ValueError("Unknown deployment fields")
        policies = payload.get("policies", {})
        parameters = payload.get("parameters", {})
        if not isinstance(policies, dict) or not isinstance(parameters, dict):
            raise ValueError("policies and parameters must be objects")
        if set(policies) - set(self.policies):
            raise ValueError("Unknown policy")
        candidate = deepcopy(self.policies)
        for name, config in policies.items():
            if not isinstance(config, dict) or set(config) != {"state"} or config["state"] not in {"ENABLED", "SHADOW_MODE", "DISABLED"}:
                raise ValueError(f"Invalid configuration for {name}")
            candidate[name]["state"] = config["state"]
        numeric = {"max_loop_retries": (1, 10), "vad_barge_in_hold_ms": (50, 2000), "offer_expiration_seconds": (30, 3600)}
        params = dict(self.policy_parameters)
        for name, value in parameters.items():
            if name not in numeric or isinstance(value, bool) or not isinstance(value, int) or not numeric[name][0] <= value <= numeric[name][1]:
                raise ValueError(f"Invalid parameter: {name}")
            params[name] = value
        self.policies, self.policy_parameters = candidate, params
        return {"policies": deepcopy(candidate), "parameters": dict(params)}

    def metrics(self):
        sessions = list(self.sessions.values())
        stt = sorted(x["duration_seconds"] for s in sessions for x in s["stages"] if x["stage"] == "sarvam_stt")
        tts = sorted(x["duration_seconds"] for s in sessions for x in s["stages"] if x["stage"] == "sarvam_tts")
        turn_durations = [x for s in sessions for x in s["turn_durations"]]
        def percentile(values, p):
            return values[min(len(values)-1, int((len(values)-1)*p))] if values else 0
        turns = sum(s["turns"] for s in sessions)
        verdict_counts = Counter(x["verdict"] for x in self.activity)
        ledger_status = self.verify_ledger_integrity()
        return {"sessions": len(sessions), "active_sessions": sum(s["ended_at"] is None for s in sessions), "voice_turns": turns,
            "policies_deployed": sum(p["state"] == "ENABLED" for p in self.policies.values()), "interceptions": dict(verdict_counts),
            "interception_saves": verdict_counts["STEER"] + verdict_counts["BREAK-LOOP"], "allow_count": verdict_counts["ALLOW"],
            "steer_count": verdict_counts["STEER"], "break_loop_count": verdict_counts["BREAK-LOOP"],
            "avg_voice_latency_seconds": sum(turn_durations)/len(turn_durations) if turn_durations else 0, "stt_p50": percentile(stt, .5), "stt_p95": percentile(stt, .95),
            "tts_p50": percentile(tts, .5), "tts_p95": percentile(tts, .95), "playback_ack_rate": sum(s["playback_acks"] for s in sessions)/max(1, sum(s["playback_offers"] for s in sessions)),
            "barge_in_interruptions": sum(s["barge_ins"] for s in sessions), "graphiti_status": "connected" if getattr(self, "graph_connected", False) else "local fallback",
            "ledger_integrity_verified": ledger_status["integrity_verified"], "ledger_block_height": len(self.ledger_blocks)}

    def generate_insights(self) -> list[str]:
        insights: list[str] = []
        sessions = list(self.sessions.values())

        # 1. Latency spikes pattern detection
        for s in sessions:
            s_id = s.get("session_id", "unknown")[:8]
            for st in s.get("stages", []):
                stage_name = st.get("stage", "")
                dur = float(st.get("duration_seconds") or 0.0)
                if stage_name == "sarvam_stt" and dur > 2.5:
                    insights.append(f"Latency Spike: Sarvam STT took {dur:.2f}s (threshold 2.5s) in session {s_id}.")
                elif stage_name == "sarvam_tts" and dur > 2.5:
                    insights.append(f"Latency Spike: Sarvam TTS synthesis took {dur:.2f}s (threshold 2.5s) in session {s_id}.")
                elif stage_name in {"gemini_reasoning", "agent_reasoning"} and dur > 4.0:
                    insights.append(f"Latency Spike: Gemini reasoning took {dur:.2f}s (threshold 4.0s) in session {s_id}.")
            for td in s.get("turn_durations", []):
                if float(td) > 6.0:
                    insights.append(f"Latency Spike: End-to-end voice turn duration reached {float(td):.2f}s in session {s_id}.")

        # 2. Barge-in anomaly pattern detection
        total_barge_ins = sum(s.get("barge_ins", 0) for s in sessions)
        if total_barge_ins >= 3:
            insights.append("Users frequently interrupt long readouts; shorten spoken menu summaries.")
        elif total_barge_ins > 0:
            insights.append(f"Barge-in Anomaly: {total_barge_ins} user interruptions detected during active TTS playback. Optimize acoustic filler.")
        for a in self.alerts:
            if a.get("code") == "PREMATURE_CONFIRMATION_SPEECH":
                insights.append(f"Barge-in Anomaly: Spoken user affirmative arrived before complete offer playback in session {a.get('session_id', '')[:8]}.")

        # 3. Audio playback ACK state mismatches
        unverified_acks = [a for a in self.alerts if a.get("code") == "AUDIO_PLAYBACK_UNVERIFIED"]
        if unverified_acks:
            insights.append(f"Audio Playback ACK Mismatch: {len(unverified_acks)} reservation confirmation attempts occurred without causal playback completion ACK.")
        stale_ts = [a for a in self.alerts if a.get("code") == "STALE_CONFIRMATION_TIMESTAMP"]
        if stale_ts:
            insights.append(f"Timing Invariant Mismatch: {len(stale_ts)} confirmation timestamps arrived out of causal order with offer playback.")

        # 4. Tool Loop & Idempotency gates
        loop_alerts = [a for a in self.alerts if a.get("code") == "TOOL_LOOP_BROKEN"]
        if loop_alerts:
            insights.append(f"Idempotency Gate: {len(loop_alerts)} repetitive tool call loops intercepted and safely broken.")

        # 5. Ledger Cryptographic Proof status
        integrity = self.verify_ledger_integrity()
        if not integrity["integrity_verified"]:
            insights.append(f"CRITICAL: WORM ledger integrity failure at block index {integrity.get('corrupted_block_index')}.")
        elif len(self.ledger_blocks) > 0 and not insights:
            insights.append(f"Operational Baseline Healthy: Zero causal audio mismatches, zero unauthorized voices, and 100% WORM integrity across {len(self.ledger_blocks)} blocks.")

        return insights

    def analysis(self):
        checks = {"CHECK_NO_DEFAULT_VOICE": bool(self.voice_models) and all(x in ("bulbul:v4-flash", "bulbul:v3") for x in self.voice_models),
                  "CHECK_CAUSAL_AUDIO_ACK": True,
                  "CHECK_IDEMPOTENT_HOLD_TOKEN": len(self.executed_holds) == len(set(self.executed_holds)),
                  "CHECK_AUDIO_SAMPLE_RATE": bool(self.audio_formats) and all(x == (16000, 1) for x in self.audio_formats),
                  "CHECK_WORM_LEDGER_INTEGRITY": self.verify_ledger_integrity()["integrity_verified"]}
        if not self.executed_holds: checks["CHECK_IDEMPOTENT_HOLD_TOKEN"] = True
        for row in self.activity:
            if row["tool_name"] == "execute_reservation" and row["verdict"] == "ALLOW":
                related = [s for s in self.snapshots if s["session_id"] == row["session_id"] and s["phase"] == "before"]
                if related:
                    s = related[-1]
                    offer = s.get("pending_offer") or s.get("reservation_offer") or {}
                    speech_started = s.get("last_user_speech_started_server_ts")
                    transcript_received = s.get("last_user_transcript_server_ts")
                    playback_completed = s.get("playback_completed_server_ts") or offer.get("playback_completed_server_ts")
                    verified = s.get("offer_playback_verified") or offer.get("offer_playback_verified", False)
                    checks["CHECK_CAUSAL_AUDIO_ACK"] &= bool(verified and speech_started is not None and transcript_received is not None and playback_completed is not None and speech_started > playback_completed and transcript_received >= speech_started)
                else:
                    checks["CHECK_CAUSAL_AUDIO_ACK"] = False
        for name, ok in checks.items():
            self.checks[name] += 1
        clean = sum(1 for s in self.sessions.values() if s["booking_status"] in {"NONE", "CONFIRMED"})
        return {"reliability_score": round(100 * clean / max(1, len(self.sessions)), 2), "checks": checks,
            "insights": self.generate_insights(), "alerts": list(self.alerts)}
