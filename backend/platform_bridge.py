"""Optional, privacy-minimized outbound bridge to a Failproof event ingest host."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx


def _machine_id() -> str:
    configured = os.getenv("FAILPROOF_MACHINE_ID", "").strip()
    if configured:
        return configured[:128]
    return hashlib.sha256(f"failproof-voice-concierge:{socket.gethostname()}".encode()).hexdigest()[:24]


def _event_time(value: Any = None) -> str:
    try:
        timestamp = float(value)
    except (TypeError, ValueError):
        timestamp = time.time()
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


class PlatformBridge:
    """Batch local WORM, session summaries, and health events to a configured `/events` endpoint.

    The dashboard URL is never treated as an ingest endpoint. Delivery is disabled unless both an
    HTTPS ingest URL and a scoped API key are configured. Failproof-hosted ingest additionally
    requires the organization-issued client certificate and private key (mTLS).
    """

    def __init__(self, control: Any, *, environ: dict[str, str] | None = None,
                 client: Any = None, heartbeat_seconds: float = 30.0,
                 poll_seconds: float = 1.0):
        self.control = control
        env = os.environ if environ is None else environ
        self.url = env.get("FAILPROOF_PLATFORM_INGEST_URL", "").strip()
        self.api_key = env.get("FAILPROOF_PLATFORM_API_KEY", "").strip()
        self.machine_id = _machine_id() if environ is None else env.get("FAILPROOF_MACHINE_ID", "local-concierge").strip()
        self.machine_name = env.get("FAILPROOF_MACHINE_NAME", socket.gethostname()).strip()[:128]
        self.cert_path = env.get("FAILPROOF_PLATFORM_CLIENT_CERT", "").strip()
        self.key_path = env.get("FAILPROOF_PLATFORM_CLIENT_KEY", "").strip()
        self.heartbeat_seconds = heartbeat_seconds
        self.poll_seconds = poll_seconds
        self.client = client
        self.enabled, self.disabled_reason = self._validate_config()
        self._task: asyncio.Task | None = None
        self._owns_client = False
        self._closed = asyncio.Event()
        self._block_cursor = 0
        self._event_cursor = 0
        self._session_fingerprints: dict[str, str] = {}
        self._last_heartbeat = 0.0
        self._backoff = 1.0
        self.delivery_state = "disabled" if not self.enabled else "idle"

    def _validate_config(self) -> tuple[bool, str | None]:
        if not self.api_key:
            return False, "missing_api_key"
        if not self.url:
            return False, "missing_ingest_url"
        try:
            parsed = urlsplit(self.url)
            if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                    or parsed.fragment or not parsed.path.rstrip("/").endswith("/events")):
                return False, "invalid_ingest_url"
        except ValueError:
            return False, "invalid_ingest_url"
        host = parsed.hostname.lower()
        if host == "app.befailproof.ai" or host.endswith(".app.befailproof.ai"):
            return False, "dashboard_host_is_not_ingest_host"
        if (self.cert_path and not self.key_path) or (self.key_path and not self.cert_path):
            return False, "incomplete_mtls_pair"
        if not (self.cert_path and self.key_path):
            return False, "failproof_ingest_requires_mtls"
        if self.cert_path and (not Path(self.cert_path).is_file() or not Path(self.key_path).is_file()):
            return False, "mtls_file_not_found"
        return True, None

    async def start(self) -> None:
        if not self.enabled or self._task:
            return
        self._closed = asyncio.Event()
        self._last_heartbeat = 0.0
        if self.client is None:
            cert = (self.cert_path, self.key_path) if self.cert_path else None
            try:
                self.client = httpx.AsyncClient(timeout=httpx.Timeout(5.0), cert=cert, verify=True,
                                                follow_redirects=False)
            except Exception:
                self.enabled = False
                self.disabled_reason = "client_initialization_failed"
                self.delivery_state = "disabled"
                return
            self._owns_client = True
        self.delivery_state = "connecting"
        self._task = asyncio.create_task(self._run(), name="failproof-platform-bridge")

    async def stop(self) -> None:
        self._closed.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self._owns_client and self.client is not None:
            await self.client.aclose()
            self.client = None
            self._owns_client = False

    def health(self) -> dict[str, Any]:
        return {"enabled": self.enabled, "delivery_state": self.delivery_state,
                "disabled_reason": self.disabled_reason,
                "machine_id": self.machine_id if self.enabled else None}

    def _collect(self, *, heartbeat: bool) -> tuple[dict[str, Any], int, int, dict[str, str]]:
        blocks = list(getattr(self.control, "ledger_blocks", []))
        local_events = list(getattr(self.control, "events", []))
        outgoing: list[dict[str, Any]] = []
        next_block = self._block_cursor
        next_event = self._event_cursor
        next_sessions = dict(self._session_fingerprints)

        for block in blocks[self._block_cursor:self._block_cursor + 100]:
            # Send the original signed block verbatim so the remote side can validate the chain.
            outgoing.append({"type": "worm_block", "event_id": block.get("entry_hash"),
                             "timestamp": _event_time(block.get("timestamp")), "data": block})
            next_block += 1

        fields = ("type", "session_id", "server_ts", "policy_id", "tool_name", "verdict", "reason_code",
                  "offer_id", "interruption", "fallback", "stage", "duration_seconds", "provider")
        for row in local_events[self._event_cursor:self._event_cursor + 200]:
            event = {key: row[key] for key in fields if key in row}
            if "type" not in event:
                continue
            event_id = hashlib.sha256(json.dumps(event, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            outgoing.append({"type": "session_event", "event_id": event_id,
                             "timestamp": _event_time(row.get("server_ts")), "data": event})
            next_event += 1

        sessions = getattr(self.control, "sessions", {})
        for session_id, row in sessions.items():
            summary = {key: row.get(key) for key in
                       ("turns", "booking_status", "barge_ins", "playback_acks", "playback_offers", "ended_at")}
            fingerprint = hashlib.sha256(json.dumps(summary, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
            if next_sessions.get(session_id) != fingerprint:
                sid = hashlib.sha256(session_id.encode()).hexdigest()[:24]
                outgoing.append({"type": "session_summary", "event_id": f"{sid}:{fingerprint[:24]}",
                                 "timestamp": _event_time(row.get("ended_at") or row.get("started_at")),
                                 "session_id": sid, "data": summary})
                next_sessions[session_id] = fingerprint

        if heartbeat:
            try:
                metrics = self.control.metrics()
                health = {key: metrics.get(key) for key in
                          ("active_sessions", "voice_turns", "playback_ack_rate", "barge_in_interruptions",
                           "graphiti_status", "ledger_integrity_verified", "ledger_block_height")}
            except Exception:
                health = {"ledger_integrity_verified": False}
            outgoing.append({"type": "machine_heartbeat", "event_id": f"{self.machine_id}:{int(time.time())}",
                             "timestamp": _event_time(), "machine_id": self.machine_id,
                             "machine_name": self.machine_name, "status": "online", "data": health})

        body = {"schema_version": 1, "machine_id": self.machine_id, "machine_name": self.machine_name,
                "sent_at": _event_time(), "events": outgoing}
        return body, next_block, next_event, next_sessions

    async def publish_once(self, *, heartbeat: bool = False) -> bool:
        if not self.enabled or self.client is None:
            return False
        body, next_block, next_event, next_sessions = self._collect(heartbeat=heartbeat)
        if not body["events"]:
            return True
        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        batch_id = hashlib.sha256(canonical).hexdigest()
        try:
            response = await self.client.post(self.url, json=body, headers={
                "Authorization": f"Bearer {self.api_key}",
                "Idempotency-Key": batch_id,
                "X-Failproof-Machine-ID": self.machine_id,
            })
            response.raise_for_status()
        except Exception:
            self.delivery_state = "retrying"
            return False
        self._block_cursor = next_block
        self._event_cursor = next_event
        self._session_fingerprints = next_sessions
        self._backoff = 1.0
        self.delivery_state = "connected"
        return True

    async def _run(self) -> None:
        while not self._closed.is_set():
            heartbeat = time.monotonic() - self._last_heartbeat >= self.heartbeat_seconds
            delivered = await self.publish_once(heartbeat=heartbeat)
            if delivered and heartbeat:
                self._last_heartbeat = time.monotonic()
            delay = self.poll_seconds if delivered else self._backoff
            self._backoff = min(30.0, self._backoff * 2) if not delivered else 1.0
            try:
                await asyncio.wait_for(self._closed.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
