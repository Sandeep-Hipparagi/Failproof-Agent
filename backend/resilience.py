"""Bounded asynchronous retries for external service calls. Developed by Sandeep Hipparagi."""
from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")
_circuit_open_until: dict[str, float] = {}


class CircuitOpenError(RuntimeError):
    """Raised while a repeatedly failing dependency is in cooldown."""


async def resilient_call(operation: Callable[[], Awaitable[T]], *, timeout: float = 4.0,
                         attempts: int = 2, service: str | None = None) -> T:
    """Retry transient failures with jitter within one strict overall deadline."""
    if service and _circuit_open_until.get(service, 0) > time.monotonic():
        raise CircuitOpenError(f"Circuit open for {service}")
    async def run() -> T:
        last_error: Exception | None = None
        for attempt in range(max(1, attempts)):
            try:
                result = await operation()
                if service:
                    _circuit_open_until.pop(service, None)
                return result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                if attempt + 1 < attempts:
                    await asyncio.sleep(min(0.35, 0.08 * (2**attempt)) + random.uniform(0, 0.04))
        assert last_error is not None
        if service:
            _circuit_open_until[service] = time.monotonic() + 10.0
        raise last_error

    try:
        return await asyncio.wait_for(run(), timeout=timeout)
    except asyncio.TimeoutError:
        if service:
            _circuit_open_until[service] = time.monotonic() + 10.0
        raise
