"""Graphiti temporal memory adapter; degraded demo mode remains explicit. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import asyncio
import hashlib
import os
from collections.abc import Callable
from typing import Any

from .resilience import resilient_call

class GraphMemory:
    def __init__(self) -> None:
        self.client = None
        self.group_id = os.getenv("GRAPHITI_GROUP_ID", "failproof-concierge")
        self.local: dict[str, list[str]] = {}
        self.on_degraded: Callable[[str, str], None] | None = None
        self._write_tasks: set[asyncio.Task] = set()
        try:
            from graphiti_core import Graphiti  # type: ignore
            uri = os.getenv("NEO4J_URI")
            if uri:
                self.client = Graphiti(uri, os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "failproof"))
        except Exception:
            self.client = None

    def _group_for_user(self, user_id: str) -> str:
        """Keep Graphiti searches isolated by browser memory identity."""
        digest = hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:24]
        return f"{self.group_id}-{digest}"

    async def retrieve_preferences(self, user_id: str, query: str = "restaurant preferences") -> dict[str, Any]:
        if self.client:
            try:
                group_id = self._group_for_user(user_id)
                result = await resilient_call(lambda: self.client.search(query, group_ids=[group_id]), timeout=4.0, attempts=2, service="graphiti")
                facts = [getattr(x, "fact", str(x)) for x in result]
                return {"facts": facts, "active": True, "relationships": self._classify(facts)}
            except Exception as exc:
                if self.on_degraded:
                    self.on_degraded("GRAPHITI_FALLBACK", f"Graphiti unavailable; local memory fallback active ({type(exc).__name__})")
        facts = self.local.get(user_id, [])
        return {"facts": facts, "active": bool(facts), "relationships": self._classify(facts)}

    async def ingest_episode(self, user_id: str, text: str) -> None:
        self.local.setdefault(user_id, []).append(text)
        if self.client:
            task = asyncio.create_task(self._write_episode(user_id, text))
            self._write_tasks.add(task)
            task.add_done_callback(self._write_tasks.discard)

    async def _write_episode(self, user_id: str, text: str) -> None:
        try:
            group_id = self._group_for_user(user_id)
            await resilient_call(lambda: self.client.add_episode(name=f"conversation-{user_id}", episode_body=text, source="message", source_description="voice concierge conversation", group_id=group_id), timeout=4.0, attempts=2, service="graphiti")
        except Exception as exc:
            if self.on_degraded:
                self.on_degraded("GRAPHITI_FALLBACK", f"Graphiti episode write failed ({type(exc).__name__})")

    async def record_booking(self, user_id: str, restaurant: str, reference: str) -> None:
        await self.ingest_episode(user_id, f"PAST_BOOKING: user {user_id} BOOKED_AT {restaurant}; reference {reference}")

    async def close(self) -> None:
        if self._write_tasks:
            await asyncio.gather(*self._write_tasks, return_exceptions=True)
        if self.client:
            close = getattr(self.client, "close", None)
            if close:
                await close()

    @staticmethod
    def _classify(facts: list[str]) -> dict[str, list[str]]:
        categories = {"PREFERS_CUISINE": [], "DIETARY_RESTRICTION": [], "DISLIKES_AMBIENCE": [], "PAST_BOOKING": []}
        for fact in facts:
            upper = fact.upper()
            for key in categories:
                if key in upper or (key == "PAST_BOOKING" and "BOOKED_AT" in upper): categories[key].append(fact)
        return categories
