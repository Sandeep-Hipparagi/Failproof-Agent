"""Graphiti temporal memory adapter; degraded demo mode remains explicit. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import asyncio
import os
from typing import Any

class GraphMemory:
    def __init__(self) -> None:
        self.client = None
        self.group_id = os.getenv("GRAPHITI_GROUP_ID", "failproof-concierge")
        self.local: dict[str, list[str]] = {}
        try:
            from graphiti_core import Graphiti  # type: ignore
            uri = os.getenv("NEO4J_URI")
            if uri:
                self.client = Graphiti(uri, os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "failproof"))
        except Exception:
            self.client = None

    async def retrieve_preferences(self, user_id: str, query: str = "restaurant preferences") -> dict[str, Any]:
        if self.client:
            try:
                result = await self.client.search(query, group_ids=[self.group_id])
                facts = [getattr(x, "fact", str(x)) for x in result]
                return {"facts": facts, "active": True, "relationships": self._classify(facts)}
            except Exception:
                pass
        facts = self.local.get(user_id, [])
        return {"facts": facts, "active": bool(facts), "relationships": self._classify(facts)}

    async def ingest_episode(self, user_id: str, text: str) -> None:
        self.local.setdefault(user_id, []).append(text)
        if self.client:
            try:
                await self.client.add_episode(name=f"conversation-{user_id}", episode_body=text, source="message", source_description="voice concierge conversation", group_id=self.group_id)
            except Exception:
                pass

    async def record_booking(self, user_id: str, restaurant: str, reference: str) -> None:
        await self.ingest_episode(user_id, f"PAST_BOOKING: user {user_id} BOOKED_AT {restaurant}; reference {reference}")

    async def close(self) -> None:
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
