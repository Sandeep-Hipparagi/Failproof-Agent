"""LangGraph-backed Gemini tool loop with concurrent acoustic filler. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import asyncio
import base64
import hashlib
import inspect
import json
import os
import time
from typing import Any, TypedDict

from .failproof_enclave import Enclave, validate_tool_args
from .graph_memory import GraphMemory
from .sarvam_client import SarvamClient
from .tools.restaurant_tools import TOOL_FUNCTIONS

class ConciergeState(TypedDict, total=False):
    session_id: str
    user_id: str
    user_location: dict[str, float]
    candidate_restaurants: list[dict[str, Any]]
    pending_reservation: dict[str, Any] | None
    voice_approval_verified: bool
    last_user_transcript: str
    reservation_offer: dict[str, Any]
    pending_offer: dict[str, Any] | None
    transcript_received_server_ts: float | None
    last_user_transcript_server_ts: float | None
    messages: list[dict[str, Any]]
    memory: dict[str, Any]
    response: str
    tool_results: list[dict[str, Any]]

class AgentHarness:
    def __init__(self, memory: GraphMemory, enclave: Enclave, sarvam: SarvamClient):
        self.memory, self.enclave, self.sarvam = memory, enclave, sarvam
        self.model = None
        try:
            from google import genai
            self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
            self.model = "gemini-2.5-flash"
        except Exception:
            self.client = None
        self.graph = self._build_graph()

    def _build_graph(self):
        try:
            from langgraph.graph import StateGraph, START, END
            graph = StateGraph(dict)
            graph.add_node("ingest_voice_and_memory", self.ingest_voice_and_memory)
            graph.add_node("agent_reasoning", self.agent_reasoning)
            graph.add_node("failproof_policy_gate", self.failproof_policy_gate)
            graph.add_node("tool_execution", self.tool_execution)
            graph.add_node("tts_response_and_graph_commit", self.tts_response_and_graph_commit)
            graph.add_edge(START, "ingest_voice_and_memory")
            graph.add_edge("ingest_voice_and_memory", "agent_reasoning")
            graph.add_conditional_edges("agent_reasoning", self._route_reasoning, {"tools": "failproof_policy_gate", "done": "tts_response_and_graph_commit"})
            graph.add_conditional_edges("failproof_policy_gate", self._route_gate, {"execute": "tool_execution", "done": "tts_response_and_graph_commit"})
            graph.add_edge("tool_execution", "agent_reasoning")
            graph.add_edge("tts_response_and_graph_commit", END)
            return graph.compile()
        except Exception:
            return None

    @staticmethod
    def _route_reasoning(state): return "tools" if state.get("pending_calls") else "done"
    @staticmethod
    def _route_gate(state): return "execute" if state.get("pending_calls") else "done"

    async def ingest_voice_and_memory(self, state: dict[str, Any]) -> dict[str, Any]:
        state["memory"] = await self.memory.retrieve_preferences(state.get("user_id", state["session_id"]))
        state.setdefault("messages", []).append({"role": "user", "content": state.get("last_user_transcript", "")})
        state.setdefault("candidate_restaurants", [])
        state.setdefault("pending_reservation", None)
        state.setdefault("voice_approval_verified", False)
        return state

    async def agent_reasoning(self, state: dict[str, Any]) -> dict[str, Any]:
        if not self.client:
            state["response"] = "I’m ready to help find a restaurant. Add GEMINI_API_KEY to enable the live concierge model."
            state["pending_calls"] = []
            return state
        from google.genai import types
        declarations = [_function_declaration(name, fn) for name, fn in TOOL_FUNCTIONS.items()]
        config = types.GenerateContentConfig(system_instruction=_system_prompt(state), tools=[types.Tool(function_declarations=declarations)], automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True), thinking_config=types.ThinkingConfig(thinking_budget=0))
        contents = state["messages"]
        response = await asyncio.to_thread(self.client.models.generate_content, model=self.model, contents=contents, config=config)
        calls = []
        for part in response.candidates[0].content.parts:
            if getattr(part, "function_call", None):
                calls.append({"name": part.function_call.name, "args": dict(part.function_call.args or {})})
            elif getattr(part, "text", None):
                state["response"] = part.text
        state["pending_calls"] = calls
        state["messages"].append({"role": "model", "content": response.candidates[0].content})
        return state

    async def failproof_policy_gate(self, state: dict[str, Any]) -> dict[str, Any]:
        allowed = []
        for call in state.get("pending_calls", []):
            valid, reason = validate_tool_args(call["name"], call["args"])
            verdict = self.enclave.intercept(session_id=state["session_id"], tool_name=call["name"], args=call["args"], state=state) if valid else {"verdict": "deny", "reason": reason}
            await _emit(state, {"type": "enclave", **verdict})
            if verdict["verdict"] == "allow": allowed.append(call)
            elif verdict["verdict"] == "steer":
                state["response"] = "Please confirm by voice: " + _offer_sentence(state, call["args"])
                offer = state.get("reservation_offer", {})
                if not offer.get("presented"):
                    offer = {"restaurant_name": call["args"].get("restaurant_name"), "time_slot": call["args"].get("time_slot"), "party_size": call["args"].get("party_size"), "presented": True}
                    state["reservation_offer"] = offer
                state["messages"].append({"role": "user", "content": verdict["inject_prompt"]})
            else:
                state["response"] = "I can’t complete that action safely. Let’s try another approach."
        state["pending_calls"] = allowed
        return state

    async def tool_execution(self, state: dict[str, Any]) -> dict[str, Any]:
        calls = state.get("pending_calls", [])
        if not calls: return state
        filler_task = asyncio.create_task(self._filler(state))
        async def run(call):
            result = await TOOL_FUNCTIONS[call["name"]](**call["args"])
            if call["name"] == "discover_nearby_restaurants": state["candidate_restaurants"] = result.get("restaurants", [])
            if call["name"] == "check_table_availability" and result.get("hold_token"):
                hold = {**call["args"], **result}
                place = next((r for r in state.get("candidate_restaurants", []) if r.get("place_id") == hold["place_id"]), {})
                hold["restaurant_name"] = place.get("name", hold["place_id"])
                state["pending_reservation"] = hold
                state["pending_offer"] = _make_offer(state, hold)
            if call["name"] == "execute_reservation" and result.get("confirmed"):
                self.enclave.committed(call["args"]["hold_token"])
                await self.memory.record_booking(state.get("user_id", state["session_id"]), call["args"]["restaurant_name"], result["booking_reference"])
                state["pending_offer"] = None
            await _emit(state, {"type": "tool", "name": call["name"], "result": result})
            return {"name": call["name"], "result": result}
        results = await asyncio.gather(*(run(call) for call in calls), return_exceptions=True)
        await asyncio.gather(filler_task, return_exceptions=True)
        state["tool_results"] = [r if isinstance(r, dict) else {"error": str(r)} for r in results]
        state["messages"].append({"role": "user", "content": "Tool results: " + json.dumps(state["tool_results"], default=str)})
        state["pending_calls"] = []
        return state

    async def _filler(self, state):
        try:
            filler_text = "Let me check the menus and recent reviews around you..."
            audio = await self.sarvam.synthesize(filler_text)
            if audio:
                await _emit(state, {"type": "audio", "text": filler_text, "audio": base64.b64encode(audio).decode()})
        except Exception:
            await _emit(state, {"type": "telemetry", "message": "Sarvam filler unavailable; tool execution continues"})

    async def tts_response_and_graph_commit(self, state: dict[str, Any]) -> dict[str, Any]:
        response = (state.get("response", "") or "").strip() or "I’ve checked that for you."
        await self.memory.ingest_episode(state.get("user_id", state["session_id"]), f"User: {state.get('last_user_transcript','')}\nAssistant: {response}")
        await _emit(state, {"type": "response", "text": response})
        try:
            audio = await self.sarvam.synthesize(response)
            if audio:
                payload = {"type": "audio", "text": response, "audio": base64.b64encode(audio).decode()}
                offer = state.get("pending_offer")
                if offer and _contains_offer_terms(response, offer):
                    offer["offer_playback_verified"] = False
                    offer["playback_completed_server_ts"] = None
                    payload.update({"offer_id": offer["offer_id"], "requires_playback_ack": True})
                await _emit(state, payload)
        except Exception as exc:
            await _emit(state, {"type": "telemetry", "message": f"Sarvam voice output unavailable: {exc}"})
        return state

    async def run_turn(self, state: dict[str, Any]) -> dict[str, Any]:
        if self.graph:
            return await self.graph.ainvoke(state, config={"recursion_limit": 12})
        state = await self.ingest_voice_and_memory(state)
        for _ in range(5):
            state = await self.agent_reasoning(state)
            if not state.get("pending_calls"): break
            state = await self.failproof_policy_gate(state)
            if state.get("pending_calls"): state = await self.tool_execution(state)
            else: break
        return await self.tts_response_and_graph_commit(state)

async def _emit(state, message):
    cb = state.get("emit")
    if cb:
        result = cb(message)
        if asyncio.iscoroutine(result): await result

def _offer_sentence(state, args):
    offer = state.get("reservation_offer", {})
    return f"{offer.get('restaurant_name', args.get('restaurant_name','the restaurant'))}, {offer.get('party_size', args.get('party_size','your party'))} people, at {offer.get('time_slot', args.get('time_slot','the requested time'))}."

def _system_prompt(state):
    return ("You are a concise, voice-first Bengaluru restaurant concierge. Speak naturally and briefly. Never claim a reservation is booked unless execute_reservation confirms it. Before booking, present exact restaurant name, party size, and time, then request explicit spoken confirmation. Use check_table_availability first. Search in parallel when useful. Use dietary constraints from memory. Memory: " + json.dumps(state.get("memory", {}), default=str))

def _make_offer(state: dict[str, Any], details: dict[str, Any]) -> dict[str, Any]:
    session_id = str(state["session_id"])
    place_id = str(details["place_id"])
    time_slot = str(details["time_slot"])
    party_size = int(details["party_size"])
    hold_token = str(details["hold_token"])
    offer_id = hashlib.sha256(f"{session_id}:{place_id}:{time_slot}:{party_size}:{hold_token}".encode()).hexdigest()[:16]
    restaurant_name = str(details.get("restaurant_name") or next((r.get("name") for r in state.get("candidate_restaurants", []) if r.get("place_id") == place_id), place_id))
    return {"offer_id": offer_id, "place_id": place_id, "restaurant_name": restaurant_name, "time_slot": time_slot,
            "party_size": party_size, "hold_token": hold_token, "offer_playback_verified": False,
            "playback_completed_server_ts": None}

def _contains_offer_terms(text: str, offer: dict[str, Any]) -> bool:
    normalized = text.casefold()
    return all(str(offer.get(k, "")).casefold() in normalized for k in ("restaurant_name", "time_slot")) and str(offer.get("party_size")) in normalized

def _function_declaration(name, fn):
    from google.genai import types
    hints = inspect.signature(fn).parameters
    props = {}
    required = []
    for key, param in hints.items():
        ann = param.annotation
        typ = "ARRAY" if ann == list[str] else "INTEGER" if ann == int else "NUMBER" if ann in (float, int) else "STRING"
        props[key] = types.Schema(type=typ, items=types.Schema(type="STRING") if typ == "ARRAY" else None)
        if param.default is inspect.Parameter.empty: required.append(key)
    return types.FunctionDeclaration(name=name, description=fn.__doc__ or name, parameters=types.Schema(type="OBJECT", properties=props, required=required))
