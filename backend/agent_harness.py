"""LangGraph-backed Gemini tool loop with concurrent acoustic filler. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import asyncio
import base64
import hashlib
import inspect
import json
import logging
import os
import secrets
import time
from typing import Any, TypedDict, get_origin, get_type_hints

from .failproof_enclave import Enclave, validate_tool_args
from .graph_memory import GraphMemory
from .resilience import resilient_call
from .sarvam_client import SarvamClient
from .tools.restaurant_tools import TOOL_FUNCTIONS, TOOL_SESSION_ID

logger = logging.getLogger(__name__)

class ConciergeState(TypedDict, total=False):
    session_id: str
    user_id: str
    user_location: dict[str, float]
    candidate_restaurants: list[dict[str, Any]]
    pending_reservation: dict[str, Any] | None
    voice_approval_verified: bool
    last_user_transcript: str
    last_user_speech_started_server_ts: float | None
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
            if not os.getenv("GEMINI_API_KEY"):
                raise RuntimeError("GEMINI_API_KEY is not configured")
            from google import genai
            self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
            self.model = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
            logger.info("Gemini client configured model=%s", self.model)
        except Exception as exc:
            self.client = None
            logger.warning("\033[93m[LLM-REASON]\033[0m model unavailable (%s); text fallback will be used", type(exc).__name__)
        self.graph = self._build_graph()

    async def close(self) -> None:
        if self.client:
            async_client = getattr(self.client, "aio", None)
            close = getattr(async_client, "aclose", None)
            if close:
                await close()

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
        cp = getattr(self.enclave, "control_plane", None)
        started = time.perf_counter()
        state["memory"] = await self.memory.retrieve_preferences(state.get("user_id", state["session_id"]))
        if cp:
            cp.stage(state["session_id"], "graphiti_memory_lookup", started, fallback=not bool(getattr(self.memory, "client", None)))
            cp.query(state["session_id"], "getzep/graphiti" if getattr(self.memory, "client", None) else "local-memory", "restaurant preferences", time.perf_counter()-started)
            if not getattr(self.memory, "client", None): cp.alert("WARNING", "GRAPHITI_FALLBACK", state["session_id"], "Graphiti unavailable; local memory fallback active")
            relationships = state["memory"].get("relationships", {})
            if state["session_id"] in cp.sessions:
                cp.sessions[state["session_id"]]["dietary_constraints"] = relationships.get("DIETARY_RESTRICTION", [])
        # Keep turns as strings internally; normalize them to Gemini Content at
        # the SDK boundary so no OpenAI-style `content` dictionaries leak in.
        state.setdefault("messages", []).append(state.get("last_user_transcript", ""))
        state.setdefault("candidate_restaurants", [])
        state.setdefault("pending_reservation", None)
        state.setdefault("voice_approval_verified", False)
        return state

    async def agent_reasoning(self, state: dict[str, Any]) -> dict[str, Any]:
        if not self.client:
            logger.error("\033[91m[LLM-REASON]\033[0m no Gemini client; configure GEMINI_API_KEY and install google-genai")
            state["response"] = "I’m ready to help find a restaurant. Add GEMINI_API_KEY to enable the live concierge model."
            state["pending_calls"] = []
            return state
        from google.genai import types
        declarations = [_function_declaration(name, fn) for name, fn in TOOL_FUNCTIONS.items()]
        config = types.GenerateContentConfig(system_instruction=_system_prompt(state), tools=[types.Tool(function_declarations=declarations)], automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True), thinking_config=types.ThinkingConfig(thinking_budget=0))
        contents = _normalize_gemini_contents(state["messages"][-12:], types)
        started = time.perf_counter()
        models_to_try = [self.model]
        for fallback in ("gemini-3-flash-preview", "gemini-3.1-flash-lite", "gemini-3.5-flash", "gemini-3.8-flash"):
            if fallback not in models_to_try:
                models_to_try.append(fallback)
        response = None
        last_exc: Exception | None = None
        for model_name in models_to_try:
            logger.info("\033[96m[LLM-REASON]\033[0m request session=%s model=%s message_count=%d", state.get("session_id"), model_name, len(contents))
            try:
                async def generate():
                    return await self.client.aio.models.generate_content(model=model_name, contents=contents, config=config)
                response = await resilient_call(generate, timeout=15.0, attempts=2, service=f"gemini-{model_name}")
                self.model = model_name
                break
            except Exception as exc:
                last_exc = exc
                logger.warning("\033[93m[LLM-REASON]\033[0m model=%s failed (%s); trying fallback", model_name, type(exc).__name__)
        if response is None:
            cp = getattr(self.enclave, "control_plane", None)
            if cp: cp.alert("WARNING", "GEMINI_FALLBACK", state["session_id"], f"Reasoning unavailable ({type(last_exc).__name__})")
            logger.exception("\033[91m[LLM-REASON]\033[0m all Gemini candidate models failed session=%s error=%s", state.get("session_id"), last_exc)
            state["response"] = "I can't reach the restaurant assistant right now. Please try again in a moment."
            state["pending_calls"] = []
            return state
        cp = getattr(self.enclave, "control_plane", None)
        if cp:
            cp.stage(state["session_id"], "gemini_reasoning", started)
            cp.query(state["session_id"], self.model, json.dumps(contents, default=str), time.perf_counter()-started)
        calls = []
        candidates = getattr(response, "candidates", None) or []
        if not candidates or not getattr(candidates[0], "content", None):
            logger.error("\033[91m[LLM-REASON]\033[0m Gemini returned no candidate content session=%s", state.get("session_id"))
            state["response"] = "I couldn't get a complete response just now. Please try again."
            state["pending_calls"] = []
            return state
        for part in candidates[0].content.parts or []:
            if getattr(part, "function_call", None):
                function_call = part.function_call
                calls.append({"name": function_call.name, "args": dict(function_call.args or {}),
                              "id": getattr(function_call, "id", None)})
            elif getattr(part, "text", None):
                state["response"] = part.text
        state["pending_calls"] = calls
        logger.info("\033[92m[LLM-REASON]\033[0m response session=%s tool_calls=%d text_chars=%d", state.get("session_id"), len(calls), len(state.get("response", "")))
        state["messages"].append(response.candidates[0].content)
        return state

    async def failproof_policy_gate(self, state: dict[str, Any]) -> dict[str, Any]:
        cp = getattr(self.enclave, "control_plane", None)
        allowed = []
        rejected = []
        for call in state.get("pending_calls", []):
            if cp: cp.snapshot(state["session_id"], "before", state)
            started = time.perf_counter()
            valid, reason = validate_tool_args(call["name"], call["args"])
            verdict = self.enclave.intercept(session_id=state["session_id"], tool_name=call["name"], args=call["args"], state=state) if valid else {"verdict": "deny", "reason": reason}
            if cp:
                cp.stage(state["session_id"], "failproof_enclave_gate", started, tool_name=call["name"], verdict=verdict["verdict"])
                cp.snapshot(state["session_id"], "after", state)
                policy_id = verdict.get("policy") or ("policy_booking_voice_approval" if call["name"] == "execute_reservation" else "policy_loop_and_idempotency" if "loop" in verdict.get("reason", "").lower() else "policy_schema_and_pii_guard")
                receipt = {}
                if call["name"] == "execute_reservation":
                    offer = state.get("pending_offer") or {}
                    offer_id = Enclave.offer_id(state["session_id"], call["args"])
                    playback_ts = offer.get("playback_completed_server_ts")
                    transcript_ts = state.get("last_user_transcript_server_ts")
                    speech_started_ts = state.get("last_user_speech_started_server_ts")
                    receipt = {"offer_id": offer_id,
                               "causal_proof": {"playback_completed_ts": playback_ts,
                                                "speech_started_ts": speech_started_ts,
                                                "transcript_received_ts": transcript_ts,
                                                "delta_ms": (transcript_ts - playback_ts) * 1000 if playback_ts is not None and transcript_ts is not None else None},
                               "transcript_sha256": hashlib.sha256(str(state.get("last_user_transcript", "")).encode()).hexdigest()}
                cp.audit(session_id=state["session_id"], policy_id=policy_id, tool_name=call["name"], verdict=verdict["verdict"], reason=verdict.get("reason", ""), prompt=verdict.get("inject_prompt"), **receipt)
            await _emit(state, {"type": "enclave", **verdict})
            if verdict["verdict"] == "allow": allowed.append(call)
            else:
                rejected.append({"name": call["name"], "id": call.get("id"),
                                 "response": {"error": verdict.get("reason", "Tool denied by runtime policy"),
                                              "policy_verdict": verdict["verdict"]}})
            if verdict["verdict"] == "steer":
                state["response"] = "Please confirm by voice: " + _offer_sentence(state, call["args"])
                offer = state.get("reservation_offer", {})
                if not offer.get("presented"):
                    offer = {"restaurant_name": call["args"].get("restaurant_name"), "time_slot": call["args"].get("time_slot"), "party_size": call["args"].get("party_size"), "presented": True}
                    state["reservation_offer"] = offer
            elif verdict["verdict"] != "allow":
                state["response"] = "I can’t complete that action safely. Let’s try another approach."
        state["pending_calls"] = allowed
        state["pretool_rejections"] = rejected
        if rejected and not allowed:
            state["messages"].append(_function_response_content(rejected))
            state["pretool_rejections"] = []
        return state

    async def tool_execution(self, state: dict[str, Any]) -> dict[str, Any]:
        calls = state.get("pending_calls", [])
        if not calls: return state
        filler_task = asyncio.create_task(self._filler(state))
        async def run(call):
            started = time.perf_counter()
            cp = getattr(self.enclave, "control_plane", None)
            logger.info("\033[96m[TOOL-EXEC]\033[0m start session=%s tool=%s", state.get("session_id"), call["name"])
            await _emit(state, {"type": "tool_requested", "name": call["name"], "args": call["args"]})
            session_token = TOOL_SESSION_ID.set(state["session_id"])
            try:
                result = await TOOL_FUNCTIONS[call["name"]](**call["args"])
            finally:
                TOOL_SESSION_ID.reset(session_token)
            if cp:
                cp.stage(state["session_id"], "parallel_tool_execution", started, tool_name=call["name"])
                if call["name"] in {"discover_nearby_restaurants", "verify_google_reviews"}:
                    fallback = result.get("source", "").endswith("fallback")
                    cp.query(state["session_id"], "google-places/local-dataset", json.dumps(call["args"], sort_keys=True), time.perf_counter()-started, fallback=fallback)
                    if fallback: cp.alert("WARNING", "GOOGLE_PLACES_FALLBACK", state["session_id"], "Local Bengaluru dataset served this request")
                if call["name"] == "verify_google_reviews":
                    reviews = " ".join(result.get("recent_review_sample", [])).lower()
                    positive = sum(reviews.count(w) for w in ("excellent", "great", "friendly", "clean", "helpful"))
                    negative = sum(reviews.count(w) for w in ("noisy", "wait", "slow", "crowded", "dirty"))
                    cp.event(state["session_id"], "REVIEW_SENTIMENT", place_id=call["args"].get("place_id"), score=positive-negative, red_flag=result.get("red_flag", False))
            if call["name"] == "discover_nearby_restaurants":
                state["candidate_restaurants"] = result.get("restaurants", [])
                if cp and state["session_id"] in cp.sessions:
                    cp.sessions[state["session_id"]]["candidate_restaurants"] = result.get("restaurants", [])
            if call["name"] == "check_table_availability" and result.get("hold_token"):
                hold = {**call["args"], **result}
                place = next((r for r in state.get("candidate_restaurants", []) if r.get("place_id") == hold["place_id"]), {})
                hold["restaurant_name"] = place.get("name", hold["place_id"])
                state["pending_reservation"] = hold
                state["pending_offer"] = _make_offer(state, hold)
            if call["name"] == "execute_reservation" and result.get("confirmed"):
                self.enclave.committed(call["args"]["hold_token"], state["session_id"])
                if cp: cp.executed_holds.append(call["args"]["hold_token"])
                if cp and state["session_id"] in cp.sessions:
                    cp.sessions[state["session_id"]]["booking_status"] = "CONFIRMED"
                await self.memory.record_booking(state.get("user_id", state["session_id"]), call["args"]["restaurant_name"], result["booking_reference"])
                state["pending_offer"] = None
            pruned = _prune_tool_result(call["name"], result)
            logger.info("\033[92m[TOOL-EXEC]\033[0m complete session=%s tool=%s", state.get("session_id"), call["name"])
            await _emit(state, {"type": "tool", "name": call["name"], "result": pruned})
            return {"name": call["name"], "result": pruned}
        results = await asyncio.gather(*(run(call) for call in calls), return_exceptions=True)
        await asyncio.gather(filler_task, return_exceptions=True)
        state["tool_results"] = [r if isinstance(r, dict) else {"error": str(r)} for r in results]
        response_entries = list(state.pop("pretool_rejections", []))
        for call, result in zip(calls, results):
            if isinstance(result, dict):
                function_result = result.get("result", {})
            else:
                function_result = {"error": str(result)}
            response_entries.append({"name": call["name"], "id": call.get("id"), "response": function_result})
        state["messages"].append(_function_response_content(response_entries))
        state["pending_calls"] = []
        return state

    async def _filler(self, state):
        try:
            filler_text = "Checking the menus and latest reviews around you..."
            cp = getattr(self.enclave, "control_plane", None)
            if cp: cp.event(state["session_id"], "ACOUSTIC_FILLER_TRIGGERED")
            started = time.perf_counter()
            audio = await self.sarvam.synthesize(filler_text)
            if audio:
                if cp: cp.stage(state["session_id"], "sarvam_tts", started, model=getattr(self.sarvam, "tts_model", "bulbul:v4-flash"), acoustic_filler=True)
                await _emit(state, {"type": "audio", "text": filler_text, "audio": base64.b64encode(audio).decode()})
        except Exception as exc:
            cp = getattr(self.enclave, "control_plane", None)
            if cp: cp.alert("WARNING", "SARVAM_FILLER_UNAVAILABLE", state["session_id"], type(exc).__name__)
            logger.warning("\033[93m[TTS-OUT]\033[0m acoustic filler failed session=%s error=%s", state.get("session_id"), type(exc).__name__)
            await _emit(state, {"type": "telemetry", "message": "Sarvam filler unavailable; tool execution continues"})

    async def tts_response_and_graph_commit(self, state: dict[str, Any]) -> dict[str, Any]:
        response = (state.get("response", "") or "").strip() or "I’ve checked that for you."
        await self.memory.ingest_episode(state.get("user_id", state["session_id"]), f"User: {state.get('last_user_transcript','')}\nAssistant: {response}")
        await _emit(state, {"type": "response", "text": response})
        try:
            started = time.perf_counter()
            audio = await self.sarvam.synthesize(response)
            cp = getattr(self.enclave, "control_plane", None)
            if cp: cp.stage(state["session_id"], "sarvam_tts", started, model=getattr(self.sarvam, "tts_model", "bulbul:v4-flash"))
            if audio:
                payload = {"type": "audio", "text": response, "audio": base64.b64encode(audio).decode()}
                offer = state.get("pending_offer")
                if offer and _contains_offer_terms(response, offer):
                    offer["offer_playback_verified"] = False
                    offer["playback_completed_server_ts"] = None
                    offer["playback_ack_pending"] = True
                    offer["playback_ack_received"] = False
                    offer["playback_ack_token"] = secrets.token_urlsafe(24)
                    offer["audio_dispatched_server_ts"] = cp.timestamp() if cp else time.time()
                    payload.update({"offer_id": offer["offer_id"], "requires_playback_ack": True,
                                    "playback_ack_token": offer["playback_ack_token"]})
                await _emit(state, payload)
        except Exception as exc:
            cp = getattr(self.enclave, "control_plane", None)
            if cp: cp.alert("WARNING", "SARVAM_TTS_UNAVAILABLE", state["session_id"], type(exc).__name__)
            logger.exception("\033[91m[TTS-OUT]\033[0m response synthesis failed session=%s", state.get("session_id"))
            await _emit(state, {"type": "telemetry", "message": f"Sarvam voice output unavailable: {exc}"})
        return state

    async def run_turn(self, state: dict[str, Any]) -> dict[str, Any]:
        cp = getattr(self.enclave, "control_plane", None)
        if cp and state["session_id"] in cp.sessions:
            cp.sessions[state["session_id"]]["turns"] += 1
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


def _function_response_content(entries: list[dict[str, Any]]):
    from google.genai import types
    parts = []
    for entry in entries:
        part_args = {"name": entry["name"], "response": entry["response"]}
        parts.append(types.Part.from_function_response(**part_args))
    return types.Content(role="tool", parts=parts)


def _normalize_gemini_contents(messages: list[Any], types) -> list[Any]:
    """Convert internal messages to SDK-supported Content objects with valid roles."""
    normalized = []
    for message in messages:
        if isinstance(message, str):
            normalized.append(types.Content(role="user", parts=[types.Part.from_text(text=message)]))
        elif isinstance(message, dict):
            if "content" in message:
                inner = message["content"]
                if isinstance(inner, types.Content):
                    role = "user" if getattr(inner, "role", None) == "tool" else getattr(inner, "role", "user")
                    normalized.append(types.Content(role=role, parts=inner.parts))
                elif isinstance(inner, str):
                    normalized.append(types.Content(role="user" if message.get("role") == "tool" else message.get("role", "user"), parts=[types.Part.from_text(text=inner)]))
                else:
                    normalized.append(inner)
            elif "role" in message and "parts" in message:
                role = "user" if message["role"] == "tool" else message["role"]
                normalized.append(types.Content(role=role, parts=message["parts"]))
            else:
                normalized.append(message)
        elif isinstance(message, types.Content):
            if getattr(message, "role", None) == "tool":
                normalized.append(types.Content(role="user", parts=message.parts))
            else:
                normalized.append(message)
        else:
            normalized.append(message)
    return normalized

def _offer_sentence(state, args):
    offer = state.get("reservation_offer", {})
    return f"{offer.get('restaurant_name', args.get('restaurant_name','the restaurant'))}, {offer.get('party_size', args.get('party_size','your party'))} people, at {offer.get('time_slot', args.get('time_slot','the requested time'))}."

def _system_prompt(state):
    return ("You are a concise, voice-first Bengaluru restaurant concierge. Speak naturally and briefly. Never claim a reservation is booked unless execute_reservation confirms it. Before booking, present exact restaurant name, party size, and time, then request explicit spoken confirmation. Use check_table_availability first. Search in parallel when useful. Use dietary constraints from memory. Memory: " + json.dumps(state.get("memory", {}), default=str))


def _prune_tool_result(name: str, result: dict[str, Any]) -> dict[str, Any]:
    """Keep only compact, decision-relevant fields in model and client histories."""
    if name == "discover_nearby_restaurants":
        keys = ("place_id", "name", "rating", "price_level")
        return {"restaurants": [{key: item.get(key) for key in keys} for item in result.get("restaurants", [])[:3]], "source": result.get("source")}
    if name == "verify_google_reviews":
        return {"pros": result.get("pros", [])[:3], "cons": result.get("cons", [])[:3], "red_flag": result.get("red_flag", False), "red_flag_reason": result.get("red_flag_reason")}
    if name == "inspect_menu_and_dietary_fit":
        return {"dishes": result.get("dishes", [])[:3], "note": result.get("note") or result.get("disclaimer")}
    if name == "check_table_availability":
        keys = ("available", "hold_token", "expires_in_seconds", "time_slot", "party_size", "reason")
        return {key: result[key] for key in keys if key in result}
    keys = ("confirmed", "booking_reference", "restaurant_name", "time_slot", "party_size", "place_id", "reason")
    return {key: result[key] for key in keys if key in result}

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
            "playback_completed_server_ts": None, "playback_ack_pending": False,
            "playback_ack_received": False, "playback_ack_token": None,
            "audio_dispatched_server_ts": None}

def _contains_offer_terms(text: str, offer: dict[str, Any]) -> bool:
    normalized = text.casefold()
    return all(str(offer.get(k, "")).casefold() in normalized for k in ("restaurant_name", "time_slot")) and str(offer.get("party_size")) in normalized

def _function_declaration(name, fn):
    from google.genai import types
    hints = inspect.signature(fn).parameters
    resolved_hints = get_type_hints(fn)
    props = {}
    required = []
    for key, param in hints.items():
        ann = resolved_hints.get(key, param.annotation)
        typ = _schema_type(ann)
        props[key] = types.Schema(type=typ, items=types.Schema(type="STRING") if typ == "ARRAY" else None)
        if param.default is inspect.Parameter.empty: required.append(key)
    return types.FunctionDeclaration(name=name, description=fn.__doc__ or name, parameters=types.Schema(type="OBJECT", properties=props, required=required))


def _schema_type(annotation: Any) -> str:
    if get_origin(annotation) is list:
        return "ARRAY"
    if annotation is int:
        return "INTEGER"
    if annotation is float:
        return "NUMBER"
    return "STRING"
