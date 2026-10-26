"""Regression checks for browser capture wiring and session-scoped memory."""
import asyncio
import re
from pathlib import Path

from backend.graph_memory import GraphMemory
from backend.agent_harness import _schema_type
from backend.tools.restaurant_tools import discover_nearby_restaurants, check_table_availability, inspect_menu_and_dietary_fit
from typing import get_type_hints


def test_microphone_source_feeds_the_pcm_script_processor():
    html = (Path(__file__).resolve().parents[1] / "frontend" / "index.html").read_text(encoding="utf-8")
    assert re.search(r"processor=audioContext\.createScriptProcessor\([^;]+\);source\.connect\(processor\);", html)
    assert "resampleTo16k(samples,audioContext.sampleRate)" in html


def test_local_memory_is_scoped_to_stable_browser_user_id():
    memory = GraphMemory()

    async def run():
        await memory.ingest_episode("browser-a", "PREFERS_CUISINE: South Indian")
        a = await memory.retrieve_preferences("browser-a")
        b = await memory.retrieve_preferences("browser-b")
        return a, b

    a, b = asyncio.run(run())
    assert a["facts"] and not b["facts"]
    assert a["relationships"]["PREFERS_CUISINE"]


def test_graphiti_group_ids_are_stable_and_user_isolated():
    memory = GraphMemory()
    assert memory._group_for_user("browser-a") == memory._group_for_user("browser-a")
    assert memory._group_for_user("browser-a") != memory._group_for_user("browser-b")


def test_gemini_tool_schema_preserves_float_integer_and_list_types():
    discover = get_type_hints(discover_nearby_restaurants)
    availability = get_type_hints(check_table_availability)
    menu = get_type_hints(inspect_menu_and_dietary_fit)
    assert _schema_type(discover["lat"]) == "NUMBER"
    assert _schema_type(availability["party_size"]) == "INTEGER"
    assert _schema_type(menu["dietary_constraints"]) == "ARRAY"


def test_google_genai_tool_declarations_and_responses_match_installed_sdk():
    import pytest
    pytest.importorskip("google.genai")
    from google.genai import types
    from backend.agent_harness import (
        _function_declaration,
        _function_response_content,
        _normalize_gemini_contents,
    )

    declaration = _function_declaration("discover_nearby_restaurants", discover_nearby_restaurants)
    response = _function_response_content([
        {"name": "discover_nearby_restaurants", "response": {"restaurants": []}}
    ])
    assert declaration.parameters.properties["lat"].type.name == "NUMBER"
    assert response.role == "tool"
    assert response.parts[0].function_response.name == "discover_nearby_restaurants"

    history = _normalize_gemini_contents(["Find a restaurant", response], types)
    request = types._GenerateContentParameters.model_validate({
        "model": "gemini-3.5-flash",
        "contents": history,
    })
    assert request.contents[0].role == "user"
    assert request.contents[0].parts[0].text == "Find a restaurant"
