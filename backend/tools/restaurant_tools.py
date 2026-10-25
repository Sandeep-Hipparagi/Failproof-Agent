"""Restaurant discovery and booking tools with deterministic Bengaluru demo data. Developed by Sandeep Hipparagi."""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any

import httpx

SEED: list[dict[str, Any]] = [
    {"place_id": "seed_mtr_lalbagh", "name": "MTR (Lalbagh Road)", "rating": 4.4, "address": "14, Lalbagh Road, Bengaluru", "price_level": 2, "cuisine": "South Indian", "reviews": ["Excellent rava idli and service", "Busy at peak hours", "Clean dining room"], "menu": [{"name": "Rava Idli", "price_inr": 95, "allergens": ["dairy", "gluten"]}, {"name": "Masala Dosa", "price_inr": 120, "allergens": ["dairy", "gluten"]}, {"name": "Kesari Bath", "price_inr": 85, "allergens": ["dairy", "nuts"]}]},
    {"place_id": "seed_vidyarthi_bhavan", "name": "Vidyarthi Bhavan", "rating": 4.3, "address": "32, Gandhi Bazaar Main Road, Basavanagudi", "price_level": 1, "cuisine": "South Indian", "reviews": ["Crisp benne dosa", "Long waits on weekends", "Friendly staff"], "menu": [{"name": "Benne Masala Dosa", "price_inr": 90, "allergens": ["dairy", "gluten"]}, {"name": "Idli Vada", "price_inr": 75, "allergens": ["dairy"]}, {"name": "Filter Coffee", "price_inr": 35, "allergens": ["dairy"]}]},
    {"place_id": "seed_toit_indiranagar", "name": "Toit, Indiranagar", "rating": 4.5, "address": "298, 100 Feet Road, Indiranagar", "price_level": 3, "cuisine": "Continental", "reviews": ["Great ambience", "Can be noisy in the evening", "Helpful service"], "menu": [{"name": "Grilled Chicken Bowl", "price_inr": 480, "allergens": []}, {"name": "Paneer Salad", "price_inr": 390, "allergens": ["dairy", "nuts"]}, {"name": "Mushroom Pizza", "price_inr": 420, "allergens": ["dairy", "gluten"]}]},
    {"place_id": "seed_ಸರೂja", "name": "Nagarjuna, Residency Road", "rating": 4.2, "address": "Residency Road, Bengaluru", "price_level": 2, "cuisine": "Andhra", "reviews": ["Spicy meals", "Quick service", "Some peak-time waits"], "menu": [{"name": "Andhra Chicken Meal", "price_inr": 350, "allergens": []}, {"name": "Paneer Meal", "price_inr": 290, "allergens": ["dairy"]}, {"name": "Gutti Vankaya", "price_inr": 260, "allergens": ["nuts"]}]},
]
_holds: dict[str, dict[str, Any]] = {}
_bookings: dict[str, dict[str, Any]] = {}


def _seed_matches(cuisine: str, radius_m: int = 3000) -> list[dict[str, Any]]:
    q = cuisine.lower()
    matches = [r for r in SEED if q in r["cuisine"].lower() or q in r["name"].lower() or q in ("any", "all", "")]
    return [{k: r[k] for k in ("place_id", "name", "rating", "address", "price_level")} for r in (matches or SEED)[:3]]


async def discover_nearby_restaurants(lat: float, lng: float, cuisine: str, radius_m: int = 3000) -> dict[str, Any]:
    key = os.getenv("GOOGLE_PLACES_API_KEY")
    if key:
        try:
            async with httpx.AsyncClient(timeout=3.5) as client:
                resp = await client.post("https://places.googleapis.com/v1/places:searchText", headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": "places.id,places.displayName,places.rating,places.formattedAddress,places.priceLevel"}, json={"textQuery": f"{cuisine} restaurants", "locationBias": {"circle": {"center": {"latitude": lat, "longitude": lng}, "radius": float(radius_m)}}, "maxResultCount": 3})
                resp.raise_for_status()
                rows = resp.json().get("places", [])[:3]
                if rows:
                    return {"restaurants": [{"place_id": p["id"], "name": p.get("displayName", {}).get("text", ""), "rating": p.get("rating"), "address": p.get("formattedAddress", ""), "price_level": p.get("priceLevel")} for p in rows], "source": "google_places"}
        except Exception:
            pass
    return {"restaurants": _seed_matches(cuisine, radius_m), "source": "local_bengaluru_fallback"}


async def verify_google_reviews(place_id: str) -> dict[str, Any]:
    key = os.getenv("GOOGLE_PLACES_API_KEY")
    if key and not place_id.startswith("seed_"):
        try:
            async with httpx.AsyncClient(timeout=3.5) as client:
                resp = await client.get(f"https://places.googleapis.com/v1/places/{place_id}", headers={"X-Goog-Api-Key": key, "X-Goog-FieldMask": "reviews"})
                resp.raise_for_status()
                reviews = [r.get("text", {}).get("text", "") for r in resp.json().get("reviews", [])]
                if reviews:
                    return _summarize_reviews(reviews)
        except Exception:
            pass
    row = next((r for r in SEED if r["place_id"] == place_id), None)
    return _summarize_reviews(row["reviews"] if row else ["Review data unavailable; verify with venue."])


def _summarize_reviews(reviews: list[str]) -> dict[str, Any]:
    joined = " ".join(reviews).lower()
    red = any(w in joined for w in ("food poisoning", "unclean", "hygiene", "dirty", "rude staff", "scam"))
    return {"recent_review_sample": reviews[:5], "pros": [x for x in ("ambience", "service", "clean", "food", "friendly") if x in joined], "cons": [x for x in ("noisy", "wait", "slow", "crowded") if x in joined], "red_flag": red, "red_flag_reason": "Potential hygiene or service complaints found" if red else None}


async def inspect_menu_and_dietary_fit(place_id: str, dietary_constraints: list[str]) -> dict[str, Any]:
    row = next((r for r in SEED if r["place_id"] == place_id), None)
    if not row:
        return {"dishes": [], "note": "Structured menu unavailable; confirm ingredients directly with restaurant."}
    constraints = {x.lower() for x in dietary_constraints}
    def fits(d: dict[str, Any]) -> bool:
        allergens = set(d["allergens"])
        if "vegan" in constraints and allergens & {"dairy", "egg", "honey"}: return False
        if "vegetarian" in constraints and any(x in d["name"].lower() for x in ("chicken", "mutton", "fish", "meat")): return False
        if "gluten-free" in constraints and "gluten" in allergens: return False
        return not (allergens & constraints)
    return {"dishes": [d for d in row["menu"] if fits(d)][:3], "disclaimer": "Seed menu for demonstration; confirm allergens with venue."}


async def check_table_availability(place_id: str, time_slot: str, party_size: int) -> dict[str, Any]:
    if party_size < 1 or party_size > 20:
        return {"available": False, "reason": "Party size must be between 1 and 20"}
    token = "hold_" + uuid.uuid4().hex[:12]
    _holds[token] = {"place_id": place_id, "time_slot": time_slot, "party_size": party_size, "expires_at": time.time() + 300}
    return {"available": True, "hold_token": token, "expires_in_seconds": 300, "time_slot": time_slot, "party_size": party_size, "note": "Temporary demo hold only; no booking has been made."}


async def execute_reservation(place_id: str, restaurant_name: str, time_slot: str, party_size: int, hold_token: str, user_confirmation_quote: str) -> dict[str, Any]:
    hold = _holds.get(hold_token)
    if not hold or hold["expires_at"] < time.time():
        return {"confirmed": False, "reason": "Hold token is missing or expired"}
    if (hold["place_id"], hold["time_slot"], hold["party_size"]) != (place_id, time_slot, party_size):
        return {"confirmed": False, "reason": "Reservation details do not match the availability hold"}
    if hold_token in _bookings:
        return {"confirmed": False, "reason": "This hold token has already been used"}
    ref = "FP-" + uuid.uuid4().hex[:8].upper()
    result = {"confirmed": True, "booking_reference": ref, "restaurant_name": restaurant_name, "time_slot": time_slot, "party_size": party_size, "place_id": place_id}
    _bookings[hold_token] = result
    return result


TOOL_FUNCTIONS = {f.__name__: f for f in (discover_nearby_restaurants, verify_google_reviews, inspect_menu_and_dietary_fit, check_table_availability, execute_reservation)}
