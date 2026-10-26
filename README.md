# Failproof Restaurant Concierge

**Developed by Sandeep Hipparagi**

A minimal, voice-only Bengaluru restaurant concierge built around a fail-closed reservation flow. The architecture combines Sarvam speech services, Gemini reasoning, concurrent restaurant tools, temporal Graphiti memory, and deterministic runtime interception.

## Architecture

- **Voice pipeline:** Sarvam `saaras:v4` receives mono 16 kHz PCM captured from the microphone. Sarvam `bulbul:v4-flash` produces all spoken responses. The browser has no speech synthesis or default-voice fallback.
- **Reasoning and tools:** Google `gemini-2.5-flash` selects restaurant discovery, review, menu, availability, and reservation tools. Independent tool calls run concurrently; Sarvam acoustic filler masks lookup latency.
- **Failproof runtime enclave:** Every tool call is validated before execution. The enclave supports `allow`, `steer`, and `break-loop` outcomes, hashes reservation offer details into an offer ID, and accepts booking only after the client reports natural completion with `TTS_PLAYBACK_COMPLETED`. Server receipt timestamps enforce that the affirmative transcript arrived after playback completion.
- **Temporal memory:** Graphiti (`getzep/graphiti`) retrieves preference and booking facts and ingests conversation episodes. Neo4j is provided through Docker Compose; in-process memory is used automatically if Graphiti is unavailable.
- **Restaurant data:** Google Places API (New) is optional. Local Bengaluru discovery data remains available when Places is missing or rate-limited.
- **Cost controls:** Read-only restaurant tools use a 300-second argument-keyed cache, tool histories are reduced to decision-relevant fields, and the standard acoustic filler is warmed and cached when Sarvam is configured.
- **Audit durability:** Verdicts and gate snapshots are fsync-committed to a SHA-256 chained JSONL ledger. Verify with `GET /api/fix/activity/verify`.
- **Hosted telemetry (optional):** `backend/platform_bridge.py` forwards signed WORM blocks, transcript-free session telemetry, and machine heartbeats to a configured HTTPS `/events` ingest URL. It is disabled when configuration is absent. Failproof-hosted ingestion uses a separate ingest hostname and requires its scoped API key plus mTLS certificate and key; `/blazingfast` is the dashboard, not the ingest endpoint.

## Requirements

- Python 3.11 or newer
- Docker Compose for persistent Neo4j-backed memory (optional)
- Sarvam and Gemini API keys for live voice and reasoning; Google Places is optional

## Run locally

1. Copy `.env.example` to `.env` and add `SARVAM_API_KEY` and `GEMINI_API_KEY`.
2. Optionally add `GOOGLE_PLACES_API_KEY` and start Neo4j with `docker compose up -d`.
3. Create a virtual environment and install dependencies: `python -m venv .venv`, then `.\.venv\Scripts\python.exe -m pip install -r requirements.txt` on Windows (or activate `.venv` and run `python -m pip install -r requirements.txt` elsewhere).
4. Start the server with the same environment: `.\.venv\Scripts\python.exe -m uvicorn backend.main:app --reload` on Windows (or `python -m uvicorn backend.main:app --reload` after activating `.venv`). Running Uvicorn outside the project environment can silently disable Gemini if `google-genai` is missing.
5. Open `http://localhost:8000`, allow microphone access, and hold the center orb while speaking.

To enable hosted telemetry, obtain the tenant's ingest URL, `events:add` API key, and client certificate/key from the platform administrator. Configure `FAILPROOF_PLATFORM_INGEST_URL`, `FAILPROOF_PLATFORM_API_KEY`, `FAILPROOF_PLATFORM_CLIENT_CERT`, and `FAILPROOF_PLATFORM_CLIENT_KEY` in `.env`. Delivery retries in memory with backoff; absent or invalid configuration leaves local operation available.

## Rehearsal and verification

Run `python -m py_compile backend/*.py backend/tools/*.py` and `pytest` for unit checks. Run `python backend/smoke_test_live_loop.py` for a deterministic WebSocket rehearsal of the interruption-to-steer and verified-playback-to-booking sequence. The smoke rehearsal uses local test doubles for speech recognition and model decisions; it does not require API credentials or create a live restaurant booking.

## Source map

- `backend/main.py`: FastAPI endpoints, WebSocket event handling, server-side causal timestamps.
- `backend/platform_bridge.py`: optional authenticated WORM/session/heartbeat bridge.
- `backend/sarvam_client.py`: Sarvam STT and TTS requests.
- `backend/agent_harness.py`: LangGraph state cycle, Gemini tool loop, offer hashing, and filler audio.
- `backend/failproof_enclave.py`: argument validation, booking policies, idempotency, and loop breaker.
- `backend/graph_memory.py`: Graphiti retrieval and episode ingestion with local fallback.
- `backend/tools/restaurant_tools.py`: restaurant, review, menu, availability, and reservation tools.
- `backend/test_enclave_playback_ack.py`: enclave playback and timestamp policy tests.
- `backend/smoke_test_live_loop.py`: end-to-end WebSocket rehearsal.
- `frontend/index.html`: minimal push-to-talk voice interface and telemetry.

## Data limitations

Local review and menu entries are demonstration data. Confirm current menu ingredients and allergens directly with the restaurant. The local availability and reservation path is a demonstration adapter rather than a connected booking provider.
