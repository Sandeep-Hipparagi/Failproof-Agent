"""Sarvam REST wire-contract tests for the voice pipeline. Developed by Sandeep Hipparagi."""
import asyncio
import base64
import json
import os

import httpx

from backend.sarvam_client import SarvamClient


def test_sarvam_stt_sends_wav_multipart_and_authentication(monkeypatch):
    monkeypatch.setenv("SARVAM_API_KEY", "test-key")
    captured = {}

    def handler(request):
        captured["request"] = request
        return httpx.Response(200, json={"transcript": "Find a South Indian restaurant"})

    async def run():
        sarvam = SarvamClient()
        sarvam.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            transcript = await sarvam.transcribe(b"\x01\x00" * 320, 16000)
            return transcript
        finally:
            await sarvam.http_client.aclose()

    assert asyncio.run(run()) == "Find a South Indian restaurant"
    request = captured["request"]
    body = request.content
    assert request.url.path == "/speech-to-text"
    assert request.headers["api-subscription-key"] == "test-key"
    assert b'name="model"' in body and b"saaras:v3" in body
    assert b'name="sampling_rate"' in body and b"16000" in body
    assert b'filename="speech.wav"' in body and b"RIFF" in body


def test_sarvam_tts_uses_valid_bulbul_v3_contract(monkeypatch):
    monkeypatch.setenv("SARVAM_API_KEY", "test-key")
    monkeypatch.setenv("SARVAM_SPEAKER", "roopa_en_conversational")
    captured = {}
    audio_bytes = b"RIFF\x00\x00\x00\x00WAVE"

    def handler(request):
        captured["request"] = request
        captured["payload"] = json.loads(request.content)
        return httpx.Response(200, json={"audios": [base64.b64encode(audio_bytes).decode()]})

    async def run():
        sarvam = SarvamClient()
        sarvam.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        try:
            return await sarvam.synthesize("Hello there")
        finally:
            await sarvam.http_client.aclose()

    assert asyncio.run(run()) == audio_bytes
    request, payload = captured["request"], captured["payload"]
    assert request.url.path == "/text-to-speech"
    assert request.headers["api-subscription-key"] == "test-key"
    assert payload["language_code"] == "en-IN"
    assert payload["model"] == "bulbul:v3"
    assert payload["speaker"] == "roopa"
    assert payload["speech_sample_rate"] == 16000
    assert payload["output_audio_codec"] == "wav"
    assert "target_language_code" not in payload
    assert "enable_preprocessing" not in payload
