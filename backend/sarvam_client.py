"""Sarvam-only speech adapters. No browser or operating-system speech fallback. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import base64
import io
import os
import wave
import httpx

from .resilience import resilient_call

SARVAM_BASE = "https://api.sarvam.ai"

LEGACY_SPEAKER_MAP = {
    "meera": "roopa_en_conversational",
    "pavithra": "simran_en_conversation",
    "mahesh": "dev_en_conversational",
    "default": "roopa_en_conversational",
}

class SarvamClient:
    tts_model = "bulbul:v3"

    def __init__(self) -> None:
        self.api_key = os.getenv("SARVAM_API_KEY", "")
        self.http_client: httpx.AsyncClient | None = None
        self._audio_cache: dict[str, bytes] = {}

    def _resolve_speaker(self) -> str:
        speaker = os.getenv("SARVAM_SPEAKER", "roopa_en_conversational").strip()
        return LEGACY_SPEAKER_MAP.get(speaker.lower(), speaker or "roopa_en_conversational")

    async def transcribe(self, pcm16: bytes, sample_rate: int = 16000) -> str:
        if not self.api_key:
            raise RuntimeError("SARVAM_API_KEY is required for speech recognition")
        if not pcm16 or not any(pcm16):
            return ""
        async def request(client):
            async def post():
                response = await client.post(
                    f"{SARVAM_BASE}/speech-to-text",
                    headers={"api-subscription-key": self.api_key},
                    data={"model": "saaras:v3", "language_code": "unknown", "sampling_rate": str(sample_rate), "input_audio_codec": "wav"},
                    files={"file": ("speech.wav", _wav(pcm16, sample_rate), "audio/wav")}
                )
                response.raise_for_status()
                return response
            return await resilient_call(post, timeout=4.0, attempts=2, service="sarvam-stt")
        if self.http_client:
            response = await request(self.http_client)
        else:
            async with httpx.AsyncClient(timeout=4.0) as client:
                response = await request(client)
        return response.json().get("transcript", "")

    async def synthesize(self, text: str) -> bytes:
        clean_text = (text or "").strip()
        if not clean_text:
            return b""
        if clean_text in self._audio_cache:
            return self._audio_cache[clean_text]
        if not self.api_key:
            raise RuntimeError("SARVAM_API_KEY is required for voice output")
        speaker = self._resolve_speaker()
        async def request(client):
            async def post():
                response = await client.post(
                    f"{SARVAM_BASE}/text-to-speech",
                    headers={"api-subscription-key": self.api_key, "Content-Type": "application/json"},
                    json={
                        "text": clean_text[:2500],
                        "target_language_code": "en-IN",
                        "model": "bulbul:v3",
                        "speaker": speaker,
                        "pace": 1.05,
                        "speech_sample_rate": 16000,
                        "enable_preprocessing": True
                    }
                )
                response.raise_for_status()
                return response
            return await resilient_call(post, timeout=4.0, attempts=2, service="sarvam-tts")
        if self.http_client:
            response = await request(self.http_client)
        else:
            async with httpx.AsyncClient(timeout=4.0) as client:
                response = await request(client)
        payload = response.json()
        audios = payload.get("audios") or []
        audio = base64.b64decode(audios[0]) if audios else b""
        if audio:
            if len(self._audio_cache) >= 64:
                self._audio_cache.pop(next(iter(self._audio_cache)))
            self._audio_cache[clean_text] = audio
        return audio

def _wav(pcm: bytes, sample_rate: int) -> bytes:
    out = io.BytesIO()
    with wave.open(out, "wb") as wf:
        wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(sample_rate); wf.writeframes(pcm)
    return out.getvalue()

