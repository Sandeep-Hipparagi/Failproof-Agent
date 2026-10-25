"""Sarvam-only speech adapters. No browser or operating-system speech fallback. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import os
from typing import AsyncIterator
import httpx

SARVAM_BASE = "https://api.sarvam.ai"

class SarvamClient:
    def __init__(self) -> None:
        self.api_key = os.getenv("SARVAM_API_KEY", "")

    async def transcribe(self, pcm16: bytes, sample_rate: int = 16000) -> str:
        if not self.api_key:
            raise RuntimeError("SARVAM_API_KEY is required for speech recognition")
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(f"{SARVAM_BASE}/speech-to-text", headers={"api-subscription-key": self.api_key}, data={"model": "saaras:v3", "language_code": "unknown", "sampling_rate": str(sample_rate), "input_audio_codec": "wav"}, files={"file": ("speech.wav", _wav(pcm16, sample_rate), "audio/wav")})
            response.raise_for_status()
            return response.json().get("transcript", "")

    async def synthesize(self, text: str) -> bytes:
        if not self.api_key:
            raise RuntimeError("SARVAM_API_KEY is required for voice output")
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(f"{SARVAM_BASE}/text-to-speech", headers={"api-subscription-key": self.api_key, "Content-Type": "application/json"}, json={"text": text[:2500], "target_language_code": "en-IN", "model": "bulbul:v4-flash", "speaker": os.getenv("SARVAM_SPEAKER", "meera"), "pace": 1.05, "speech_sample_rate": 16000, "enable_preprocessing": True})
            response.raise_for_status()
            payload = response.json()
            import base64
            return base64.b64decode(payload["audios"][0])

    async def stream_tts(self, text: str, chunk_size: int = 4096) -> AsyncIterator[bytes]:
        audio = await self.synthesize(text)
        for offset in range(0, len(audio), chunk_size):
            yield audio[offset:offset + chunk_size]

def _wav(pcm: bytes, sample_rate: int) -> bytes:
    import io, wave
    out = io.BytesIO()
    with wave.open(out, "wb") as wf:
        wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(sample_rate); wf.writeframes(pcm)
    return out.getvalue()
