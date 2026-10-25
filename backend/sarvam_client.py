"""Sarvam-only speech adapters. No browser or operating-system speech fallback. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import base64
import io
import os
from typing import AsyncIterator
import wave
import httpx

SARVAM_BASE = "https://api.sarvam.ai"

LEGACY_SPEAKER_MAP = {
    "meera": "roopa_en_conversational",
    "pavithra": "simran_en_conversation",
    "mahesh": "dev_en_conversational",
    "default": "roopa_en_conversational",
}

class SarvamClient:
    def __init__(self) -> None:
        self.api_key = os.getenv("SARVAM_API_KEY", "")

    def _resolve_speaker(self) -> str:
        speaker = os.getenv("SARVAM_SPEAKER", "roopa_en_conversational").strip()
        return LEGACY_SPEAKER_MAP.get(speaker.lower(), speaker or "roopa_en_conversational")

    async def transcribe(self, pcm16: bytes, sample_rate: int = 16000) -> str:
        if not self.api_key:
            raise RuntimeError("SARVAM_API_KEY is required for speech recognition")
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{SARVAM_BASE}/speech-to-text",
                headers={"api-subscription-key": self.api_key},
                data={"model": "saaras:v3", "language_code": "unknown", "sampling_rate": str(sample_rate), "input_audio_codec": "wav"},
                files={"file": ("speech.wav", _wav(pcm16, sample_rate), "audio/wav")}
            )
            response.raise_for_status()
            return response.json().get("transcript", "")

    async def synthesize(self, text: str) -> bytes:
        if not self.api_key:
            raise RuntimeError("SARVAM_API_KEY is required for voice output")
        clean_text = (text or "").strip()
        if not clean_text:
            return b""
        speaker = self._resolve_speaker()
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{SARVAM_BASE}/text-to-speech",
                headers={"api-subscription-key": self.api_key, "Content-Type": "application/json"},
                json={
                    "text": clean_text[:2500],
                    "target_language_code": "en-IN",
                    "model": "bulbul:v4-flash",
                    "speaker": speaker,
                    "pace": 1.05,
                    "speech_sample_rate": 16000,
                    "enable_preprocessing": True
                }
            )
            response.raise_for_status()
            payload = response.json()
            audios = payload.get("audios") or []
            if not audios:
                return b""
            return base64.b64decode(audios[0])

    async def stream_tts(self, text: str, chunk_size: int = 4096) -> AsyncIterator[bytes]:
        audio = await self.synthesize(text)
        for offset in range(0, len(audio), chunk_size):
            yield audio[offset:offset + chunk_size]

def _wav(pcm: bytes, sample_rate: int) -> bytes:
    out = io.BytesIO()
    with wave.open(out, "wb") as wf:
        wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(sample_rate); wf.writeframes(pcm)
    return out.getvalue()

