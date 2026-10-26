"""Sarvam-only speech adapters. No browser or operating-system speech fallback. Developed by Sandeep Hipparagi."""
from __future__ import annotations
import base64
import io
import logging
import os
import wave
import httpx

from .resilience import resilient_call

SARVAM_BASE = "https://api.sarvam.ai"
logger = logging.getLogger(__name__)

LEGACY_SPEAKER_MAP = {
    "meera": "roopa",
    "pavithra": "simran",
    "mahesh": "dev",
    "roopa_en_conversational": "roopa",
    "simran_en_conversation": "simran",
    "dev_en_conversational": "dev",
    "default": "roopa",
}

class SarvamClient:
    tts_model = "bulbul:v3"

    def __init__(self) -> None:
        self.api_key = os.getenv("SARVAM_API_KEY", "")
        self.http_client: httpx.AsyncClient | None = None
        self._audio_cache: dict[str, bytes] = {}

    def _resolve_speaker(self) -> str:
        speaker = os.getenv("SARVAM_SPEAKER", "roopa").strip()
        return LEGACY_SPEAKER_MAP.get(speaker.lower(), speaker or "roopa")

    async def transcribe(self, pcm16: bytes, sample_rate: int = 16000) -> str:
        if not self.api_key:
            logger.error("\033[91m[STT-IN]\033[0m missing SARVAM_API_KEY")
            raise RuntimeError("SARVAM_API_KEY is required for speech recognition")
        if not pcm16 or not any(pcm16):
            logger.warning("\033[93m[STT-IN]\033[0m empty or silent PCM payload; skipping request")
            return ""
        logger.info("\033[96m[STT-IN]\033[0m submitting wav_bytes=%d sample_rate=%d channels=1 model=saaras:v3", len(pcm16), sample_rate)
        async def request(client):
            async def post():
                response = await client.post(
                    f"{SARVAM_BASE}/speech-to-text",
                    headers={"api-subscription-key": self.api_key},
                    data={"model": "saaras:v3", "language_code": "unknown", "sampling_rate": str(sample_rate)},
                    files={"file": ("speech.wav", _wav(pcm16, sample_rate), "audio/wav")}
                )
                if response.is_error:
                    logger.error("\033[91m[STT-IN]\033[0m Sarvam HTTP %s: %s", response.status_code, response.text[:300])
                    response.raise_for_status()
                return response
            return await resilient_call(post, timeout=8.0, attempts=2, service="sarvam-stt")
        if self.http_client:
            response = await request(self.http_client)
        else:
            async with httpx.AsyncClient(timeout=8.0) as client:
                response = await request(client)
        transcript = response.json().get("transcript", "")
        logger.info("\033[92m[STT-IN]\033[0m completed transcript_chars=%d", len(transcript or ""))
        return transcript

    async def synthesize(self, text: str) -> bytes:
        clean_text = (text or "").strip()
        if not clean_text:
            return b""
        if clean_text in self._audio_cache:
            logger.info("\033[95m[TTS-OUT]\033[0m audio cache hit text_chars=%d", len(clean_text))
            return self._audio_cache[clean_text]
        if not self.api_key:
            logger.error("\033[91m[TTS-OUT]\033[0m missing SARVAM_API_KEY")
            raise RuntimeError("SARVAM_API_KEY is required for voice output")
        speaker = self._resolve_speaker()
        logger.info("\033[95m[TTS-OUT]\033[0m submitting text_chars=%d model=%s speaker=%s", len(clean_text), self.tts_model, speaker)
        async def request(client):
            async def post():
                response = await client.post(
                    f"{SARVAM_BASE}/text-to-speech",
                    headers={"api-subscription-key": self.api_key, "Content-Type": "application/json"},
                    json={
                        "text": clean_text[:2500],
                        "language_code": "en-IN",
                        "model": "bulbul:v3",
                        "speaker": speaker,
                        "pace": 1.05,
                        "speech_sample_rate": 16000,
                        "output_audio_codec": "wav"
                    }
                )
                if response.is_error:
                    logger.error("\033[91m[TTS-OUT]\033[0m Sarvam HTTP %s: %s", response.status_code, response.text[:300])
                    response.raise_for_status()
                return response
            return await resilient_call(post, timeout=8.0, attempts=2, service="sarvam-tts")
        if self.http_client:
            response = await request(self.http_client)
        else:
            async with httpx.AsyncClient(timeout=8.0) as client:
                response = await request(client)
        payload = response.json()
        audios = payload.get("audios") or []
        audio = base64.b64decode(audios[0]) if audios else b""
        if not audio:
            logger.error("\033[91m[TTS-OUT]\033[0m Sarvam response contained no audio")
            raise RuntimeError("Sarvam TTS response did not contain audio")
        logger.info("\033[92m[TTS-OUT]\033[0m synthesized audio_bytes=%d", len(audio))
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
