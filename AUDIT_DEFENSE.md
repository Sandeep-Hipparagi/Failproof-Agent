# Audit Defense Brief

**Developed by Sandeep Hipparagi**

## Reservation authorization

Each active offer is bound to the session, place, time, party size, and temporary hold token by a deterministic SHA-256 offer ID. The server issues a fresh random playback ACK token when it sends offer audio. The WebSocket handler accepts only a matching pending offer and token, compares the token in constant time, and consumes it after the first completion or interruption event. The enclave separately matches the reservation arguments to the active offer and requires server-recorded playback completion, a later server-stamped final transcript, and explicit affirmative language without negation.

The random ACK token prevents accidental cross-offer acknowledgements and straightforward replay. It does **not** prove that sound physically reached a speaker: the browser receives the token and can be modified to send a false completion event. A browser-side ACK is telemetry from a client, not hardware-backed acoustic attestation. Stronger proof would require a trusted playback path or an independently trusted device signal; this application does not claim either.

## WORM ledger

Audit records form a SHA-256 hash chain and are appended with flush and `fsync`. A per-ledger process lock and operating-system file lock serialize concurrent readers and writers; each writer verifies an externally changed ledger before continuing. Invalid records fail verification and lock subsequent writes. This is **tamper-evident application storage**, not an immutable hardware-backed WORM appliance: an administrator able to rewrite the complete file can recompute the chain, so independent signatures or external anchoring would be needed to prove the history against that administrator.

## Hosted telemetry

The optional bridge sends original WORM blocks, allowlisted session events and summaries, and health heartbeats. Transcript text and user location are excluded from session events. TLS verification stays enabled; failed batches retry with backoff, and missing configuration leaves local behavior available. Failproof hosted deployments use a separate event-ingest hostname and may require a scoped API key plus mTLS credentials; the dashboard URL is not an ingest endpoint. A successful local enqueue or HTTP response does not by itself demonstrate that the dashboard has registered this process as a guarded machine.

## Judge Q&A

- **Can the client fake `TTS_PLAYBACK_COMPLETED`?** Yes. The nonce is visible to that client. It prevents replay across offers but is not physical playback attestation.
- **Why is a forged model quote insufficient?** The quote alone is not the authorization source. The server requires a matching active offer, a consumed playback handshake, a later server-timestamped transcript, and affirmative intent.
- **Can the ledger be rewritten by an administrator?** A rewrite can be detected against a separately retained chain head or signed checkpoint. The local chain alone cannot resist an administrator who rewrites every block and recomputes every hash.
- **What makes the hosted machine appear?** The tenant-issued ingest URL, API key, and required client certificate/key must be configured. The dashboard URL alone cannot receive agent events.
