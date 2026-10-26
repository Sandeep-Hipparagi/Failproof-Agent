# Failproof Buildathon Stage Runbook

**Developed by Sandeep Hipparagi**

## A. 60-Second Pre-Stage Terminal Checklist

Run these from the repository root in PowerShell. The seeder reports credential presence only; it never prints secret values.

```powershell
python -m backend.seed_stage_demo --reset
python -m py_compile (Get-ChildItem backend -Recurse -Filter *.py | ForEach-Object FullName)
pytest -q backend
uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

In a second terminal, open the stage UI and use the browser's full-screen shortcut (`F11`). The page is dark by design.

```powershell
Start-Process "http://localhost:8000"
```

If `.env` contains live credentials, load them into the **Uvicorn terminal's process environment** before launch; the app reads environment variables and does not load `.env` itself. A safe PowerShell import that does not echo values:

```powershell
Get-Content .env | Where-Object { $_ -match '^\s*[^#][^=]*=' } | ForEach-Object {
  $key, $value = $_.Split('=', 2)
  [Environment]::SetEnvironmentVariable($key.Trim(), $value.Trim().Trim('"').Trim("'"), 'Process')
}
```

The seed creates two activity decisions (one `STEER`, one `ALLOW`) with durable before/after snapshots. It does not make a real reservation. The server restores those activity rows and snapshots on boot. In commit `03a4c8b`, session waterfall arrays and in-memory alerts are not hydrated from the ledger, so run the live interruption act below to populate those two views during the presentation. Read-only tool caches are process-local; the CLI preflight exercises local fallback paths, while the live app warms its filler when Sarvam is configured and fills tool caches on first use.

**Emergency offline rollback** (from a clean checkout):

```powershell
git checkout v1.0.0-gold; python -m backend.seed_stage_demo --reset
```

This uses the local deterministic dataset when external credentials or network access are unavailable. The local booking adapter is a demonstration path, not a live restaurant reservation.

## B. 2-Person Stage Choreography · 120 Seconds

| Time | Lead | Action and line |
|---|---|---|
| 0:00–0:20 | Person A | “A generated confirmation request is not proof the person heard it. The model can finish the text while audio is still buffering—or while the user is interrupting.” |
| 0:20–0:50 | Person B | Hold the orb and ask for a South Indian table for two at MTR. The server's warmed Sarvam filler can play from cached audio while independent tools run. Explain that first-time read queries may miss the process-local cache; local Bengaluru data is the fallback. |
| 0:50–1:25 | Person B + A | As the reservation offer is spoken, Person A interrupts and says “Yes, book it” before the offer finishes. Point to `BARGE-IN: UNVERIFIED` and `STEER`; let the agent repeat the complete offer. |
| 1:25–2:00 | Person B + A | Wait for playback to finish and the green audio status. Person B says “Yes, confirm the table for two at 8 PM.” Person A opens **Enclave Control Plane**: Home counters, Fix hash-linked activity, Trace stages/snapshots, then Analyse causal and WORM checks. The seeded baseline is visible immediately; the live call adds current-session stages and the interception alert. |

## C. Judge Q&A · Crisp Answers

1. **Why not just use a system prompt to ask for confirmation?**
   A prompt is probabilistic and cannot prove the browser finished playing the offer. The enclave gates `execute_reservation` on the playback ACK, an affirmative transcript received later by the server, and exact offer terms.

2. **How do you prevent client clock skew from spoofing the causal gate?**
   `client_ts` is ignored for authorization. The server timestamps playback-ACK receipt and transcript receipt, then requires `transcript_received_server_ts > playback_completed_server_ts`. These are server wall-clock receipt timestamps; this implementation does not claim a monotonic hardware clock.

3. **How did you reduce voice latency with parallel tools and safety checks?**
   Gemini runs with `thinking_budget=0`, tool results and recent model context are pruned, and read-only restaurant lookups use a 300-second cache. The standard filler is warmed at app startup when Sarvam is configured, so repeated filler playback avoids another synthesis request. WORM `fsync` remains on the audit path; we do not claim an unmeasured sub-3-ms bound.

4. **What happens if Places, Graphiti, or Wi-Fi degrades?**
   External calls have a four-second overall deadline and bounded retries. On the first outage the call can consume that deadline; the circuit then opens so subsequent calls fall back quickly to the local Bengaluru dataset or in-process memory and emit a warning.

5. **How does this generalize beyond restaurant bookings?**
   The reusable pattern is completed-audio proof, causal ordering, exact transaction terms, and a hash-chained decision receipt. Payments, prescription refills, and cancellations each need their own domain-specific schemas and safety policies before they are safe to enable.
