# Protocol

The wire contract, decided once before any code that depends on it (per
[build-plan.md](build-plan.md), "Protocol"). Two independent surfaces:
client↔gateway over WebSocket, and gateway↔backend over HTTP. Nothing here
describes audio *processing* — VAD, chunk-cutting policy, reframing — that
is entirely internal to `internal/audio`
([implementation-plan.md](implementation-plan.md), "The audio boundary")
and never appears on either wire.

Every event type below is part of the permanent contract even though M1
only wires a subset. Each entry says which milestone actually emits it, so
this file doesn't need rewriting per milestone — see
[STATUS.md](STATUS.md) for current build status.

## Client → Gateway (WebSocket, binary frames)

Binary, not JSON/base64: base64 inflates payloads by a third and a stream
sends up to 50 messages/sec (build-plan.md, "Client to gateway").

```
byte 0         type        1 = AUDIO, 2 = CONTROL
bytes 1-8      seq         uint64 big-endian
bytes 9-12     capture_ms  uint32 big-endian, client clock, relative
bytes 13-16    num_samples uint32 big-endian  <- the payload's own duration
bytes 17+      payload     PCM s16le (AUDIO), or UTF-8 JSON (CONTROL)
```

`num_samples` is authoritative and never inferred from arrival cadence or
message size (invariant 1). The gateway computes `duration_ms =
num_samples * 1000 / sample_rate_hz` and validates
`len(payload) == num_samples * 2` (s16le, mono — see
[DECISIONS.md](DECISIONS.md) / [FAQ.md](FAQ.md) for why this exact
format). **A frame that fails this check is a protocol violation, not a
transient loss**: unlike a sequence gap (which produces `discontinuity`
and continues), a duration mismatch emits `error` and the session closes.
Continuing on unverified framing risks exactly the silent corruption
invariant 1 exists to prevent. **Wired at M1.**

A client may batch several nominal frames into one message (larger
`num_samples`, one `seq`); the chunker accumulates by duration so this
changes nothing downstream. **Wired at M1** (pass-through pipeline; see
`internal/audio`).

### `session.start` (CONTROL)

```json
{
  "type": "session.start",
  "mode": "online",
  "sample_rate_hz": 16000,
  "encoding": "pcm_s16le",
  "channels": 1,
  "nominal_frame_ms": 20
}
```

`mode` is `online` or `offline`. `sample_rate_hz`/`encoding`/`channels`
must exactly equal the locked format (16000 / `pcm_s16le` / 1) — the
server rejects anything else rather than transcoding or guessing
(`error`, connection closed before a session is created). `nominal_frame_ms`
is a hint for buffer sizing only, never used to compute duration.
**Wired at M1.**

### `session.end` (CONTROL)

```json
{ "type": "session.end" }
```

Client-requested graceful finalization: flush any pending audio, emit a
`final` for the open utterance, close the socket. Exists because M1 has no
VAD-driven endpointing yet (`speech.start`/auto-finalization land at M4);
until then, an utterance's only finalization boundary is this message or
the connection closing. **Wired at M1.**

## Gateway → Client (WebSocket, JSON text frames)

Events are comparatively rare (one per dispatched chunk, not one per audio
frame — roughly 6/sec at a 160ms online chunk versus the ~50/sec audio
frame rate), so they go over ordinary JSON text frames rather than the
binary format. Readability wins where the rate argument doesn't apply.

| Event | Contract | Wired at |
|---|---|---|
| `ack` | Cumulative: `highest_contiguous_seq` the gateway has durably accepted. Lets a reconnecting client know what it can drop from its own retry ring ([implementation-plan.md](implementation-plan.md) defect #7). Emitted once per dispatched chunk. | **M1** |
| `speech.start` | VAD detected speech; a new utterance opened. | M4 (VAD) |
| `partial` | Provisional; **replaces** the previous partial entirely. | **M1** (mock adapter text) |
| `partial.reset` | Discard what is displayed; a failover occurred. | M3 (failover) |
| `final` | Immutable; exactly one per utterance; carries its audio range. | **M1** (on `session.end`) |
| `discontinuity` | Audio was lost between these sequence numbers. | **M1** |
| `overloaded` | Admission refused; retryable. | M7 (backpressure) |
| `error` | Terminal for this session. | **M1** |

```json
{ "type": "ack", "session_id": "s1", "highest_contiguous_seq": 108 }

{ "type": "partial", "session_id": "s1", "utterance_id": "u1",
  "revision": 3, "failover_epoch": 0, "text": "..." }

{ "type": "discontinuity", "session_id": "s1",
  "seq_start": 42, "seq_end": 51 }

{ "type": "final", "session_id": "s1", "utterance_id": "u1",
  "seq_start": 0, "seq_end": 220, "text": "..." }

{ "type": "error", "session_id": "s1", "reason": "duration_mismatch" }
```

Guarantees (see `internal/session` for enforcement, one place only):

- `revision` strictly increases within an utterance.
- No event follows a `final` for the same `utterance_id`.
- Re-delivering an identical `final` is a no-op — delivery is at-least-once.
- `failover_epoch` is always `0` until M3; present now so the wire shape
  never changes when failover lands.

## Gateway → Backend (HTTP)

Stateful session semantics behind a small HTTP surface, matching
`internal/backend.Client` exactly. `push` carries a **binary body**, not
base64 — the doc's own base64 argument applies just as much on this hop
([implementation-plan.md](implementation-plan.md) defect #5).

```
POST /v1/stream/open
  body: {"session_id", "sample_rate_hz", "mode"}
  -> 200 {"handle", "compatibility_key_hash", "capabilities", "generation"}

POST /v1/stream/push
  headers: X-Handle, X-Seq-Start, X-Seq-End, X-Expected-Generation
  body:    raw PCM bytes, Content-Type: application/octet-stream
  -> 200 {"text", "last_seq_applied", "generation"}
  -> 409 {"error": "stale_generation"}           <- compare-and-commit rejection

POST /v1/stream/flush
  body: {"handle"}
  -> 200 {"text", "final": true}

POST /v1/stream/restore        # same-model failover only
  body: {"checkpoint_blob"}    # base64 OK here: infrequent, never the hot path
  -> 200 {"handle", "generation", "last_seq_applied"}
  -> 501 {"error": "not_supported"}              <- capabilities.serializable == false

POST /v1/stream/close
  body: {"handle"}
  -> 200 {}

GET /health
  -> WorkerAdvert (docs/build-plan.md "Worker advertisement")
```

`capabilities` (returned from `open`) is the fleet's addition to the
original contract — see [implementation-plan.md](implementation-plan.md),
"Capability-aware routing": `streaming`, `serializable`, `endpointing`,
`modes`, `min_chunk_ms`, `max_chunk_ms`. A router (M3+) filters on these
before it ever scores a candidate.

Two fields carry the correctness weight, both **wired at M1**:

- **`last_seq_applied`** makes replay idempotent — the worker discards
  anything at or below what it already consumed.
- **`generation` / `expected_generation`** is compare-and-commit — a
  mismatch means the caller's view of state is stale, and the write is
  rejected rather than silently corrupting the session.

`open`/`push`/`flush`/`close` are wired at M1 against the mock adapter.
`restore` exists in the interface at M1 (so the contract is complete and
stable) but has no real caller until M3.
