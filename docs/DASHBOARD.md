# The dashboard (M9)

```bash
make dashboard      # docker compose up, then open the URL it prints
make demo           # the same thing headlessly, with assertions
make ha             # nginx L4 in front of two gateways
```

`http://localhost:7000/dashboard/` — or `:7001` if macOS AirPlay Receiver
has taken 7000, which it does by default from Monterey onward.

One HTML file, served from the gateway binary's embedded FS. No build
step, no framework, no npm, no CDN. That is a constraint rather than a
preference: the page has to work inside a container that may have no
internet, on a reviewer's machine, from `docker compose up` alone. Charts
are ~150 lines of canvas for the same reason.

---

## What you can do with it

| | |
|---|---|
| **Start load** | `5 / 20 / 50 / 100 / 200` streams, or `stop`. Drives the loadgen container's control API through the gateway. |
| **Break a node** | Click any node: kill, restore, drain, blackhole, slow, 429, corrupt checkpoints, reset. |
| **Watch traffic move** | The stacked traffic chart is backend calls/sec per worker. A kill collapses one band and raises the others. |
| **Watch latency react** | p50/p95/p99 of backend push latency, with every fault and failover marked. |
| **Watch the nodes** | Memory against each container's cgroup limit, queue depth, CPU, sessions, RTF — per worker, and for the gateway. |
| **Isolate one stream** | Click a row for its full event log: partials, revisions, epochs, and the compatibility keys on a failover. |

The demo the whole thing exists for: press **20**, wait for the bands to
settle, click the busiest node, choose **kill**. The band collapses,
traffic redistributes, latency spikes and recovers, `failover_total`
climbs, and `dup finals` stays at 0.

---

## Why SSE

The project already speaks WebSocket, on :7070, for audio. The dashboard
feed is Server-Sent Events on :7000 instead. Four reasons, in order of
how much they mattered:

**1. Reconnection is the feature, not a detail.** `EventSource`
reconnects on its own, with backoff, resending `Last-Event-ID` — so the
server replays the notable-event ring and the browser closes its own gap.
This matters *precisely when the dashboard is doing its job*: you click
Kill, something upstream hiccups, and the page has to come back by itself
rather than showing a frozen chart that looks like a dead system. Over
WebSocket that is hand-written JS reconnect-and-resume logic, in a
milestone with a one-day budget. Here it is zero lines, and it is tested
(`TestLastEventIDReplaysTheGap`).

**2. The traffic is one-directional.** Telemetry goes server→browser.
Control goes browser→server as `POST /api/chaos/{worker}/{action}`, which
is request/response — what HTTP is already good at. Nothing here needs a
bidirectional stream, and picking one would mean inventing framing and a
message-type dispatcher for traffic that has neither.

**3. It shares nothing with the audio socket.** That socket is binary,
sequence-validated, and has a session lifecycle: a browser attaching to it
is a connection that code assumes is a client session. Worse, this
project has a standing `coder/websocket` hazard — cancelling a `Read`'s
context closes the whole connection — that has already caused one real
bug (see STATUS.md, M1). Keeping the dashboard off that transport keeps
it out of that class of mistake entirely.

**4. No dependency, no handshake, debuggable with curl.**

```bash
curl -N localhost:7000/api/events        # the live feed, as text
```

### What SSE costs

Stated rather than discovered later:

- **Text only.** Fine — these are JSON frames.
- **Six connections per origin on HTTP/1.1.** Irrelevant for one
  dashboard tab; would matter if the page opened a stream per panel.
- **Any L7 proxy in front of it must not buffer.** The response headers
  set `X-Accel-Buffering: no`, but that only helps with proxies that
  honour it. The HA profile avoids the problem structurally by proxying at
  **L4** — TCP passthrough cannot buffer a response it never parses. See
  `ha/nginx.conf`.

---

## What goes over the feed, and what deliberately does not

At 200 sessions the fleet produces roughly 1,200 backend pushes a second,
each yielding a `partial` and an `ack` — about 2,400 events/sec of mostly
near-identical text. build-plan.md says "send everything, filter in the
browser", which is right for the event feed and wrong for that firehose;
it was written before the admission ceiling was 200.

So the feed is split three ways:

| | carries | cadence |
|---|---|---|
| **snapshot** | fleet, metrics, latency percentiles, per-worker call counters, newest 50 stream rows + the total | every 250 ms |
| **event** | failovers, resets, speech starts, finals, errors, refusals, chaos actions | as they happen, with an `id` |
| **`GET /api/streams/{id}/log`** | everything for ONE stream, partials included | polled only while a stream is selected |

`ack` never enters a stream's log at all. It is one event per push
carrying a number the stream row already tracks, and at 6/s it would evict
a 256-entry inspector history every 40 seconds — hiding the failover the
inspector exists to show.

Nothing is hidden; the full stream list is one `GET /api/streams` away.
It is just not re-sent four times a second.

---

## The live mic page

`/dashboard/mic.html` — speak into the fleet yourself, **while the load
generator runs**. It is a real client: same WebSocket protocol as
`cmd/loadgen`, same admission controller, same router.

Everything else here observes the system from outside. This is the one
place a human can hear their own words being served by a fleet that is
simultaneously carrying 30 synthetic streams, and watch:

- **which worker answered**, and whether the session is pinned to it or
  running on the shared KV tier (a `kv:`-prefixed handle means the latter);
- **what is in the KV cache** — the actual tensor list, read from the
  worker's `/v1/kv/layout`, which derives it from the ONNX graph. Worth
  looking at once: zipformer's 1.09 MB of "KV cache" is only 467 KB of
  attention K/V and 614 KB of *convolution* state, while whisper's 5.51 MB
  is perfectly symmetric decoder self-attention;
- **how audio is chunked**, and how much of it the VAD gated as silence —
  gated frames are still journalled (a replay must reproduce them) but
  never become a backend call, so the panel reports the calls avoided;
- **its own versions in the tier**, which should stay at one or two rather
  than growing: a version is retired once its successor is durable
  ([KVCACHE.md](KVCACHE.md)).

Capture is exactly 16 kHz mono s16le because the gateway refuses anything
else rather than transcoding. Microphone access needs a secure context, so
open it on `localhost`, not a LAN IP.

`make test-mic` verifies the page's framing code against a live gateway by
**running that code** — the protocol block is lifted out of the page rather
than reimplemented, because a second copy in the test could be correct
while the page stayed broken.

## Endpoints

```
GET  /dashboard/                        the page
GET  /dashboard/mic.html                live microphone client
GET  /api/kv[?session=<id>]             KV layout per family + tier contents
GET  /api/events                        SSE: snapshots + discrete events
GET  /api/streams                       full stream list
GET  /api/streams/{id}/log              one stream's history, partials included
GET  /api/nodes                         per-node resource time series
POST /api/chaos/{worker}/{action}       kill|restore|drain|slow|blackhole|429|corrupt|reset
POST /api/load/{n}                      ramp the load generator (0 = stop)
GET  /api/load                          generator status
GET  /api/debug/metrics                 the counters (pre-existing)
GET  /api/debug/workers                 the router's fleet view (pre-existing)
```

The control plane adds **no server capability**. Every action is one the
headless chaos scripts already drive (`cmd/chaostest/faults.go`), so
anything demonstrable by clicking is also assertable in CI — and if the
two ever disagree, the scripts are the truth.

### The two fault-injection ports

Request-level faults (slow, blackhole, 429, corrupt) are module state
inside the worker process, served on its own port. Process-level faults
(die, restore) cannot be — a process that has called `os._exit` cannot
resurrect itself — so those go to the supervisor parent on a second port.
Routing both through one port would mean a killed worker could never be
told to come back. Tested, because getting it wrong is quiet:
`TestProcessFaultsGoToTheSupervisorAndRequestFaultsToTheWorker`.

### Drain is the one action with no worker endpoint behind it

"Stop accepting new sessions, finish what you have" is a statement about
**selection**, and selection lives in the gateway. `router.Worker.
SetDraining` makes a worker ineligible for `Pick` while leaving every
in-flight session untouched.

It is a flag, not a fourth `Status`. Status transitions are driven by
observed outcomes and recover on their own timers, so modelling an
operator decision as one would mean a successful health probe silently
un-draining a worker somebody is deliberately taking out of service.

---

## Where the numbers come from

**Latency** is the same sample the router acts on. `dispatchChunk`
measures one backend push and reports it to *both* the router's health
window and the hub. A latency chart that disagreed with the ejection
policy it is meant to explain would be worse than no chart.

**Traffic per worker** is differenced in the browser from cumulative
counters, not computed server-side — because the server would have to
pick a window, and the right window is "whatever this viewer's snapshot
interval actually was", including the gaps when a tab was backgrounded.

**Node resources** come from each worker's own `/health`, polled once a
second by the gateway (not by the browser: a page that fans out to six
origins stops working the moment the fleet is not port-mapped, and two
tabs polling independently would build two different histories of one
fleet). `worker/resources.py` reads `/proc` and the cgroup filesystem —
no psutil, nothing added to the image.

Memory is drawn **against each container's cgroup limit**, because that is
the only version of the graph that can warn you about anything: 900MB is
comfortable at 1536M and fatal at 1024M, and the failure it misses — the
OOM killer — looks exactly like the SIGKILL the chaos suite injects on
purpose.

Readings are nullable throughout. "Not measured on this platform" and
"measured as zero" are different facts, and a chart that renders a missing
reading as 0 invents a healthy-looking flat line out of an absent one. A
node that stops answering draws a **gap**, not a line at the floor.

**Queue depth** is now real. `/health` used to report a hardcoded `0`;
the worker counts inference requests accepted (`inflight`) and requests
actually executing in the thread pool (`running`), and reports the
difference as the backlog. `state_bytes` is still an honest `0` — a real
adapter's state is an onnxruntime-owned C++ object Python cannot size
without guessing.

Nothing in the router reads any of it. Selection stays a function of
health, latency, rate budget and capability; feeding memory pressure into
it is a real design question with its own failure mode — a worker near its
limit being starved of exactly the traffic that would let it finish and
free state — not a free upgrade because the number is available now.

---

## The observation layer does not participate

`internal/dash.Hub` never blocks, never errors, and never allocates
unboundedly. A subscriber that stops reading is skipped, not waited for
(`TestBroadcastNeverBlocksOnAStalledSubscriber`). A browser that cannot
keep up loses individual events and resynchronises from the next snapshot
— a far better failure than back-pressuring a transcription session.

A dashboard that can slow down the thing it measures is worse than no
dashboard, because it makes the measurement wrong in exactly the overload
regime the measurement exists for.

There is **one** tap, in `writeLoop`: everything the client is told, the
dashboard sees. Four facts the client has no business knowing — which
worker, its compatibility key, how long a push took, and that a failover
swapped workers — get explicit calls. The alternative, a hub call beside
each of the ten `Emitter` sites, is the version that drifts the first time
someone adds an event type.

A nil `*Hub` is fully functional, like `bifrost.Client`, so the gateway's
own tests wire nothing and need no branching at the call sites.

---

## Known limits

- **Under HA, the stream list is one gateway's.** Each gateway keeps its
  own in-process hub, so a dashboard reached through nginx shows whichever
  instance it landed on. The fleet view and node graphs are identical
  either way — both gateways poll the same workers — but the stream list
  is not. Ports 7001/7002 reach each gateway directly. Fixing it properly
  means a shared store or fanning the SSE feeds together, which is a real
  piece of work and not what the HA profile is demonstrating.

- **HA does not make an in-flight session survive its gateway.** The
  gateway holds the audio journal and checkpoints in memory; killing it
  takes both. The client reconnects and resumes from its last ack
  (`cmd/loadgen -reconnect-every`), so the transcript restarts from the
  last committed final, not the last partial. Replicating the journal
  between gateways is a deliberately larger system — see DECISIONS.md.

- **`live_lag_ms` is not plotted.** build-plan.md asks for it, and it is
  genuinely the metric that reveals whether you are still real-time when
  individual calls look fast. But it is audio-time versus wall-time *for
  the client*, and the gateway cannot distinguish a client that has
  fallen behind from one that is deliberately pacing slowly. Computing it
  gateway-side would produce a confident, wrong number. `cmd/loadgen`
  measures it where it is knowable.

- **Load is spread unevenly, and that is visible.** The router scores on
  latency as well as outstanding count, so the fastest worker accumulates
  most sessions — a 12-stream run distributes roughly 9/1/2 across three
  workers, with some streaming workers picked not at all. This is router
  policy, not a dashboard artefact; the dashboard is just the first place
  it became obvious. `scripts/demo.sh` selects its victim at runtime
  because of it.
