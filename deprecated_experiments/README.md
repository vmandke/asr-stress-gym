# deprecated_experiments

Code that was useful while building the system and is not on the demo path.
Moved rather than deleted: all of it worked, some of it produced numbers
quoted in `docs/`, and none of it is worth carrying in the main tree.

**The demo is:** `make start` → open the dashboard → click a node → Kill →
watch traffic move and the session recover. `make chaos` is its headless,
asserting twin. Anything not serving one of those two lives here.

| Moved | Was | Why it left |
|---|---|---|
| `cmd/inspect/` | Three CLI subcommands: `chunks`, `trace`, `fleet` | Superseded by the dashboard. The inspector pane shows per-stream flow live; `fleet` is `/api/debug/workers`; `chunks` was written to author `docs/ARCHITECTURE.md`, which is now written |
| `cmd/smoketest/` | M1's "one stream end to end" | `cmd/chaostest` covers the same path and asserts more |
| `cmd/matrix/` | Generated `docs/compat-matrix.md` from the live fleet | A doc generator, run once per fleet change, not part of any demo |
| `ha/`, `docker-compose.ha.yml` | nginx L4 in front of two gateways | Genuinely works (measured: new sessions survive a gateway kill, in-flight ones do not). Not on the demo path, and it doubles the container count on a laptop |
| `worker/adapters/whisper_*.py`, `buffered.py` | The two Whisper workers (`worker-d`/`worker-e`) | **They never took online traffic.** Both advertise `streaming: false`, so `router.Pick` filters them out of every online session — visible on the dashboard as two permanently idle nodes. `whisper_ct2` additionally cannot expose a KV cache at all (CTranslate2 keeps it internal), which is the one thing this fleet is now about |
| `scripts/verify_m0.sh`, `vad_economics.sh`, `measure_rtf.py`, `bifrost_*.sh` | One-shot measurement scripts | Each produced a number that now lives in `docs/`. Re-running them is archaeology, not demonstration |

## The sherpa-onnx adapters and the mock

An earlier draft of this file argued these four should stay in the main
tree — the sherpa adapters (`zipformer.py`, `conformer_ctc.py`,
`sherpa_online.py`) as a control group for `checkpoint_degraded_total`, and
`mock.py` as the only adapter needing no weights. They moved anyway, and
the reasons did not survive contact:

- The **control group already exists without them.** Every KV adapter
  degrades to audio replay whenever the compatibility key differs, which
  is the cross-family case the fleet exercises constantly. A second
  never-serializing adapter adds no comparison the fleet does not make.
- **`mock.py` was not what kept `make test` green.** The conformance suite
  skips on `NEEDS_WEIGHTS` when no weights root exists, so a fresh clone
  passes with no mock in the registry at all.

`registry.py` lists only `zipformer_kv`, `conformer_ctc_kv` and
`whisper_kv`. Nothing in the main tree imports anything in this directory.

## Running anything in here

These were moved wholesale and their imports still point at the main tree.
They are kept for reference and for recovering a measurement, not as a
maintained surface. `git mv` them back if one is needed again.
