.PHONY: check up down build smoke test test-go test-worker models rtf corpus inspect-chunks inspect-trace inspect-fleet vad-economics bifrost-offline bifrost-fallback matrix demo chaos bench

# Available now (M0-M1).
check:
	./scripts/check_env.sh

up:
	docker compose up --build

down:
	docker compose down -v

build:
	go build ./...
	go vet ./...

# M1: one stream end to end, against the real containerized stack (not
# cmd/gateway's own tests, which use a fake worker) — see docs/PROTOCOL.md
# and docs/implementation-plan.md's M1 done-when bar.
smoke:
	./scripts/smoke.sh

test-go:
	go test ./... -race

# Creates worker/.venv on first run (it's git-ignored, so a fresh clone
# has none) and leaves it in place for subsequent runs to reuse.
worker/.venv/bin/pytest:
	cd worker && python3 -m venv .venv && .venv/bin/pip install --quiet --upgrade pip && .venv/bin/pip install --quiet -e ".[dev]"

test-worker: worker/.venv/bin/pytest
	cd worker && .venv/bin/python -m pytest tests/ -v

# Weights for the real adapters. NOT a prerequisite of anything: the image
# fetches them itself at build time (worker/Dockerfile), and the adapter
# conformance suite SKIPS the real adapters rather than failing when they
# are absent — a fresh clone that has never run this must still get a
# green `make test`. Run it to exercise the real adapters on your host.
models:
	./models/fetch.sh

# Regenerates docs/RTF.md from the adapters as they are right now. Needs
# `make models` first, and measures the models rather than the stack —
# end-to-end latency is a different number and belongs to `make bench` (M8).
rtf: worker/.venv/bin/pytest
	cd worker && .venv/bin/python ../scripts/measure_rtf.py

test: test-go test-worker

# --- inspection utilities (cmd/inspect) ------------------------------
#
# Each answers one question the source otherwise only answers by being
# read. See docs/ARCHITECTURE.md, which is written from their output.

# What does the gateway DO to this audio? Offline — needs nothing running.
# CLIP=... to choose; defaults to a silence-heavy clip because that is
# where chunking and VAD gating are most visible.
CLIP ?= $(shell find corpus/large/silence_heavy -name '*.wav' 2>/dev/null | head -1)
inspect-chunks:
	go run ./cmd/inspect chunks $(CLIP)

# Where does the TIME go? Live — one real session, every event stamped.
inspect-trace:
	go run ./cmd/inspect trace $(TRACE_CLIP)

# What is the fleet right now — as the ROUTER sees it, not as each worker
# reports itself. A worker can be healthy and still be ejected.
inspect-fleet:
	go run ./cmd/inspect fleet

# Generate the large corpus (macOS only; git-ignored, ~200MB).
corpus:
	./scripts/gen_corpus_large.py

# What does gating silence actually save? Needs a running stack.
vad-economics:
	./scripts/vad_economics.sh

# Runnable Bifrost examples. Both use a complete WAV request; neither routes
# a stateful streaming handle through Bifrost — see docs/BIFROST-KVCACHE.md.
bifrost-offline:
	./scripts/bifrost_offline.sh

bifrost-fallback:
	./scripts/bifrost_fallback.sh

# Land at their milestone (docs/implementation-plan.md). Each fails loudly
# rather than pretending to pass, so `make <target>` is always an honest
# signal of what's actually built.
matrix:
	. ./scripts/check_env.sh && docker compose --profile models up -d --build
	MATRIX_WORKER_URLS="worker-mock=http://localhost:$${WORKER_MOCK_PORT:-18000},worker-a=http://localhost:$${WORKER_A_PORT:-18001},worker-b=http://localhost:$${WORKER_B_PORT:-18002},worker-c=http://localhost:$${WORKER_C_PORT:-18003},worker-d=http://localhost:$${WORKER_D_PORT:-18004},worker-e=http://localhost:$${WORKER_E_PORT:-18005}" go run ./cmd/matrix

demo:
	@echo "make demo: not implemented until M3/M9 (kill a worker, watch it recover)" >&2; exit 1

chaos:
	./scripts/chaos.sh

bench:
	@echo "make bench: not implemented until M8 (benchmarks A-F)" >&2; exit 1
