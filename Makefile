.PHONY: check start demo dashboard up down build test test-go test-worker models corpus chaos bench kv-quant stateless-ab stateless-proof live mic test-mic

# --- the demo ---------------------------------------------------------
#
# One command. Brings the stack up, revives anything earlier chaos killed,
# applies load, prints the dashboard URL. STREAMS=n to choose the load.
start:
	./scripts/start.sh $(STREAMS)

# The same thing headlessly, with assertions: load, kill the busiest
# worker, prove the sessions recovered and no final was duplicated.
demo:
	./scripts/demo.sh

dashboard:
	docker compose up -d --build
	@echo "dashboard: http://localhost:$${GATEWAY_DASHBOARD_PORT:-7000}/dashboard/"

# --- the evidence -----------------------------------------------------
#
# Chaos scenarios assert what the dashboard shows. 12 and 13 need the kv
# profile (worker-f/g/h) and SKIP without it rather than failing.
chaos:
	./scripts/chaos.sh

bench:
	./scripts/bench.sh

# What does quantizing the KV cache cost? (fp32 / fp16 / int8)
kv-quant:
	cd worker && .venv/bin/python ../scripts/kv_quant_bench.py

# Pinned vs stateless: the same online load run both ways, side by side.
#   make stateless-ab            # 30 streams, 60s each
#   make stateless-ab ARGS="8 20s"
stateless-ab:
	./scripts/stateless_ab.sh $(or $(ARGS),30 60s)

# One session's chunks deliberately fanned across different workers in a
# family, through Bifrost, proving the transcript survives it.
stateless-proof:
	python3 scripts/prove_stateless_bifrost.py

# The whole live setup in one command: fleet + per-family KV tiers +
# Bifrost + stateless streaming + load + the microphone page.
#   make live            # 20 background streams
#   make live STREAMS=0  # everything up, no load
live:
	./scripts/live.sh $(STREAMS)

# Speak into the fleet yourself, while the load generator runs.
# Microphone capture needs a secure context, and localhost counts as one —
# so open the printed URL rather than a LAN IP.
mic:
	@port=$${GATEWAY_DASHBOARD_PORT:-7000}; \
	 lsof -nP -iTCP:$$port -sTCP:LISTEN >/dev/null 2>&1 || true; \
	 echo "open http://localhost:$$port/dashboard/mic.html"

# Does the mic page's own framing code speak this gateway's protocol?
# Runs THAT code (lifted out of the page) against the live gateway with a
# corpus clip — a reimplementation here could pass while the page is broken.
test-mic:
	node scripts/test_mic_protocol.mjs

# --- build and test ---------------------------------------------------
check:
	./scripts/check_env.sh

up:
	docker compose up --build

down:
	docker compose down -v

build:
	go build ./...
	go vet ./...

test-go:
	go test ./... -race

worker/.venv/bin/pytest:
	cd worker && python3 -m venv .venv && .venv/bin/pip install --quiet --upgrade pip && .venv/bin/pip install --quiet -e ".[dev]"

test-worker: worker/.venv/bin/pytest
	cd worker && .venv/bin/python -m pytest tests/ -v

test: test-go test-worker

# --- assets -----------------------------------------------------------
#
# Weights are fetched at image build time; this is for running adapters on
# the host. The conformance suite SKIPS adapters whose weights are absent,
# so a fresh clone still gets a green `make test`.
models:
	./models/fetch.sh

corpus:
	./scripts/gen_corpus_large.py
