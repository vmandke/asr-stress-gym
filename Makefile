.PHONY: check dashboard chaos kv-quant live mic test-mic up down reset build test test-go test-worker models corpus

# --- the demo ---------------------------------------------------------
#
dashboard:
	docker compose up -d --build
	@echo "dashboard: http://localhost:$${GATEWAY_DASHBOARD_PORT:-7000}/dashboard/"

# --- the evidence -----------------------------------------------------
#
# Chaos checks exercise a Bifrost primary failure and admission overload in
# the live dynamic-fleet topology. Run `make live` first; see docs/CHAOS.md.
chaos:
	./scripts/chaos.sh

# What does quantizing the KV cache cost? (fp32 / fp16 / int8)
kv-quant: worker/.venv/bin/pytest
	cd worker && .venv/bin/python ../scripts/kv_quant_bench.py

# The whole live setup in one command: fleet + per-family KV tiers +
# Bifrost + stateless streaming + load + the microphone page.
#   make live            # starts idle (zero background streams)
#   make live STREAMS=20 # everything up with background load
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

# Remove the whole demo, including workers launched from the dashboard.
# This preserves named volumes. Use `make reset` only when their data should
# be discarded too.
down:
	@workers="$$(docker ps -aq --filter label=asr-stress-gym.managed=true)"; \
	 if [ -n "$$workers" ]; then docker rm -f $$workers; fi
	docker compose down --remove-orphans

reset:
	@workers="$$(docker ps -aq --filter label=asr-stress-gym.managed=true)"; \
	 if [ -n "$$workers" ]; then docker rm -f $$workers; fi
	docker compose down --remove-orphans -v

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
