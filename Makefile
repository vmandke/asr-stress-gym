.PHONY: check up down build smoke test test-go test-worker matrix demo chaos bench

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

test: test-go test-worker

# Land at their milestone (docs/implementation-plan.md). Each fails loudly
# rather than pretending to pass, so `make <target>` is always an honest
# signal of what's actually built.
matrix:
	@echo "make matrix: not implemented until M6 (docs/compat-matrix.md)" >&2; exit 1

demo:
	@echo "make demo: not implemented until M3/M9 (kill a worker, watch it recover)" >&2; exit 1

chaos:
	./scripts/chaos.sh

bench:
	@echo "make bench: not implemented until M8 (benchmarks A-F)" >&2; exit 1
