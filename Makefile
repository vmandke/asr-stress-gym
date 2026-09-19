.PHONY: check up down build smoke test matrix demo chaos bench

# Available now (M0).
check:
	./scripts/check_env.sh

up:
	docker compose up --build

down:
	docker compose down -v

build:
	go build ./...
	go vet ./...

# Land at their milestone (docs/implementation-plan.md). Each fails loudly
# rather than pretending to pass, so `make <target>` is always an honest
# signal of what's actually built.
smoke:
	@echo "make smoke: not implemented until M1 (one stream end to end)" >&2; exit 1

test:
	go test ./...
	@echo "worker adapter conformance suite: not implemented until M1/M5" >&2; exit 1

matrix:
	@echo "make matrix: not implemented until M6 (docs/compat-matrix.md)" >&2; exit 1

demo:
	@echo "make demo: not implemented until M3/M9 (kill a worker, watch it recover)" >&2; exit 1

chaos:
	@echo "make chaos: not implemented until M3 (scenarios 2,3,5 first)" >&2; exit 1

bench:
	@echo "make bench: not implemented until M8 (benchmarks A-F)" >&2; exit 1
