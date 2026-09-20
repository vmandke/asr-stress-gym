# Worker image. One image for the whole fleet, differentiated at runtime by
# env vars (WORKER_ID, MODEL, ADAPTER) so docker-compose.yml stays one
# Dockerfile — see docs/implementation-plan.md "Model fleet and deployments".
#
# CPU-only int8 by design (docs/build-plan.md "Platform"). Model weights are
# baked in at BUILD time by models/fetch.sh below; a worker never downloads
# anything when a container starts. Every fleet member shares this image, so
# the ~365MB of weights is one layer on disk, not five copies — which is why
# baking all four models into one image beats four per-model images here.
#
# NOTE: the build context is the REPOSITORY ROOT, not worker/, because the
# weights live in models/. docker-compose.yml sets `context: .` with
# `dockerfile: worker/Dockerfile` accordingly.
FROM python:3.11-slim AS weights

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY models/fetch.sh /fetch/fetch.sh
# A separate stage so that re-running fetch.sh is cached independently of
# the Python layers: editing an adapter must not re-download 365MB.
RUN MODELS_DIR=/weights bash /fetch/fetch.sh

FROM python:3.11-slim

WORKDIR /app
COPY worker/pyproject.toml ./
COPY worker/server.py worker/state.py worker/supervisor.py worker/resources.py worker/kvtier.py ./
COPY worker/adapters ./adapters
COPY worker/kvcache ./kvcache

# fastapi/uvicorn for the HTTP surface (docs/PROTOCOL.md); sherpa-onnx,
# faster-whisper and ctranslate2 for the real adapters. All five adapters'
# runtimes are installed in the one image; adapters/registry.py imports
# lazily so a container only pays to load the one ADAPTER names.
RUN pip install --no-cache-dir .

# adapters/weights.py looks here first (then at the repo checkout, for
# `make test-worker` on a developer's host).
COPY --from=weights /weights /app/models

EXPOSE 9000 9001
# supervisor.py, not server.py, directly — it spawns server.py as a child
# and answers /admin/die + /admin/restore on 9001 even after that child
# is gone. See docs/PROTOCOL.md "Fault injection".
CMD ["python3", "supervisor.py"]
