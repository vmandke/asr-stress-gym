# Worker image. Build context is worker/. One image, differentiated at
# runtime by env vars (WORKER_ID, MODEL, COMPAT_KEY_HASH, and from M5
# onward ADAPTER) so the fleet in docker-compose.yml stays one Dockerfile
# — see docs/implementation-plan.md "Model fleet and deployments".
#
# CPU-only int8 by design (docs/build-plan.md "Platform"); model weights
# are baked in at build time from M5 onward via models/fetch.sh, never
# downloaded at container start.
FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml ./
COPY server.py ./
COPY adapters ./adapters

# M0: stdlib http.server only, so there is nothing to install yet.
# `pip install .` starts here once pyproject.toml grows dependencies at
# M1 (fastapi/uvicorn) and M5 (per-adapter: numpy, sherpa-onnx, ...).

EXPOSE 9000
CMD ["python3", "server.py"]
