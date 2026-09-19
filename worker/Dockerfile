# Worker image. Build context is worker/. One image, differentiated at
# runtime by env vars (WORKER_ID, MODEL, and from M5 onward ADAPTER) so
# the fleet in docker-compose.yml stays one Dockerfile — see
# docs/implementation-plan.md "Model fleet and deployments".
#
# CPU-only int8 by design (docs/build-plan.md "Platform"); model weights
# are baked in at build time from M5 onward via models/fetch.sh, never
# downloaded at container start.
FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml ./
COPY server.py state.py supervisor.py ./
COPY adapters ./adapters

# M1: fastapi/uvicorn for the real open/push/flush/restore/checkpoint/
# close surface. M5 adds one dependency per adapter as it lands: numpy,
# sherpa-onnx, faster-whisper / ctranslate2, onnxruntime.
RUN pip install --no-cache-dir .

EXPOSE 9000 9001
# supervisor.py, not server.py, directly — it spawns server.py as a child
# and answers /admin/die + /admin/restore on 9001 even after that child
# is gone. See docs/PROTOCOL.md "Fault injection".
CMD ["python3", "supervisor.py"]
