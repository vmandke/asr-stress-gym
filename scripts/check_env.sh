#!/usr/bin/env bash
# Verifies the local toolchain this project needs. Read-only; exits non-zero
# with a clear message on the first missing/broken piece.
set -euo pipefail

# Homebrew's `docker` CLI (brew install docker) talks to Docker Desktop's
# daemon fine, but doesn't inherit Docker Desktop's own bin/ on PATH, so it
# can't find docker-credential-desktop for registry credential lookups
# (needed even for anonymous public-image pulls). Self-contained here so no
# system file outside this repo is touched; a no-op where that path doesn't
# exist (Linux, CI, a machine without Docker Desktop).
if [ -d "/Applications/Docker.app/Contents/Resources/bin" ]; then
  export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
fi

fail() { echo "MISSING: $1" >&2; exit 1; }

command -v go >/dev/null 2>&1 || fail "go (https://go.dev/dl/)"
command -v python3 >/dev/null 2>&1 || fail "python3"
command -v docker >/dev/null 2>&1 || fail "docker CLI"
docker version >/dev/null 2>&1 || fail "docker daemon not reachable (start Docker Desktop, or 'colima start')"
docker compose version >/dev/null 2>&1 || fail "docker compose plugin"
command -v make >/dev/null 2>&1 || fail "make"

echo "go:      $(go version)"
echo "python:  $(python3 --version)"
echo "docker:  $(docker version --format '{{.Client.Version}} (client) / {{.Server.Version}} (server)')"
echo "compose: $(docker compose version --short)"
echo "arch:    $(uname -m)"
echo "OK"
