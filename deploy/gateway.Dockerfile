# Gateway image. Build context is the repo root (needs go.mod + cmd/ + internal/).
#
# CPU-only, no CUDA — see docs/build-plan.md "Platform": a reviewer may not
# be on Apple Silicon, and GPU is a documented override, never the happy path.
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/gateway ./cmd/gateway

# Alpine (not distroless) so the compose healthcheck can run curl in-container,
# matching docs/build-plan.md's healthcheck example.
FROM alpine:3.20
RUN apk add --no-cache curl
COPY --from=build /out/gateway /gateway
EXPOSE 7000 7070
ENTRYPOINT ["/gateway"]
