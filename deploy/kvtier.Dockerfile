# Shared KV-cache tier image. Build context is the repo root.
#
# Same shape as gateway.Dockerfile: CPU-only, static binary, alpine so the
# compose healthcheck can run wget in-container. The tier holds opaque
# blobs and never parses one, so it needs nothing from worker/ — see
# cmd/kvtier/main.go for why that separation is deliberate.
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -o /out/kvtier ./cmd/kvtier

FROM alpine:3.20
COPY --from=build /out/kvtier /kvtier
EXPOSE 9500
ENTRYPOINT ["/kvtier"]
