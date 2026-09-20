# The fleet manager is intentionally a separate, tiny binary. It is the only
# demo component granted the Docker socket; request-serving gateway/Bifrost
# containers never receive that privilege.
FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY cmd/fleetmanager ./cmd/fleetmanager
RUN CGO_ENABLED=0 go build -o /out/fleetmanager ./cmd/fleetmanager

FROM alpine:3.20
RUN apk add --no-cache curl
COPY --from=build /out/fleetmanager /fleetmanager
EXPOSE 8091
ENTRYPOINT ["/fleetmanager"]
