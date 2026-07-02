# syntax=docker/dockerfile:1

# ── build ──────────────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache de dependencias.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Binario estatico.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-s -w" \
    -o /out/data-management ./cmd/data-management

# ── runtime ────────────────────────────────────────────────────────────────
# alpine (no scratch) porque el healthcheck del compose usa wget.
FROM alpine:3.20
RUN apk add --no-cache wget ca-certificates && adduser -D -u 10001 app
USER app
WORKDIR /app
COPY --from=build /out/data-management /app/data-management

# HTTP (REST API + /health + /metrics) y gRPC (ReadingService). Override con
# HTTP_PORT / GRPC_PORT.
EXPOSE 8080 9090

ENTRYPOINT ["/app/data-management"]
