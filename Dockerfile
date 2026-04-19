FROM golang:1.25-bookworm AS builder

WORKDIR /src
COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -ldflags "-s -w" -o /out/firn  ./cmd/firn  && \
    CGO_ENABLED=1 GOOS=linux go build -ldflags "-s -w" -o /out/compact ./cmd/compact

# ── runtime ──────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    adduser --system --no-create-home --uid 1000 firn

COPY --from=builder /out/firn    /firn
COPY --from=builder /out/compact /compact

USER firn

ENTRYPOINT ["/firn"]
