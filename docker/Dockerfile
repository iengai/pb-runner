# pb-runner image (P6.1 — placeholder, not yet built)
#
# Same container contract as pbtb-rust/deploy/passivbot-image (docs/CONTRACT.md):
# env BUCKET, USER_ID, BOT_ID; the binary downloads its config and keys itself.
#
# Build one image per engine line:
#   docker buildx build --platform linux/arm64 --build-arg ENGINE=engine-v8 -t pb-runner:8-v8.1.0-arm64 -f docker/Dockerfile .

ARG ENGINE=engine-v8

FROM rust:1.90-bookworm AS builder
ARG ENGINE
WORKDIR /src
COPY . .
RUN cargo build --release -p pb-runner --no-default-features --features ${ENGINE}

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /src/target/release/pb-runner /usr/local/bin/pb-runner
# The binary creates /app/configs and writes configs/<BOT_ID>.json + api-keys.json (P4.5).
ENTRYPOINT ["/usr/local/bin/pb-runner"]
