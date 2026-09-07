# pb-runner image (docs/PLAN.md P6.1).
#
# Same container contract as pbtb-rust/deploy/passivbot-image (docs/CONTRACT.md):
# ECS injects BUCKET / USER_ID / BOT_ID; the binary itself downloads
# s3://$BUCKET/$USER_ID/$BOT_ID/$BOT_ID.json and api-keys.json (exit codes
# 10/20/21/22 as the Python entrypoint), then runs the loop. No shell, no
# aws cli in the image.
#
# One image per engine line (D6). Build for linux/arm64 (Graviton cluster):
#   docker buildx build --platform linux/arm64 --build-arg ENGINE=engine-v8 \
#     -t pb-runner:8-v8.1.0-arm64 -f docker/Dockerfile .
# CodeBuild on an arm64 runner builds natively (deploy/buildspec.yml).
#
# The container starts in --dry-run (plan and log only). The task definition
# passes `--live` to trade (D7 / P6.3).

ARG ENGINE=engine-v8

FROM public.ecr.aws/docker/library/rust:1.95-bookworm AS builder
ARG ENGINE
WORKDIR /src
# git is needed for the pinned passivbot_rust git dependency (rust image has it).
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates crates
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release -p pb-runner --bin pb-runner --no-default-features --features ${ENGINE} \
    && cp target/release/pb-runner /pb-runner

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /pb-runner /usr/local/bin/pb-runner
# /app/configs is created by the binary; /app must be writable by nonroot.
USER nonroot
ENTRYPOINT ["/usr/local/bin/pb-runner"]
