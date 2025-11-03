FROM ubuntu:latest AS builder

ARG BUILD_PROFILE=release

RUN apt update -y
RUN apt install curl git -y
RUN curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install linux \
  --extra-conf "sandbox = false" \
  --extra-conf "experimental-features = nix-command flakes" \
  --init none \
  --no-confirm
ENV PATH="${PATH}:/nix/var/nix/profiles/default/bin"

WORKDIR /app

COPY flake.nix flake.lock ./
RUN nix develop --command echo "Nix dev env ready"

COPY Cargo.toml Cargo.lock ./
COPY crates/sqlite-es/Cargo.toml ./crates/sqlite-es/

RUN mkdir -p src crates/sqlite-es/src && \
    echo 'fn main() {}' > src/main.rs && \
    touch src/lib.rs && \
    touch crates/sqlite-es/src/lib.rs

RUN nix develop --command cargo chef prepare --recipe-path recipe.json

RUN rm -rf src crates

RUN nix develop --command bash -c ' \
    if [ "$BUILD_PROFILE" = "release" ]; then \
        cargo chef cook --release --recipe-path recipe.json; \
    else \
        cargo chef cook --recipe-path recipe.json; \
    fi'

COPY . .

RUN nix run .#prepSolArtifacts

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    sqlx database create && \
    sqlx migrate run \
'

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    cargo test -q \
'

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    if [ "$BUILD_PROFILE" = "release" ]; then \
        cargo build --release; \
    else \
        cargo build; \
    fi'

RUN apt-get update && apt-get install -y --no-install-recommends patchelf && \
    patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 /app/target/${BUILD_PROFILE}/st0x-issuance && \
    apt-get remove -y patchelf && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

FROM debian:12-slim

ARG BUILD_PROFILE=release

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/${BUILD_PROFILE}/st0x-issuance ./server
COPY --from=builder /app/migrations ./migrations
