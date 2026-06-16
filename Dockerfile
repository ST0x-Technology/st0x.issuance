FROM ubuntu:24.04 AS builder

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
# Workspace members must exist at metadata-resolution time, so each member's
# manifest and a stub source tree have to be present before `cargo chef prepare`
# (cook reconstructs the skeleton from recipe.json afterwards).
COPY crates/dto/Cargo.toml crates/dto/Cargo.toml
COPY crates/client/Cargo.toml crates/client/Cargo.toml

RUN mkdir -p src/bin crates/dto/src crates/client/src && \
    echo 'fn main() {}' > src/main.rs && \
    echo 'fn main() {}' > src/bin/issuer.rs && \
    echo 'fn main() {}' > crates/dto/src/main.rs && \
    touch src/lib.rs crates/dto/src/lib.rs crates/client/src/lib.rs

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
    patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 /app/target/${BUILD_PROFILE}/issuer && \
    apt-get remove -y patchelf && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

FROM debian:trixie-slim

ARG BUILD_PROFILE=release

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/${BUILD_PROFILE}/st0x-issuance ./server
COPY --from=builder /app/target/${BUILD_PROFILE}/issuer ./issuer
COPY --from=builder /app/migrations ./migrations
