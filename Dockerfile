# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage — static musl binary (no libc, runs anywhere)
# ---------------------------------------------------------------------------
FROM rust:1.88-alpine AS build

RUN apk add --no-cache musl-dev pkgconfig

WORKDIR /src

# Cache dependencies first
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-musl \
    && rm -rf src

# Build the real binary
COPY . .
RUN touch src/main.rs \
    && cargo build --release --target x86_64-unknown-linux-musl \
    && cp target/x86_64-unknown-linux-musl/release/pulseagent /pulseagent

# ---------------------------------------------------------------------------
# Runtime stage — distroless static (CA certs only, non-root)
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /pulseagent /usr/local/bin/pulseagent

# Default debug UI / OTLP receiver ports
EXPOSE 8000 4318

ENTRYPOINT ["/usr/local/bin/pulseagent"]
