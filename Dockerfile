FROM rust:1.85-slim AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cargo build --release -p rmq -p rmq-perf && \
    strip target/release/rmq target/release/rmq-perf

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/rmq /usr/local/bin/rmq
COPY --from=builder /build/target/release/rmq-perf /usr/local/bin/rmq-perf

RUN useradd -r -s /bin/false rmq && mkdir -p /var/lib/rmq && chown rmq:rmq /var/lib/rmq

USER rmq
VOLUME /var/lib/rmq

EXPOSE 5672 5671 1883 5680 15672

ENTRYPOINT ["rmq"]
CMD ["-d", "/var/lib/rmq", "-b", "0.0.0.0:5672"]
