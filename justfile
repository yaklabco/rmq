# RMQ — Local CI
# Run `just` for the full pipeline, `just -l` to list targets.

set dotenv-load := false

# Default: full CI pipeline
default: fmt lint test build

# --- Formatting ---

# Check formatting (CI mode — fails on diff)
fmt-check:
    cargo fmt --all -- --check

# Fix formatting
fmt:
    cargo fmt --all

# --- Linting ---

# Run clippy (deny warnings, allow common false positives)
lint:
    cargo clippy --workspace --all-targets -- \
        -D warnings \
        -A clippy::approx_constant \
        -A clippy::module_name_repetitions \
        -A clippy::must_use_candidate \
        -A clippy::missing_errors_doc \
        -A clippy::missing_panics_doc \
        -A clippy::too_many_lines \
        -A clippy::wildcard_imports \
        -A clippy::large_enum_variant \
        -A clippy::ptr_arg \
        -A dead-code \
        -A clippy::absurd_extreme_comparisons \
        -A clippy::new_without_default \
        -A clippy::option_map_or_none \
        -A clippy::derivable_impls \
        -A clippy::match_like_matches_macro \
        -A clippy::manual_is_variant_and \
        -A clippy::needless_return \
        -A clippy::collapsible_if \
        -A clippy::explicit_auto_deref \
        -A clippy::unnecessary_map_or \
        -A clippy::unnecessary_min_or_max \
        -A clippy::result_large_err \
        -A clippy::redundant_pattern_matching \
        -A clippy::while_let_loop \
        -A clippy::io_other_error \
        -A clippy::len_zero \
        -A unused-imports \
        -A unused-variables

# --- Testing ---

# Run all tests
test:
    cargo test --workspace

# Run tests with output
test-verbose:
    cargo test --workspace -- --nocapture

# Run a specific test
test-one NAME:
    cargo test --workspace -- {{NAME}} --nocapture

# --- Building ---

# Debug build
build:
    cargo build --workspace

# Release build (native)
build-release:
    cargo build --release -p rmq -p rmq-perf

# Cross-compile for Linux aarch64
build-linux-arm:
    cargo zigbuild --release --target aarch64-unknown-linux-gnu -p rmq -p rmq-perf

# Cross-compile for Linux x86_64
build-linux-x64:
    cargo zigbuild --release --target x86_64-unknown-linux-gnu -p rmq -p rmq-perf

# Build all targets
build-all: build-release build-linux-arm build-linux-x64

# --- Security ---

# Audit dependencies for vulnerabilities
audit:
    cargo audit

# Check dependency licenses and bans
deny:
    cargo deny check

# Full security check
security: audit deny

# --- Benchmarks ---

# Quick benchmark (AMQP, local)
bench:
    #!/usr/bin/env bash
    set -e
    cargo build --release -p rmq -p rmq-perf
    rm -rf /tmp/rmq-bench-data
    ./target/release/rmq -d /tmp/rmq-bench-data -b 127.0.0.1:5672 \
        --mgmt-bind 127.0.0.1:15672 --mqtt-bind 127.0.0.1:1883 \
        --native-bind 127.0.0.1:5680 &
    RMQ_PID=$!
    sleep 1
    echo "--- Publish 500k (4p, persistent) ---"
    ./target/release/rmq-perf publish -n 500000 --size 128 --publishers 4 \
        --persistent --routing-key bench-q
    echo ""
    echo "--- E2E 500k (4p/4c) ---"
    ./target/release/rmq-perf throughput -n 500000 --size 128 \
        --publishers 4 --consumers 4 --prefetch 500
    echo ""
    echo "--- Native 5M (8 conn) ---"
    ./target/release/rmq-perf native --addr 127.0.0.1:5680 \
        -n 5000000 --size 128 --batch 10000 --connections 8
    kill $RMQ_PID 2>/dev/null
    rm -rf /tmp/rmq-bench-data

# --- Running ---

# Run the broker locally
run:
    cargo run -p rmq -- -d /tmp/rmq-dev -b 127.0.0.1:5672

# Run with release optimizations
run-release:
    cargo run --release -p rmq -- -d /tmp/rmq-dev -b 127.0.0.1:5672

# --- Maintenance ---

# Clean build artifacts
clean:
    cargo clean
    rm -rf /tmp/rmq-dev /tmp/rmq-bench-data

# Update dependencies
update:
    cargo update

# Check for outdated dependencies
outdated:
    cargo outdated -R

# --- CI Pipeline ---

# Full CI (what pre-push runs)
ci: fmt-check lint test build security

# Quick check (what pre-commit runs)
check: fmt-check lint
