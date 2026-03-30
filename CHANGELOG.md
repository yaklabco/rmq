# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Full AMQP 0.9.1 protocol implementation (all exchange types, QoS, transactions)
- mmap-backed persistent message store with segment rotation and crash recovery
- Fast-path channel delivery with store fallback for zero-contention consuming
- Batched fsync (group commit) for durable messages
- TLS support via rustls
- MQTT 3.1.1 protocol bridge to AMQP topic exchange
- HTTP management API (axum) with Basic auth, CRUD, definitions import/export
- Native batch protocol achieving 5M+ msg/s with persistence
- User authentication with bcrypt/SHA256 password hashing
- Per-vhost regex permissions (configure/read/write)
- Policy engine with regex matching and operator policies
- Priority queues, message TTL, max-length, dead-letter exchanges
- Shovels and federation with hop tracking
- Clustering with SHA1 checksums, LZ4 replication, file-based sync
- Publisher confirms with batched fsync guarantee
- TCP_CORK on Linux, madvise hints for mmap segments
- Segment index for fast skip-over-acked scanning
- Zero-allocation hot path (Arc messages, reusable buffers)
- Cross-compilation for Linux aarch64 and x86_64 via cargo-zigbuild
