# ballista-riffle

[Riffle](https://github.com/zuston/riffle) (Apache Uniffle) remote shuffle service extension for [Apache Ballista](https://github.com/apache/datafusion-ballista).

## Overview

This extension enables Ballista to use Riffle as a remote shuffle backend, decoupling compute from storage for disaggregated architectures. Instead of writing shuffle data to local disk, executors push partition data to dedicated Riffle shuffle servers. Reducers read from the shuffle servers directly.

## Features

- Full Uniffle gRPC protocol support (register, push, read, report, commit, heartbeat)
- Arrow IPC serialization with LZ4 compression
- Multi-block deserialization (handles multiple mappers writing to same partition)
- Per-partition server routing via coordinator assignments
- Chunked writes for large partitions (configurable `max_block_size`)
- Retry logic with exponential backoff on transient errors
- Uniffle-compatible block ID encoding
- Paginated reads for partitions > 64MB
- Scheduler lifecycle management (register app, heartbeat, cleanup)

## Usage

This crate follows the [Ballista extension pattern](https://github.com/milenkovicm/ballista_extensions).

## Testing

```bash
# Unit tests (no Docker needed)
cargo test

# Integration tests with testcontainers (needs Docker + riffle-test image)
# Build the image first:
#   cd docker && docker build -t riffle-test .
cargo test --features testcontainers
```

## License

Apache License 2.0
