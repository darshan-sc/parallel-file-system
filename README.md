# Parallel File System

A distributed file system built in Go that stripes data across independent storage nodes for parallel I/O. A dedicated metadata server handles coordination — locking, routing, and cache invalidation — while file servers handle raw block storage.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     Client                          │
│  ┌──────────────────────────────────────────────┐   │
│  │         LRU Write-Back Cache (128 blocks)    │   │
│  └──────────────────────────────────────────────┘   │
└────────────┬──────────────────────┬─────────────────┘
             │ gRPC                 │ gRPC (parallel)
             ▼                      ▼
    ┌─────────────────┐   ┌────────────────────────┐
    │ Metadata Server │   │  File Servers (N nodes) │
    │                 │   │  ┌──────┐ ┌──────┐      │
    │ - File registry │   │  │  FS0 │ │  FS1 │ ...  │
    │ - R/W locks     │   │  └──────┘ └──────┘      │
    │ - Invalidations │   │   block0   block1        │
    └─────────────────┘   └────────────────────────┘
```

**Metadata server** owns file metadata, the stripe recipe (which servers hold which blocks), and a reader-writer lock table. It never touches file data.

**File servers** are dumb block stores — they only expose `ReadBytes` / `WriteBytes` over gRPC. Each server holds one stripe of the file's data.

**Client** resolves block → server mapping locally using the stripe recipe returned at open time, then issues reads and writes directly to the relevant file servers in parallel.

## Features

- **Parallel file striping** — blocks are distributed across N file servers in round-robin order. A 4-block write to a 3-server file touches all three servers concurrently.
- **Reader-writer locks** — the metadata server enforces file-level exclusive write locks and shared read locks using Mesa-style condition variables. Readers block while a writer is active; multiple readers coexist freely.
- **LRU write-back cache** — the client caches up to 128 blocks (512 bytes each). Dirty blocks are flushed on eviction or file close, not on every write.
- **Block-level cache invalidation** — when a writer closes a file, the metadata server pushes an invalidation notice listing only the blocks that were dirtied. Other clients evict only those blocks, preserving cache warmth for unmodified regions.
- **Push invalidation via streaming RPC** — each client holds a long-lived `Subscribe` stream to the metadata server. Invalidation notices are pushed immediately on writer close with no polling.

## Project Structure

```
cmd/
  metaserver/   # metadata server binary
  fileserver/   # file server binary
  demo/         # runnable end-to-end demo
pkg/
  metaserver/   # lock table, metadata store, gRPC server
  fileserver/   # block storage, gRPC server
  client/       # LRU cache, striping logic, public API
  common/       # shared constants (block size, ports, modes)
proto/
  metadata/     # MetadataService proto definition
  fileserver/   # FileService proto definition
gen/            # generated protobuf Go code
tests/          # integration and unit tests
```

## Getting Started

**Prerequisites:** Go 1.22+, `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`

```bash
# Build binaries
make build

# Run the self-contained demo (spins up servers in-process)
go run ./cmd/demo

# Run all tests
make test

# Regenerate protobuf code after editing .proto files
make proto
```

### Running a real cluster

Start one metadata server and any number of file servers:

```bash
# Terminal 1 — metadata server (points at two file servers)
./metaserver -port 50051 -file-servers localhost:50052,localhost:50053

# Terminal 2 — file server 0
./fileserver -port 50052 -data-dir /tmp/pfs0

# Terminal 3 — file server 1
./fileserver -port 50053 -data-dir /tmp/pfs1
```

### Client API

```go
c, _ := client.NewClient("client-id", "localhost:50051", nil)
defer c.Shutdown()

c.PfsCreate("data.bin", 2)          // create file, stripe across 2 servers

fd := c.PfsOpen("data.bin", common.ModeWrite)
c.PfsWrite(fd, buf, len(buf), 0)   // writes go to cache
c.PfsClose(fd)                      // flushes dirty blocks to file servers

fd2 := c.PfsOpen("data.bin", common.ModeRead)
c.PfsRead(fd2, out, len(out), 0)
c.PfsClose(fd2)
```

## Design Notes

**Striping** — logical block `i` lives on file server `i % stripe_width`. The client computes this locally from the stripe recipe, so reads and writes bypass the metadata server entirely after open.

**Consistency** — sequential consistency is guaranteed by the lock protocol: a writer holds an exclusive lock for the entire session. On close, the metadata server sends block-level invalidation notices to all other clients before releasing the lock, ensuring readers never see stale cache entries for written blocks.

**Write-back cache** — dirty blocks accumulate in the client cache and are written to file servers on eviction (LRU pressure) or on `PfsClose`. This batches small writes into larger I/Os and reduces round trips to the file servers.
