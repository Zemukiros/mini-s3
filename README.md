# mini-s3

A self-healing, replicated object store built with Java 21 + Spring Boot + PostgreSQL.

- **Consistent hashing** (200 virtual nodes) places each object on 3 of N storage nodes
- **Quorum writes** (W=2 of N=3): uploads succeed as soon as 2 copies are durable
- **Checksum-verified reads** with automatic fallback and read repair
- **Heartbeat failure detection** + a **repair worker** that rebuilds lost copies
- **Garbage collection** for deletes, overwrites, crashed uploads and surplus copies
- Crash-safe node writes: temp file → SHA-256 verify → fsync → atomic rename

See **[DESIGN.md](DESIGN.md)** for the architecture and every design decision.

## Run it

You need **Docker Desktop**. Java and Maven aren't needed; the build runs inside Docker.

```bash
docker compose up -d --build        # Postgres, API, 4 storage nodes, worker
```

## Demo: kill a node, lose nothing

```powershell
# Windows PowerShell
powershell -ExecutionPolicy Bypass -File scripts\demo.ps1
```
```bash
# macOS / Linux / WSL
bash scripts/demo.sh
```

The demo uploads a 5 MB file plus 20 small ones, stops a storage node that holds a copy, waits for the worker to detect it and rebuild the missing copies, downloads the file and verifies the SHA-256 matches, then restarts the node and watches the surplus copies get trimmed.

## API

All public endpoints need the header `X-Api-Key: dev-key` (set `API_KEY` to change it).

| Method | Path | |
|---|---|---|
| `PUT` | `/buckets/{bucket}/objects/{key}` | upload (raw body) |
| `GET` | `/buckets/{bucket}/objects/{key}` | download |
| `HEAD` | `/buckets/{bucket}/objects/{key}` | metadata |
| `DELETE` | `/buckets/{bucket}/objects/{key}` | delete |
| `GET` | `/buckets/{bucket}/objects?prefix=` | list |
| `GET` | `/admin/cluster` | nodes, status, copies per node |
| `GET` | `/admin/replicas/{bucket}/{key}` | where one object's copies live |

```bash
curl -H "X-Api-Key: dev-key" -H "Content-Type: image/png" -T cat.png localhost:8080/buckets/pics/objects/cat.png
curl -H "X-Api-Key: dev-key" localhost:8080/buckets/pics/objects/cat.png -o cat-copy.png
```

## Tests

```bash
mvn test     # HashRingTest: balance with virtual nodes, ~1/N movement on node add/remove
```

## Layout

```
src/main/java/dev/minis3/
  common/   HashRing, MetadataRepository (all SQL), StorageClient, placement
  api/      ObjectController (public API), ObjectService (write + read paths), heartbeats, auth
  storage/  BlobStore (disk), BlobController (/blobs/{id}), HeartbeatSender
  worker/   FailureDetector, RepairWorker, GarbageCollector
src/main/resources/schema.sql
```
