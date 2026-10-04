# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Mini-S3 is a replicated object store: Java 21 · Spring Boot 3.5 · PostgreSQL 16 · Docker Compose.
Each object is kept as 3 copies across 4 storage nodes (consistent hashing, quorum writes W=2 of N=3,
checksum-verified reads with read repair, heartbeat failure detection, a repair worker and a garbage collector).

Read `DESIGN.md` before changing behaviour — it records the reason for every design decision, plus the
known gaps (§11). `README.md` has the API table and demo instructions.

## Commands

```bash
docker compose up -d --build          # Postgres, API (:8080), storage-1..4, worker
docker compose logs -f worker         # watch failure detection / repair / GC
docker compose down -v                # stop and wipe all data (pgdata + node volumes)
bash scripts/demo.sh                  # kill a node, verify repair + SHA-256, restart, watch surplus trimmed
mvn test                              # HashRingTest (the only test class)
mvn test -Dtest=HashRingTest#<method> # single test
```

Java and Maven are not required locally: the `Dockerfile` builds with `maven:3.9-eclipse-temurin-21`.
Without a local JDK, run tests with
`docker run --rm -v "$PWD":/src -w /src maven:3.9-eclipse-temurin-21 mvn -q test`.
`scripts/demo.ps1` is the PowerShell twin of `demo.sh`; keep the two in step.

Public endpoints need `X-Api-Key` (default `dev-key`, set via `API_KEY`).

## Architecture

**One codebase, three roles**, chosen by `SPRING_PROFILES_ACTIVE` (see `MiniS3Application`). Every
component carries a `@Profile`, and each role has its own `application-<role>.properties`:

- `api` (`dev.minis3.api`) — public REST API (`ObjectController`), write/read paths (`ObjectService`),
  `ApiKeyFilter`, and `InternalController` (`/internal/heartbeat`). Owns the schema: `schema.sql` runs
  on API startup (`spring.sql.init.mode=always`).
- `storage` (`dev.minis3.storage`) — dumb blob store on local disk (`BlobStore`, `BlobController` at
  `/blobs/{id}`), sends heartbeats (`HeartbeatSender`). Never talks to Postgres; datasource
  auto-config is excluded for this profile.
- `worker` (`dev.minis3.worker`) — `FailureDetector`, `RepairWorker`, `GarbageCollector` on fixed schedules.
- `common` — shared by api + worker: `HashRing`, `MetadataRepository`, `StorageClient`, `ClusterConfig`.

Only the API port is published to the host; storage nodes are reachable only on the Compose network.

**Invariants to preserve** (each is argued in `DESIGN.md`):

- **Objects are immutable.** Every upload gets a new UUID, including an overwrite of an existing key; the
  old version is marked `DELETED` for GC. Never write into an existing blob.
- **The `replicas` table is the source of truth for reads** — the hash ring only decides where *new*
  copies go. Reads must never recompute placement from the ring.
- **One live version per key** is enforced by the partial unique index `objects_live_key` in `schema.sql`.
- **Crash-safe node writes:** temp file → SHA-256 verify → fsync → atomic rename.
- **Repair is level-triggered**: the worker compares desired vs actual copies each pass, and claims work
  with `FOR UPDATE SKIP LOCKED`.

## Conventions

- All SQL lives in `MetadataRepository`, using plain `JdbcTemplate` on purpose (advisory locks,
  `SKIP LOCKED` and partial indexes stay visible). No JPA.
- Replication settings (`replication-factor`, `write-quorum`, `virtual-nodes`) are in
  `application.properties`; worker intervals and heartbeat timeout are in `application-worker.properties`.
- `HashRing` is ported bit-for-bit into the portfolio's in-browser simulator (`~/projects/portfolio`,
  `src/lib/hashRing.ts`, with intervals mirrored in `src/lib/miniS3Sim.ts`). If you change hashing,
  virtual-node count, preference-list order or any interval here, flag that the port needs updating.
- Recorded drill numbers vary per run — never quote a count as fixed without a recorded run behind it.
