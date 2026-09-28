# Mini-S3: Design Doc

A replicated object store. Clients upload and download files through an API. Every file is kept as 3 copies across a pool of storage nodes, and the system repairs itself automatically when a node dies.

This doc explains **why** each piece exists. If you can explain every section here out loud, you can defend this project in any interview.

---

## 1. The big picture

```
                 ┌──────────────┐
  client ──────► │  API server  │ ◄──── heartbeats every 2s ────┐
  (curl, SDK)    │ (Spring Boot)│                               │
                 └──┬────────┬──┘                               │
        metadata    │        │   bytes (HTTP PUT/GET /blobs/id) │
                    ▼        ▼                                  │
             ┌──────────┐  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐
             │ Postgres │  │ node-1 │ │ node-2 │ │ node-3 │ │ node-4 │
             └────▲─────┘  └────────┘ └────────┘ └────────┘ └────────┘
                  │               ▲  copies bytes between nodes
             ┌────┴─────┐         │
             │  Worker  │─────────┘
             └──────────┘  failure detection · re-replication · GC
```

| Component | Job | Knows about |
|---|---|---|
| **API server** | Public API. Decides where copies go, writes them, serves reads. | Postgres + all nodes |
| **Storage node** | Stores bytes on local disk under an ID. Nothing else. | Only its own disk |
| **Postgres** | Source of truth for metadata: what exists, its checksum, where each copy lives. | – |
| **Worker** | Background janitor. Detects dead nodes, rebuilds missing copies, deletes garbage. | Postgres + all nodes |

**One codebase, three roles.** Every process runs the same jar; `SPRING_PROFILES_ACTIVE=api|storage|worker` decides what it is. That gives you one build and one Docker image, and the roles still deploy and scale independently.

---

## 2. Why separate metadata from bytes?

Postgres is great at small, structured, queryable data: "list every file in bucket X starting with `photos/`", "which objects have fewer than 3 copies?". It's bad at multi-gigabyte blobs, which bloat backups, crowd the buffer cache, and make every query slower.

Storage nodes are the opposite: great at big sequential reads and writes, useless for queries.

So each side does what it's good at. This is how real S3, GFS/HDFS (NameNode vs DataNodes), and Facebook's Haystack work.

**Tradeoff to mention:** Postgres is now a single point of failure. If it's down, nothing works, even though the bytes are safe. The production fix is a Postgres primary with a synchronous standby. At huge scale you'd shard metadata by bucket, or use a distributed database (S3 uses its own sharded metadata tier).

---

## 3. Data model

```sql
nodes    (id, url, status UP|DOWN, last_heartbeat)
objects  (id UUID, bucket, object_key, size_bytes, content_type, sha256,
          status PENDING|COMMITTED|DELETED, created_at)
replicas (object_id, node_id)          -- where each copy physically lives
```

### Key decision: objects are immutable

Every upload gets a **new UUID**, even when you overwrite an existing key. "Overwrite" just means the key now points at the new UUID, and the old version is marked `DELETED` for the garbage collector.

Why this matters:
- A copy can never be **stale**. It's either there (and correct) or missing. There are no "which copy has the latest version?" conflicts, so no vector clocks or version numbers are needed.
- A node that was dead for a day can come back, and every blob on it is still valid.
- Concurrent writers to the same key never write into the same file.

### Key decision: the `replicas` table is the source of truth for reads, not the hash ring

The hash ring decides **where to put new copies**. Once a copy is written, its location is recorded in `replicas`, and reads use that table.

If reads recomputed placement from the ring, adding a node would change the ring, and suddenly reads would look on nodes that don't have the data. By recording locations, you can add or remove nodes freely, and nothing breaks.

*Interview line: "The ring is a placement policy, not a lookup table."*

### Key decision: one live version per key, enforced by the database

```sql
CREATE UNIQUE INDEX objects_live_key ON objects (bucket, object_key) WHERE status = 'COMMITTED';
```
This is a **partial unique index**. There can be many `DELETED` versions of a key, but at most one `COMMITTED`. Even if the Java code had a race bug, Postgres would reject a second live version.

---

## 4. Placement: consistent hashing

**Problem:** Given an object ID, which 3 of the N nodes store it?

**Naive answer:** `hash(id) % N`. When N goes from 4 to 5, almost every key maps to a different node, so you'd have to move ~80% of your data.

**Consistent hashing:** Put nodes and keys on the same circle of hash values (0 to 2⁶⁴). A key belongs to the first node clockwise from it. For 3 copies, keep walking and take the first 3 **distinct** nodes. That's the key's *preference list*.

Adding a node only takes over the arc just before it, so only about 1/N of keys move.

**Virtual nodes:** With one point per node, arcs are randomly sized, so one node can get a huge share. Each physical node is therefore placed at 200 points (`node-1#0`, `node-1#1`, …), which averages the arcs out.

Measured by `HashRingTest` (100,000 keys):

| | Result |
|---|---|
| 5 nodes, 1 vnode each | busiest node holds **31%** (fair share is 20%) |
| 5 nodes, 200 vnodes each | busiest node holds **~22%** |
| Add a 5th node (consistent hashing) | **~19%** of keys move |
| Add a 5th node (`hash % N`) | **80%** of keys move |

Code: `common/HashRing.java`, a `TreeMap<Long, String>`. `tailMap(hash)` finds "the next point clockwise" in O(log n).

---

## 5. The write path (quorum writes)

`api/ObjectService.put()`

1. **Stream the upload to a temp file** on the API server while computing SHA-256 and size. Nothing is buffered in memory, so a 1 GB file uses 64 KB of RAM. Uploads over the size cap are rejected mid-stream.
2. **Pick targets:** the preference list of 3 UP nodes from the ring.
3. **Insert the object as `PENDING`.** If the API crashes after this, the garbage is *visible* to the worker, which expires it after 15 minutes. If you wrote bytes first and metadata second, a crash would leave invisible orphan bytes forever.
4. **Fan out:** send to all 3 nodes in parallel (one virtual thread each). Each node gets the expected SHA-256 in a header and **rejects the write if the bytes don't match** (corruption in transit). As each node acks, a `replicas` row is inserted.
5. **Return at W = 2.** As soon as 2 of 3 succeed, the object is flipped to `COMMITTED` and the client gets `201`. The third write keeps going in the background.
6. **If 2 is impossible** (2 failures), the object is marked `DELETED` and the client gets `503`. The GC removes any copy that did land.

### Why W = 2 out of N = 3?

- **W = 3:** One slow or dead node blocks every upload. Availability suffers.
- **W = 1:** Fast, but if that one node dies before replication, the write is lost even though you told the user "success".
- **W = 2:** Survives any single failure, and latency is set by the 2nd-fastest node, not the slowest. If the 3rd copy fails, the repair worker creates it.

This is the classic N/W/R tunable-consistency idea from Amazon's Dynamo paper.

### How does a node store a blob safely?

`storage/BlobStore.write()`: write to a temp file, verify the checksum, **fsync**, then **atomic rename** into place. A crash at any point leaves either the old state or the complete new blob, never a half-written file that looks real. The two-character subdirectories (`ab/abcd…`) keep any one directory from holding millions of files.

### Concurrent uploads to the same key

Two clients upload `photo.jpg` at the same time. Both uploads get their own UUIDs, so the bytes never collide. At commit time:

```sql
SELECT pg_advisory_xact_lock(hashtextextended(bucket || '/' || key, 0));
UPDATE objects SET status='DELETED'   WHERE bucket=? AND object_key=? AND status='COMMITTED';
UPDATE objects SET status='COMMITTED' WHERE id=? AND status='PENDING';
```

The advisory lock serializes commits **per key** (other keys aren't blocked). **Last commit wins**, which is the same semantics S3 has.

---

## 6. The read path (verified reads + read repair)

`api/ObjectService.get()`

1. Look up the live version and its replicas on **UP** nodes.
2. Shuffle them, so reads spread across nodes instead of hammering one.
3. Download from the first replica and **check SHA-256 against Postgres**.
4. If a copy is **missing or corrupt**, delete its `replicas` row and try the next one. That object is now under-replicated, so the worker will rebuild the copy. This is **read repair**: reads detect damage, and the background process heals it.

**Why R = 1 and not "read from 2 and compare"?** Because objects are immutable and we have an authoritative checksum. One copy that matches the checksum is provably correct. Quorum reads are needed in systems where copies can hold *different versions* (Dynamo, Cassandra), and immutability removes that problem.

**Tradeoff:** The API downloads the whole file and verifies it before sending the first byte, which adds latency on big files. The production fix is **per-chunk checksums**: split files into 8 MB chunks, each with its own hash, so each chunk can be verified while streaming.

---

## 7. Failure detection (heartbeats)

- Each storage node POSTs `/internal/heartbeat` every **2s**. The first heartbeat doubles as **registration**, so scaling out is just starting another container.
- The worker runs `UPDATE nodes SET status='DOWN' WHERE last_heartbeat < now() - 6s` every 2s.
- A DOWN node that heartbeats again is flipped back to UP.

**"How do you know a node is dead and not just slow?"** You can't. In an asynchronous network, a slow node and a dead node look identical. That's a fundamental result (FLP). You pick a timeout and live with the tradeoff:
- **Too short:** A GC pause or network blip triggers a pointless re-replication storm, copying terabytes for a node that comes back 2 seconds later.
- **Too long:** Data sits with fewer copies for longer.

6s keeps the demo snappy. Real systems often wait **minutes** before re-replicating (HDFS defaults to ~10 minutes).

---

## 8. Self-healing (the repair worker)

`worker/RepairWorker.java`, every 3s:

```sql
SELECT o.* FROM objects o
WHERE o.status = 'COMMITTED'
  AND (SELECT count(*) FROM replicas r JOIN nodes n ON n.id = r.node_id
       WHERE r.object_id = o.id AND n.status = 'UP') < 3
LIMIT 50
FOR UPDATE OF o SKIP LOCKED
```

For each object: download a verified copy from a live holder, then write it to the next UP nodes on its ring that don't have it yet.

### Level-triggered, not event-triggered

The worker doesn't listen for "node-2 died" events. It keeps asking **"what's the difference between desired state (3 live copies) and actual state?"** and fixes the difference. That means it heals *every* cause of missing copies the same way:
- a node died
- a read found a corrupt copy
- the 3rd write of an upload failed
- a previous repair crashed halfway

Kubernetes controllers use exactly this pattern.

### Why `FOR UPDATE SKIP LOCKED`?

It turns a table into a **work queue**. Run 5 workers, and each one grabs *different* rows. A row locked by one worker is skipped by the others instead of waited on. So the worker scales horizontally with zero coordination code.

### When the dead node comes back

It has old copies, and the worker already made new ones, so some objects now have 4 live copies. The GC keeps the 3 the ring prefers and deletes the rest. Immutability makes this safe: the returning node's copies are never wrong, just extra.

---

## 9. Garbage collection

`worker/GarbageCollector.java`, every 5s:

| Garbage | Where it comes from | Fix |
|---|---|---|
| `DELETED` objects | user delete, overwrite, failed upload | delete bytes from each UP node, then the rows |
| stale `PENDING` | API crashed mid-upload | after 15 min → `DELETED` |
| extra copies | dead node came back | trim to the ring's top 3 |

**Deletes are asynchronous.** `DELETE` returns immediately after flipping the status, and the bytes disappear seconds later. S3 behaves the same way. If a node holding a copy is DOWN, that delete is retried on a later pass.

---

## 10. What I'd change at 1000× scale

Know these cold. "What would you do differently at scale?" is almost guaranteed.

1. **Metadata tier:** Postgres primary + synchronous replica now. Later, shard by `hash(bucket)` or move to a distributed SQL store.
2. **Chunking:** Split objects into 8–64 MB chunks, each replicated and checksummed independently. That enables parallel uploads (S3 multipart), resumable transfers, and streaming verification.
3. **Erasure coding instead of 3× replication:** Reed-Solomon 6+3 survives 3 lost pieces at 1.5× storage overhead instead of 3×. Replication for hot data, erasure coding for cold.
4. **Failure domains:** Never put 2 copies in the same rack or availability zone. The ring would need to be rack-aware.
5. **Background scrubber:** Periodically re-read every blob and verify checksums, to find bit rot before a user does.
6. **Throttled repair:** Cap repair bandwidth so a node failure doesn't saturate the network and hurt live traffic.
7. **API tier:** Multiple stateless API servers behind a load balancer (Nginx/ALB). They already are stateless: all state is in Postgres and the nodes.
8. **Auth:** Per-user access keys with signed requests (like AWS SigV4) instead of one shared API key.

## 11. Known gaps (be upfront about these)

- An upload that fails quorum can leave an orphan blob if a straggler write lands *after* the GC removed the object row. The fix is a scrubber that lists each node's files and deletes IDs with no metadata.
- The repair worker holds row locks while copying bytes. For huge objects you'd claim work with a short lease column instead of a long transaction.
- Internal endpoints (`/internal/heartbeat`, node `/blobs`) are unauthenticated and rely on network isolation. In production you'd use mTLS between services.

---

## 12. Interview questions you should be able to answer

1. Why not store files in Postgres? → §2
2. Why consistent hashing over `hash % N`? What are virtual nodes for? → §4
3. Why W=2 of 3? What happens if the 3rd write fails? → §5
4. What if two people upload the same key at once? → §5
5. How do reads know a copy is good? Why not read from 2 replicas? → §6
6. Dead vs slow: how do you tell? What's the tradeoff in the timeout? → §7
7. What happens when a dead node comes back? → §8
8. How would you run multiple repair workers safely? → §8 (SKIP LOCKED)
9. What happens if the API crashes mid-upload? → §5 step 3, §9
10. What's the single point of failure, and how would you remove it? → §2, §10
