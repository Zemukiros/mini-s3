package dev.minis3.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import dev.minis3.common.ClusterConfig.Placement;
import dev.minis3.common.Hashing;
import dev.minis3.common.MetadataRepository;
import dev.minis3.common.Node;
import dev.minis3.common.ObjectMeta;
import dev.minis3.common.StorageClient;

/**
 * The write and read paths. This is the heart of the system.
 */
@Service
@Profile("api")
public class ObjectService {

    private static final Logger log = LoggerFactory.getLogger(ObjectService.class);

    private final MetadataRepository repo;
    private final StorageClient nodes;
    private final Placement placement;
    private final int replicationFactor;
    private final int writeQuorum;
    private final long maxUploadBytes;
    private final Path tmpDir;
    // One cheap virtual thread per replica write.
    private final ExecutorService fanOut = Executors.newVirtualThreadPerTaskExecutor();

    public ObjectService(MetadataRepository repo, StorageClient nodes, Placement placement,
                         @Value("${minis3.replication-factor}") int replicationFactor,
                         @Value("${minis3.write-quorum}") int writeQuorum,
                         @Value("${minis3.max-upload-bytes}") long maxUploadBytes,
                         @Value("${minis3.tmp-dir}") String tmpDir) throws IOException {
        this.repo = repo;
        this.nodes = nodes;
        this.placement = placement;
        this.replicationFactor = replicationFactor;
        this.writeQuorum = writeQuorum;
        this.maxUploadBytes = maxUploadBytes;
        this.tmpDir = Files.createDirectories(Path.of(tmpDir));
    }

    // ================================================================ WRITE

    /**
     * 1. Stream the upload to a temp file, computing SHA-256 and size.
     * 2. Ask the hash ring for N=3 target nodes.
     * 3. Record the object as PENDING (so a crash here leaves traceable garbage, not invisible garbage).
     * 4. Send to all 3 in parallel; record each replica as it lands.
     * 5. As soon as W=2 succeed, COMMIT and return. The 3rd keeps going in the background.
     * 6. If W can't be reached, tombstone the object; the GC removes any copies that did land.
     */
    public ObjectMeta put(String bucket, String key, String contentType, InputStream body) throws IOException {
        UUID id = UUID.randomUUID();
        Path tmp = tmpDir.resolve(id + ".upload");
        Hashing.Written written;
        try {
            written = Hashing.copyAndHash(body, tmp, maxUploadBytes);
        } catch (Hashing.TooLargeException e) {
            Files.deleteIfExists(tmp);
            throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, e.getMessage());
        } catch (IOException e) {
            Files.deleteIfExists(tmp);
            throw e;
        }

        List<Node> targets = placement.preferenceList(id, placement.upNodes(), replicationFactor);
        if (targets.size() < writeQuorum) {
            Files.deleteIfExists(tmp);
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Only " + targets.size() + " storage nodes are up; need " + writeQuorum);
        }

        ObjectMeta meta = new ObjectMeta(id, bucket, key, written.size(), contentType,
                written.sha256(), "PENDING", Instant.now());
        repo.insertPending(meta);

        // Quorum tracking: complete TRUE at W successes, FALSE once W becomes impossible.
        CompletableFuture<Boolean> quorum = new CompletableFuture<>();
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        int maxFailures = targets.size() - writeQuorum;

        List<CompletableFuture<Void>> writes = new ArrayList<>();
        for (Node node : targets) {
            writes.add(CompletableFuture.runAsync(() -> {
                try {
                    nodes.put(node.url(), id, tmp, written.sha256());
                    repo.addReplica(id, node.id());
                    if (ok.incrementAndGet() >= writeQuorum) {
                        quorum.complete(true);
                    }
                } catch (Exception e) {
                    log.warn("Replica write of {} to {} failed: {}", id, node.id(), e.toString());
                    if (failed.incrementAndGet() > maxFailures) {
                        quorum.complete(false);
                    }
                }
            }, fanOut));
        }

        // Only delete the temp file once every replica write, including stragglers, is done.
        CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new))
                .whenComplete((v, e) -> deleteQuietly(tmp));

        boolean reached;
        try {
            reached = quorum.get(60, TimeUnit.SECONDS);
        } catch (Exception e) { // timeout or interruption: treat as "quorum not reached"
            reached = false;
        }

        if (!reached) {
            repo.markDeleted(id);
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Write quorum not reached (" + ok.get() + "/" + writeQuorum + ")");
        }
        if (!repo.commit(id, bucket, key)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Upload expired before commit");
        }
        log.info("PUT {}/{} -> {} ({} bytes) acked by {}/{} nodes",
                bucket, key, id, written.size(), ok.get(), targets.size());
        return new ObjectMeta(id, bucket, key, written.size(), contentType, written.sha256(), "COMMITTED", meta.createdAt());
    }

    // ================================================================= READ

    /**
     * Try live replicas one at a time. Every download is checked against the
     * SHA-256 stored in Postgres. A corrupted or missing copy is dropped from
     * the replicas table, which makes the object under-replicated, so the
     * worker will create a fresh copy. That's "read repair".
     *
     * Replicas are shuffled so reads spread across nodes instead of always
     * hitting the first one.
     */
    public Download get(String bucket, String key) throws IOException {
        ObjectMeta meta = find(bucket, key);
        List<Node> replicas = new ArrayList<>(repo.liveReplicaNodes(meta.id()));
        Collections.shuffle(replicas);

        for (Node node : replicas) {
            Path tmp = tmpDir.resolve(meta.id() + "-" + UUID.randomUUID() + ".download");
            try {
                if (!nodes.get(node.url(), meta.id(), tmp)) {
                    log.warn("Replica of {} missing on {}; dropping it", meta.id(), node.id());
                    repo.removeReplica(meta.id(), node.id());
                } else if (!Hashing.sha256(tmp).equals(meta.sha256())) {
                    log.warn("Replica of {} on {} is CORRUPT; dropping it", meta.id(), node.id());
                    repo.removeReplica(meta.id(), node.id());
                } else {
                    return new Download(meta, tmp, node.id());
                }
            } catch (Exception e) {
                log.warn("Read of {} from {} failed: {}", meta.id(), node.id(), e.toString());
            }
            deleteQuietly(tmp);
        }
        throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "No healthy replica available");
    }

    public ObjectMeta find(String bucket, String key) {
        return repo.findLive(bucket, key)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No such key"));
    }

    public boolean delete(String bucket, String key) {
        return repo.deleteLive(bucket, key);
    }

    public List<ObjectMeta> list(String bucket, String prefix, int limit) {
        return repo.list(bucket, prefix, Math.min(Math.max(limit, 1), 1000));
    }

    public record Download(ObjectMeta meta, Path file, String servedBy) {
    }

    static void deleteQuietly(Path p) {
        try {
            Files.deleteIfExists(p);
        } catch (IOException ignored) {
        }
    }
}
