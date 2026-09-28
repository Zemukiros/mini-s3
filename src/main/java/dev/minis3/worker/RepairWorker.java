package dev.minis3.worker;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import dev.minis3.common.ClusterConfig.Placement;
import dev.minis3.common.Hashing;
import dev.minis3.common.MetadataRepository;
import dev.minis3.common.Node;
import dev.minis3.common.ObjectMeta;
import dev.minis3.common.StorageClient;

/**
 * Brings every committed object back to N live copies.
 *
 * It doesn't react to "node X died" events. Instead it repeatedly asks the
 * database "which objects have fewer than 3 copies on UP nodes?" and fixes
 * them. This level-triggered approach (compare desired state to actual
 * state, fix the difference) is the same idea Kubernetes controllers use.
 * It self-heals no matter how things broke: a dead node, a corrupt copy
 * dropped by a read, a straggler write that never landed, or a worker that
 * crashed halfway through a previous repair.
 */
@Component
@Profile("worker")
public class RepairWorker {

    private static final Logger log = LoggerFactory.getLogger(RepairWorker.class);

    private final MetadataRepository repo;
    private final StorageClient nodes;
    private final Placement placement;
    private final int replicationFactor;
    private final Path tmpDir;

    public RepairWorker(MetadataRepository repo, StorageClient nodes, Placement placement,
                        @Value("${minis3.replication-factor}") int replicationFactor,
                        @Value("${minis3.tmp-dir}") String tmpDir) throws IOException {
        this.repo = repo;
        this.nodes = nodes;
        this.placement = placement;
        this.replicationFactor = replicationFactor;
        this.tmpDir = Files.createDirectories(Path.of(tmpDir));
    }

    @Scheduled(fixedDelayString = "${minis3.repair-ms}", initialDelay = 5000)
    @Transactional
    public void repair() {
        List<ObjectMeta> batch;
        try {
            batch = repo.claimUnderReplicated(replicationFactor, 50);
        } catch (Exception e) {
            log.debug("Repair skipped: {}", e.toString());
            return;
        }
        if (batch.isEmpty()) {
            return;
        }
        List<Node> up = placement.upNodes();
        int fixed = 0;
        for (ObjectMeta o : batch) {
            try {
                fixed += repairOne(o, up);
            } catch (Exception e) {
                log.warn("Repair of {} failed: {}", o.id(), e.toString());
            }
        }
        log.info("Repair pass: {} under-replicated objects, {} new copies made", batch.size(), fixed);
    }

    private int repairOne(ObjectMeta o, List<Node> up) throws IOException, InterruptedException {
        List<Node> holders = repo.liveReplicaNodes(o.id());
        if (holders.isEmpty()) {
            // Every copy is on a DOWN node. Nothing to copy from until one comes back.
            log.error("Object {} ({}/{}) has NO live replicas", o.id(), o.bucket(), o.key());
            return 0;
        }
        Set<String> have = holders.stream().map(Node::id).collect(Collectors.toSet());

        // Targets: walk the ring from the object's position and take UP nodes
        // that don't already hold a copy. Same placement rule as the write path.
        List<Node> targets = placement.preferenceList(o.id(), up, up.size()).stream()
                .filter(n -> !have.contains(n.id()))
                .limit(replicationFactor - holders.size())
                .toList();
        if (targets.isEmpty()) {
            return 0; // fewer UP nodes than N: best we can do right now
        }

        Path copy = fetchVerified(o, holders);
        if (copy == null) {
            return 0;
        }
        int made = 0;
        try {
            for (Node t : targets) {
                try {
                    nodes.put(t.url(), o.id(), copy, o.sha256());
                    repo.addReplica(o.id(), t.id());
                    made++;
                    log.info("Re-replicated {} ({}/{}) -> {}", o.id(), o.bucket(), o.key(), t.id());
                } catch (Exception e) {
                    log.warn("Copy of {} to {} failed: {}", o.id(), t.id(), e.toString());
                }
            }
        } finally {
            Files.deleteIfExists(copy);
        }
        return made;
    }

    /** Download from any healthy holder, verifying the checksum. Corrupt copies get dropped. */
    private Path fetchVerified(ObjectMeta o, List<Node> holders) throws IOException {
        for (Node src : holders) {
            Path tmp = tmpDir.resolve(o.id() + "-" + UUID.randomUUID() + ".repair");
            try {
                if (nodes.get(src.url(), o.id(), tmp) && Hashing.sha256(tmp).equals(o.sha256())) {
                    return tmp;
                }
                log.warn("Source copy of {} on {} missing or corrupt; dropping it", o.id(), src.id());
                repo.removeReplica(o.id(), src.id());
            } catch (Exception e) {
                log.warn("Fetch of {} from {} failed: {}", o.id(), src.id(), e.toString());
            }
            Files.deleteIfExists(tmp);
        }
        return null;
    }
}
