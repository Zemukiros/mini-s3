package dev.minis3.worker;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import dev.minis3.common.ClusterConfig.Placement;
import dev.minis3.common.MetadataRepository;
import dev.minis3.common.Node;
import dev.minis3.common.ObjectMeta;
import dev.minis3.common.StorageClient;

/**
 * Reclaims space. Three kinds of garbage:
 *
 *  1. DELETED objects (user deletes, overwritten versions, failed writes):
 *     delete the bytes from every UP node, then drop the metadata.
 *  2. Stale PENDING objects (the API crashed between upload and commit):
 *     after a timeout they become DELETED, and then case 1 handles them.
 *  3. Over-replicated objects (a DOWN node came back after the repair worker
 *     already made new copies): keep the N copies the ring prefers and
 *     delete the rest.
 *
 * Deletes on a DOWN node are simply retried on a later pass, once it's back.
 */
@Component
@Profile("worker")
public class GarbageCollector {

    private static final Logger log = LoggerFactory.getLogger(GarbageCollector.class);

    private final MetadataRepository repo;
    private final StorageClient nodes;
    private final Placement placement;
    private final int replicationFactor;
    private final int pendingTimeoutMinutes;

    public GarbageCollector(MetadataRepository repo, StorageClient nodes, Placement placement,
                            @Value("${minis3.replication-factor}") int replicationFactor,
                            @Value("${minis3.pending-timeout-minutes}") int pendingTimeoutMinutes) {
        this.repo = repo;
        this.nodes = nodes;
        this.placement = placement;
        this.replicationFactor = replicationFactor;
        this.pendingTimeoutMinutes = pendingTimeoutMinutes;
    }

    @Scheduled(fixedDelayString = "${minis3.gc-ms}", initialDelay = 7000)
    @Transactional
    public void collect() {
        try {
            int expired = repo.expireStalePending(pendingTimeoutMinutes);
            if (expired > 0) {
                log.info("Expired {} stale PENDING uploads", expired);
            }
            reclaimDeleted();
            trimOverReplicated();
        } catch (Exception e) {
            log.debug("GC skipped: {}", e.toString());
        }
    }

    private void reclaimDeleted() {
        List<ObjectMeta> batch = repo.claimDeleted(100);
        int reclaimed = 0;
        for (ObjectMeta o : batch) {
            List<Node> replicas = repo.allReplicaNodes(o.id());
            boolean allGone = true;
            for (Node n : replicas) {
                if (!"UP".equals(n.status())) {
                    allGone = false; // retry once the node is back
                    continue;
                }
                try {
                    nodes.delete(n.url(), o.id());
                    repo.removeReplica(o.id(), n.id());
                } catch (Exception e) {
                    allGone = false;
                }
            }
            if (allGone) {
                repo.removeObject(o.id());
                reclaimed++;
            }
        }
        if (reclaimed > 0) {
            log.info("GC reclaimed {} deleted objects", reclaimed);
        }
    }

    private void trimOverReplicated() {
        for (ObjectMeta o : repo.claimOverReplicated(replicationFactor, 100)) {
            List<Node> holders = repo.liveReplicaNodes(o.id());
            Set<String> keep = placement.preferenceList(o.id(), holders, replicationFactor).stream()
                    .map(Node::id).collect(Collectors.toSet());
            for (Node n : holders) {
                if (keep.contains(n.id())) {
                    continue;
                }
                try {
                    // Metadata first: once the row is gone no reader will pick this copy,
                    // so deleting the file afterwards can't break an in-flight read plan.
                    repo.removeReplica(o.id(), n.id());
                    nodes.delete(n.url(), o.id());
                    log.info("Trimmed extra copy of {} on {}", o.id(), n.id());
                } catch (Exception e) {
                    log.warn("Trim of {} on {} failed: {}", o.id(), n.id(), e.toString());
                }
            }
        }
    }
}
