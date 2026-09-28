package dev.minis3.worker;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import dev.minis3.common.MetadataRepository;

/**
 * "Dead or just slow?" is unanswerable in a distributed system, so we pick a
 * timeout: a node that has missed ~3 heartbeats (6s) is declared DOWN.
 *
 * Too short a timeout and a GC pause or network blip triggers a pointless
 * mass re-replication. Too long and data sits under-replicated for longer.
 * Real systems often wait minutes before re-replicating for this reason;
 * 6s here keeps the demo snappy.
 */
@Component
@Profile("worker")
public class FailureDetector {

    private static final Logger log = LoggerFactory.getLogger(FailureDetector.class);

    private final MetadataRepository repo;
    private final int timeoutSeconds;

    public FailureDetector(MetadataRepository repo,
                           @Value("${minis3.heartbeat-timeout-seconds}") int timeoutSeconds) {
        this.repo = repo;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Scheduled(fixedDelayString = "${minis3.failure-check-ms}", initialDelay = 3000)
    public void check() {
        try {
            List<String> dead = repo.markSilentNodesDown(timeoutSeconds);
            for (String id : dead) {
                log.warn("Node {} missed heartbeats for {}s -> marked DOWN", id, timeoutSeconds);
            }
        } catch (Exception e) {
            log.debug("Failure check skipped: {}", e.toString());
        }
    }
}
