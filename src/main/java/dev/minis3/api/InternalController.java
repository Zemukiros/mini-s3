package dev.minis3.api;

import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import dev.minis3.common.MetadataRepository;

/** Cluster-internal endpoints. Not exposed outside the private network in docker-compose. */
@RestController
@Profile("api")
public class InternalController {

    private final MetadataRepository repo;

    public InternalController(MetadataRepository repo) {
        this.repo = repo;
    }

    public record Heartbeat(String id, String url) {
    }

    @PostMapping("/internal/heartbeat")
    public ResponseEntity<Void> heartbeat(@RequestBody Heartbeat hb) {
        repo.heartbeat(hb.id(), hb.url());
        return ResponseEntity.noContent().build();
    }
}
