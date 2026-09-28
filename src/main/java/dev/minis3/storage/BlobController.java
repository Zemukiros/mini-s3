package dev.minis3.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import jakarta.servlet.http.HttpServletRequest;

/**
 * Internal API of a storage node. Deliberately dumb: it knows nothing about
 * buckets, keys, users or other nodes. It stores bytes under an id.
 * All the intelligence lives in the API server and the worker.
 */
@RestController
@Profile("storage")
public class BlobController {

    private final BlobStore store;
    private final String nodeId;

    public BlobController(BlobStore store, @Value("${minis3.node-id}") String nodeId) {
        this.store = store;
        this.nodeId = nodeId;
    }

    @PutMapping("/blobs/{id}")
    public ResponseEntity<String> put(@PathVariable UUID id,
                                      @RequestHeader(value = "X-Content-SHA256", required = false) String sha256,
                                      HttpServletRequest request) throws IOException {
        boolean ok = store.write(id, request.getInputStream(), sha256);
        if (!ok) {
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body("checksum mismatch");
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(nodeId);
    }

    @GetMapping("/blobs/{id}")
    public ResponseEntity<Resource> get(@PathVariable UUID id) throws IOException {
        Path p = store.read(id);
        if (p == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .contentLength(Files.size(p))
                .body(new FileSystemResource(p));
    }

    @DeleteMapping("/blobs/{id}")
    public ResponseEntity<Void> delete(@PathVariable UUID id) throws IOException {
        return store.delete(id) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("node", nodeId, "status", "UP");
    }
}
