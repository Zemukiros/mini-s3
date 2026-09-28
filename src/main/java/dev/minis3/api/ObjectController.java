package dev.minis3.api;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import dev.minis3.common.MetadataRepository;
import dev.minis3.common.ObjectMeta;
import jakarta.servlet.http.HttpServletRequest;

/**
 * Public, S3-shaped API.
 *
 *   PUT    /buckets/{bucket}/objects/{key...}   upload (raw body)
 *   GET    /buckets/{bucket}/objects/{key...}   download
 *   HEAD   /buckets/{bucket}/objects/{key...}   metadata only
 *   DELETE /buckets/{bucket}/objects/{key...}   delete
 *   GET    /buckets/{bucket}/objects?prefix=    list
 *   GET    /admin/cluster                       nodes + replica counts
 */
@RestController
@Profile("api")
public class ObjectController {

    private final ObjectService service;
    private final MetadataRepository repo;

    public ObjectController(ObjectService service, MetadataRepository repo) {
        this.service = service;
        this.repo = repo;
    }

    @PutMapping("/buckets/{bucket}/objects/{*key}")
    public ResponseEntity<Map<String, Object>> put(@PathVariable String bucket, @PathVariable String key,
                                                   @RequestHeader(value = "Content-Type", required = false) String contentType,
                                                   HttpServletRequest request) throws IOException {
        key = normalize(key);
        try (InputStream in = request.getInputStream()) {
            ObjectMeta m = service.put(bucket, key, contentType, in);
            return ResponseEntity.status(HttpStatus.CREATED)
                    .eTag(m.sha256())
                    .body(describe(m));
        }
    }

    @GetMapping("/buckets/{bucket}/objects/{*key}")
    public ResponseEntity<StreamingResponseBody> get(@PathVariable String bucket, @PathVariable String key) throws IOException {
        ObjectService.Download d = service.get(bucket, normalize(key));
        StreamingResponseBody body = out -> {
            try {
                Files.copy(d.file(), out);
            } finally {
                ObjectService.deleteQuietly(d.file());
            }
        };
        return ResponseEntity.ok()
                .contentType(mediaType(d.meta().contentType()))
                .contentLength(d.meta().size())
                .eTag(d.meta().sha256())
                .header("X-Served-By", d.servedBy())
                .body(body);
    }

    @RequestMapping(value = "/buckets/{bucket}/objects/{*key}", method = RequestMethod.HEAD)
    public ResponseEntity<Void> head(@PathVariable String bucket, @PathVariable String key) {
        ObjectMeta m = service.find(bucket, normalize(key));
        return ResponseEntity.ok()
                .contentType(mediaType(m.contentType()))
                .contentLength(m.size())
                .eTag(m.sha256())
                .lastModified(m.createdAt())
                .build();
    }

    @DeleteMapping("/buckets/{bucket}/objects/{*key}")
    public ResponseEntity<Void> delete(@PathVariable String bucket, @PathVariable String key) {
        return service.delete(bucket, normalize(key))
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    @GetMapping("/buckets/{bucket}/objects")
    public List<Map<String, Object>> list(@PathVariable String bucket,
                                          @RequestParam(defaultValue = "") String prefix,
                                          @RequestParam(defaultValue = "100") int limit) {
        return service.list(bucket, prefix, limit).stream().map(ObjectController::describe).toList();
    }

    @GetMapping("/admin/cluster")
    public Map<String, Object> cluster() {
        return Map.of("nodes", repo.replicaCountsPerNode());
    }

    /** Which nodes hold copies of this object, and are they UP? Handy for the failure demo. */
    @GetMapping("/admin/replicas/{bucket}/{*key}")
    public Map<String, Object> replicas(@PathVariable String bucket, @PathVariable String key) {
        ObjectMeta m = service.find(bucket, normalize(key));
        return Map.of("id", m.id(), "replicas", repo.allReplicaNodes(m.id()));
    }

    private static String normalize(String key) {
        String k = key.startsWith("/") ? key.substring(1) : key;
        if (k.isEmpty() || k.length() > 1024) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Key must be 1-1024 characters");
        }
        return k;
    }

    private static MediaType mediaType(String ct) {
        try {
            return ct == null ? MediaType.APPLICATION_OCTET_STREAM : MediaType.parseMediaType(ct);
        } catch (Exception e) {
            return MediaType.APPLICATION_OCTET_STREAM;
        }
    }

    private static Map<String, Object> describe(ObjectMeta m) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("bucket", m.bucket());
        out.put("key", m.key());
        out.put("id", m.id());
        out.put("size", m.size());
        out.put("contentType", m.contentType());
        out.put("sha256", m.sha256());
        out.put("createdAt", m.createdAt().toString());
        return out;
    }
}
