package dev.minis3.common;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;

/**
 * HTTP client the API server and the worker use to talk to storage nodes.
 * Storage nodes expose a tiny internal API: PUT/GET/DELETE /blobs/{id}.
 */
public class StorageClient {

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    /** Upload a local file to a node. The node verifies the checksum before accepting it. */
    public void put(String nodeUrl, UUID id, Path file, String sha256) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(nodeUrl + "/blobs/" + id))
                .timeout(Duration.ofMinutes(5))
                .header("X-Content-SHA256", sha256)
                .PUT(HttpRequest.BodyPublishers.ofFile(file))
                .build();
        HttpResponse<String> res = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() != 201) {
            throw new IOException("PUT to " + nodeUrl + " failed: " + res.statusCode() + " " + res.body());
        }
    }

    /** Download a blob to a local file. Returns false if the node doesn't have it. */
    public boolean get(String nodeUrl, UUID id, Path target) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(nodeUrl + "/blobs/" + id))
                .timeout(Duration.ofMinutes(5))
                .GET()
                .build();
        HttpResponse<Path> res = http.send(req, HttpResponse.BodyHandlers.ofFile(target));
        return res.statusCode() == 200;
    }

    public void delete(String nodeUrl, UUID id) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder(URI.create(nodeUrl + "/blobs/" + id))
                .timeout(Duration.ofSeconds(10))
                .DELETE()
                .build();
        HttpResponse<Void> res = http.send(req, HttpResponse.BodyHandlers.discarding());
        if (res.statusCode() != 204 && res.statusCode() != 404) {
            throw new IOException("DELETE on " + nodeUrl + " failed: " + res.statusCode());
        }
    }
}
