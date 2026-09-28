package dev.minis3.storage;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Push-based liveness: every 2s the node tells the API "I'm alive, here's my URL".
 *
 * The first heartbeat also acts as registration, so adding capacity is just
 * starting another storage container; no config change anywhere else.
 * If a node is marked DOWN and later comes back, its next heartbeat flips it
 * back to UP.
 */
@Component
@Profile("storage")
public class HeartbeatSender {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatSender.class);

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
    private final String apiUrl;
    private final String body;
    private boolean lastOk = true;

    public HeartbeatSender(@Value("${minis3.api-url}") String apiUrl,
                           @Value("${minis3.node-id}") String nodeId,
                           @Value("${minis3.node-url}") String nodeUrl) {
        this.apiUrl = apiUrl;
        this.body = "{\"id\":\"" + nodeId + "\",\"url\":\"" + nodeUrl + "\"}";
    }

    @Scheduled(fixedDelayString = "${minis3.heartbeat-ms}")
    public void beat() {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(apiUrl + "/internal/heartbeat"))
                    .timeout(Duration.ofSeconds(1))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();
            int status = http.send(req, HttpResponse.BodyHandlers.discarding()).statusCode();
            if (!lastOk) {
                log.info("Heartbeat restored (status {})", status);
            }
            lastOk = true;
        } catch (Exception e) {
            if (lastOk) {
                log.warn("Heartbeat to {} failed: {}", apiUrl, e.toString());
            }
            lastOk = false;
        }
    }
}
