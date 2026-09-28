package dev.minis3.common;

import java.time.Instant;
import java.util.UUID;

public record ObjectMeta(
        UUID id,
        String bucket,
        String key,
        long size,
        String contentType,
        String sha256,
        String status,
        Instant createdAt) {
}
