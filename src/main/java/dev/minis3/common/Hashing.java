package dev.minis3.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class Hashing {

    private Hashing() {
    }

    public static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String hex(MessageDigest digest) {
        return HexFormat.of().formatHex(digest.digest());
    }

    public static String sha256(Path file) throws IOException {
        MessageDigest md = sha256();
        try (InputStream in = new DigestInputStream(Files.newInputStream(file), md)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return hex(md);
    }

    /** Thrown when a stream exceeds its byte limit. */
    public static class TooLargeException extends IOException {
        public TooLargeException(long limit) {
            super("Upload exceeds limit of " + limit + " bytes");
        }
    }

    /**
     * Copy {@code in} to {@code target} while hashing, refusing to write more
     * than {@code maxBytes}. Streaming to disk means a 1 GB upload never sits
     * in memory.
     */
    public static Written copyAndHash(InputStream in, Path target, long maxBytes) throws IOException {
        MessageDigest md = sha256();
        long total = 0;
        byte[] buf = new byte[64 * 1024];
        try (OutputStream out = Files.newOutputStream(target)) {
            int r;
            while ((r = in.read(buf)) != -1) {
                total += r;
                if (total > maxBytes) {
                    throw new TooLargeException(maxBytes);
                }
                md.update(buf, 0, r);
                out.write(buf, 0, r);
            }
        }
        return new Written(total, hex(md));
    }

    public record Written(long size, String sha256) {
    }
}
