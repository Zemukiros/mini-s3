package dev.minis3.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import dev.minis3.common.Hashing;

/**
 * Stores blobs as plain files on local disk.
 *
 * Layout: {dataDir}/ab/abcd1234-....  The two-character prefix directory
 * keeps any single directory from holding millions of files, which makes
 * filesystems slow.
 *
 * Durability rule: a blob is only visible once it is fully written,
 * checksum-verified and fsynced. We write to a temp file and then atomically
 * rename it into place, so a crash mid-write leaves a stray temp file, never
 * a half-written blob that looks real.
 */
@Component
@Profile("storage")
public class BlobStore {

    private final Path dataDir;
    private final Path tmpDir;

    public BlobStore(@Value("${minis3.data-dir}") String dataDir) throws IOException {
        this.dataDir = Path.of(dataDir);
        this.tmpDir = this.dataDir.resolve(".tmp");
        Files.createDirectories(tmpDir);
    }

    Path pathFor(UUID id) {
        String s = id.toString();
        return dataDir.resolve(s.substring(0, 2)).resolve(s);
    }

    /** Returns false if the bytes don't match the expected checksum (nothing is stored). */
    public boolean write(UUID id, InputStream in, String expectedSha256) throws IOException {
        Path tmp = Files.createTempFile(tmpDir, id.toString(), ".part");
        try {
            Hashing.Written w = Hashing.copyAndHash(in, tmp, Long.MAX_VALUE);
            if (expectedSha256 != null && !expectedSha256.equalsIgnoreCase(w.sha256())) {
                return false;
            }
            try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                ch.force(true); // fsync: bytes are on disk, not just in the page cache
            }
            Path target = pathFor(id);
            Files.createDirectories(target.getParent());
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    public Path read(UUID id) {
        Path p = pathFor(id);
        return Files.exists(p) ? p : null;
    }

    public boolean delete(UUID id) throws IOException {
        return Files.deleteIfExists(pathFor(id));
    }
}
