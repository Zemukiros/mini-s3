package dev.minis3.common;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * All SQL lives here. Plain JdbcTemplate instead of JPA on purpose: the
 * interesting parts of this system (advisory locks, SKIP LOCKED, partial
 * unique indexes) are easier to see and explain as raw SQL.
 */
@Repository
@Profile({"api", "worker"})
public class MetadataRepository {

    private final JdbcTemplate jdbc;

    public MetadataRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<Node> NODE = (rs, i) ->
            new Node(rs.getString("id"), rs.getString("url"), rs.getString("status"));

    private static final RowMapper<ObjectMeta> OBJECT = MetadataRepository::mapObject;

    private static ObjectMeta mapObject(ResultSet rs, int i) throws SQLException {
        return new ObjectMeta(
                rs.getObject("id", UUID.class),
                rs.getString("bucket"),
                rs.getString("object_key"),
                rs.getLong("size_bytes"),
                rs.getString("content_type"),
                rs.getString("sha256"),
                rs.getString("status"),
                rs.getTimestamp("created_at").toInstant());
    }

    // ---------------------------------------------------------------- nodes

    /** A heartbeat both registers new nodes and revives nodes that were marked DOWN. */
    public void heartbeat(String nodeId, String url) {
        jdbc.update("""
                INSERT INTO nodes (id, url, status, last_heartbeat) VALUES (?, ?, 'UP', now())
                ON CONFLICT (id) DO UPDATE SET url = EXCLUDED.url, status = 'UP', last_heartbeat = now()
                """, nodeId, url);
    }

    public List<Node> upNodes() {
        return jdbc.query("SELECT id, url, status FROM nodes WHERE status = 'UP' ORDER BY id", NODE);
    }

    public List<Node> allNodes() {
        return jdbc.query("SELECT id, url, status FROM nodes ORDER BY id", NODE);
    }

    /** Failure detection: any UP node silent for longer than the timeout becomes DOWN. */
    public List<String> markSilentNodesDown(int timeoutSeconds) {
        return jdbc.queryForList("""
                UPDATE nodes SET status = 'DOWN'
                WHERE status = 'UP' AND last_heartbeat < now() - make_interval(secs => ?)
                RETURNING id
                """, String.class, timeoutSeconds);
    }

    // -------------------------------------------------------------- objects

    public void insertPending(ObjectMeta o) {
        jdbc.update("""
                INSERT INTO objects (id, bucket, object_key, size_bytes, content_type, sha256, status)
                VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
                """, o.id(), o.bucket(), o.key(), o.size(), o.contentType(), o.sha256());
    }

    public void addReplica(UUID objectId, String nodeId) {
        jdbc.update("INSERT INTO replicas (object_id, node_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
                objectId, nodeId);
    }

    /**
     * Make a PENDING object the live version of its key.
     *
     * Two uploads to the same key can race. The advisory lock serializes
     * commits per (bucket, key), so exactly one version is live at a time and
     * the last one to commit wins. The partial unique index is a second line
     * of defense if this code is ever wrong.
     *
     * Returns false if the object is no longer PENDING (e.g. the GC expired it).
     */
    @Transactional
    public boolean commit(UUID id, String bucket, String key) {
        jdbc.queryForObject("SELECT pg_advisory_xact_lock(hashtextextended(? || '/' || ?, 0))",
                Object.class, bucket, key);
        jdbc.update("""
                UPDATE objects SET status = 'DELETED'
                WHERE bucket = ? AND object_key = ? AND status = 'COMMITTED'
                """, bucket, key);
        return jdbc.update("UPDATE objects SET status = 'COMMITTED' WHERE id = ? AND status = 'PENDING'", id) == 1;
    }

    /** Tombstone. The GC deletes the bytes later, the same way S3 deletes are asynchronous. */
    public void markDeleted(UUID id) {
        jdbc.update("UPDATE objects SET status = 'DELETED' WHERE id = ?", id);
    }

    public boolean deleteLive(String bucket, String key) {
        return jdbc.update("""
                UPDATE objects SET status = 'DELETED'
                WHERE bucket = ? AND object_key = ? AND status = 'COMMITTED'
                """, bucket, key) > 0;
    }

    public Optional<ObjectMeta> findLive(String bucket, String key) {
        return jdbc.query("""
                SELECT * FROM objects WHERE bucket = ? AND object_key = ? AND status = 'COMMITTED'
                """, OBJECT, bucket, key).stream().findFirst();
    }

    public List<ObjectMeta> list(String bucket, String prefix, int limit) {
        return jdbc.query("""
                SELECT * FROM objects
                WHERE bucket = ? AND status = 'COMMITTED' AND object_key LIKE ? ESCAPE '\\'
                ORDER BY object_key LIMIT ?
                """, OBJECT, bucket, escapeLike(prefix) + "%", limit);
    }

    private static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    // ------------------------------------------------------------- replicas

    /** Replicas on nodes that are currently UP. These are the copies we can actually serve. */
    public List<Node> liveReplicaNodes(UUID objectId) {
        return jdbc.query("""
                SELECT n.id, n.url, n.status FROM replicas r JOIN nodes n ON n.id = r.node_id
                WHERE r.object_id = ? AND n.status = 'UP' ORDER BY n.id
                """, NODE, objectId);
    }

    public List<Node> allReplicaNodes(UUID objectId) {
        return jdbc.query("""
                SELECT n.id, n.url, n.status FROM replicas r JOIN nodes n ON n.id = r.node_id
                WHERE r.object_id = ? ORDER BY n.id
                """, NODE, objectId);
    }

    public void removeReplica(UUID objectId, String nodeId) {
        jdbc.update("DELETE FROM replicas WHERE object_id = ? AND node_id = ?", objectId, nodeId);
    }

    // ------------------------------------------------------------ worker queues

    /**
     * Committed objects with fewer live copies than the replication factor.
     *
     * FOR UPDATE SKIP LOCKED turns this table into a work queue: if you run
     * several workers, each one claims different rows instead of all of them
     * repairing the same object. Must be called inside a transaction.
     */
    public List<ObjectMeta> claimUnderReplicated(int replicationFactor, int limit) {
        return jdbc.query("""
                SELECT o.* FROM objects o
                WHERE o.status = 'COMMITTED'
                  AND (SELECT count(*) FROM replicas r JOIN nodes n ON n.id = r.node_id
                       WHERE r.object_id = o.id AND n.status = 'UP') < ?
                ORDER BY o.created_at
                LIMIT ?
                FOR UPDATE OF o SKIP LOCKED
                """, OBJECT, replicationFactor, limit);
    }

    /** Committed objects with more live copies than needed (a dead node came back). */
    public List<ObjectMeta> claimOverReplicated(int replicationFactor, int limit) {
        return jdbc.query("""
                SELECT o.* FROM objects o
                WHERE o.status = 'COMMITTED'
                  AND (SELECT count(*) FROM replicas r JOIN nodes n ON n.id = r.node_id
                       WHERE r.object_id = o.id AND n.status = 'UP') > ?
                LIMIT ?
                FOR UPDATE OF o SKIP LOCKED
                """, OBJECT, replicationFactor, limit);
    }

    public List<ObjectMeta> claimDeleted(int limit) {
        return jdbc.query("""
                SELECT * FROM objects WHERE status = 'DELETED'
                LIMIT ? FOR UPDATE SKIP LOCKED
                """, OBJECT, limit);
    }

    /** Uploads that never committed (API crashed mid-write) become garbage. */
    public int expireStalePending(int minutes) {
        return jdbc.update("""
                UPDATE objects SET status = 'DELETED'
                WHERE status = 'PENDING' AND created_at < now() - make_interval(mins => ?)
                """, minutes);
    }

    public void removeObject(UUID id) {
        jdbc.update("DELETE FROM objects WHERE id = ?", id);
    }

    // ---------------------------------------------------------------- stats

    public List<java.util.Map<String, Object>> replicaCountsPerNode() {
        return jdbc.queryForList("""
                SELECT n.id, n.status, count(r.object_id) AS replicas
                FROM nodes n LEFT JOIN replicas r ON r.node_id = n.id
                GROUP BY n.id, n.status ORDER BY n.id
                """);
    }
}
