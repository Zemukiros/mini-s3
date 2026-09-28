-- Metadata lives here. File bytes never touch Postgres.

-- Every storage node registers itself through heartbeats.
CREATE TABLE IF NOT EXISTS nodes (
    id             TEXT PRIMARY KEY,
    url            TEXT        NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'UP',      -- UP | DOWN
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per uploaded version. Blobs are immutable: overwriting a key
-- creates a new object id, and the old version becomes DELETED.
CREATE TABLE IF NOT EXISTS objects (
    id           UUID PRIMARY KEY,
    bucket       TEXT        NOT NULL,
    object_key   TEXT        NOT NULL,
    size_bytes   BIGINT      NOT NULL,
    content_type TEXT,
    sha256       TEXT        NOT NULL,
    status       TEXT        NOT NULL,                     -- PENDING | COMMITTED | DELETED
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- At most one live version per (bucket, key).
CREATE UNIQUE INDEX IF NOT EXISTS objects_live_key
    ON objects (bucket, object_key) WHERE status = 'COMMITTED';

CREATE INDEX IF NOT EXISTS objects_status ON objects (status);

-- Where each copy physically lives. This table, not the hash ring, is the
-- source of truth for reads.
CREATE TABLE IF NOT EXISTS replicas (
    object_id  UUID        NOT NULL REFERENCES objects (id) ON DELETE CASCADE,
    node_id    TEXT        NOT NULL REFERENCES nodes (id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (object_id, node_id)
);

CREATE INDEX IF NOT EXISTS replicas_node ON replicas (node_id);
