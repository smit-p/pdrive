CREATE TABLE IF NOT EXISTS providers (
    id                  TEXT PRIMARY KEY,
    type                TEXT NOT NULL,
    display_name        TEXT NOT NULL,
    rclone_remote       TEXT NOT NULL,
    quota_total_bytes   INTEGER,
    quota_free_bytes    INTEGER,
    quota_polled_at     INTEGER,
    rate_limited_until  INTEGER
);

CREATE TABLE IF NOT EXISTS files (
    id          TEXT PRIMARY KEY,
    virtual_path TEXT NOT NULL UNIQUE,
    size_bytes  INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    modified_at INTEGER NOT NULL,
    sha256_full TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS chunks (
    id          TEXT PRIMARY KEY,
    file_id     TEXT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    sequence    INTEGER NOT NULL,
    size_bytes  INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    encrypted_size INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chunk_locations (
    chunk_id    TEXT NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL REFERENCES providers(id),
    remote_path TEXT NOT NULL,
    upload_confirmed_at INTEGER,
    PRIMARY KEY (chunk_id, provider_id)
);

CREATE INDEX IF NOT EXISTS idx_files_virtual_path ON files(virtual_path);
CREATE INDEX IF NOT EXISTS idx_chunks_file_id ON chunks(file_id);
CREATE INDEX IF NOT EXISTS idx_chunk_locations_chunk_id ON chunk_locations(chunk_id);
