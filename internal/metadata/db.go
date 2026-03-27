package metadata

import (
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// DB wraps a SQLite database connection for pdrive metadata.
type DB struct {
	conn *sql.DB
}

// Open opens (or creates) the SQLite database at dbPath with WAL mode enabled.
func Open(dbPath string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0700); err != nil {
		return nil, fmt.Errorf("creating db directory: %w", err)
	}

	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Enable WAL mode for crash safety and concurrent reads.
	if _, err := conn.Exec("PRAGMA journal_mode=WAL"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("enabling WAL mode: %w", err)
	}

	// Enable foreign keys.
	if _, err := conn.Exec("PRAGMA foreign_keys=ON"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("enabling foreign keys: %w", err)
	}

	// Run schema migrations.
	if _, err := conn.Exec(schemaSQL); err != nil {
		conn.Close()
		return nil, fmt.Errorf("running schema: %w", err)
	}

	// Incremental migrations for existing databases.
	migrations := []string{
		`ALTER TABLE files ADD COLUMN upload_state TEXT NOT NULL DEFAULT 'complete'`,
		`ALTER TABLE files ADD COLUMN tmp_path TEXT`,
	}
	for _, m := range migrations {
		// SQLite returns an error if the column already exists; ignore it.
		conn.Exec(m) //nolint:errcheck
	}

	return &DB{conn: conn}, nil
}

// Close closes the database connection.
func (db *DB) Close() error {
	return db.conn.Close()
}

// Conn returns the underlying *sql.DB for advanced use.
func (db *DB) Conn() *sql.DB {
	return db.conn
}
