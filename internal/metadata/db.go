// Package metadata manages the SQLite database that stores all pdrive
// state: files, chunks, chunk locations, providers, directories, activity
// logs, and failed-deletion retry queues.
//
// The database runs in WAL mode with a single connection (standard SQLite/Go
// pattern) and uses foreign keys with ON DELETE CASCADE for referential
// integrity.  Schema is embedded via go:embed and applied on every Open call
// (all statements use IF NOT EXISTS / IF NOT EXISTS for idempotency).
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
		_ = conn.Close()
		return nil, fmt.Errorf("enabling WAL mode: %w", err)
	}

	// SQLite pragmas are per-connection. Restrict the pool to a single
	// connection so every query uses the one we configure here. This is the
	// standard pattern for SQLite in Go and prevents FK/WAL surprises.
	conn.SetMaxOpenConns(1)

	// Enable foreign keys.
	if _, err := conn.Exec("PRAGMA foreign_keys=ON"); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("enabling foreign keys: %w", err)
	}

	// Run schema.
	if _, err := conn.Exec(schemaSQL); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("running schema: %w", err)
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
