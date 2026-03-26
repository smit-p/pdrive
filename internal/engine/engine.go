package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/smit-p/pdrive/internal/broker"
	"github.com/smit-p/pdrive/internal/chunker"
	"github.com/smit-p/pdrive/internal/metadata"
	"github.com/smit-p/pdrive/internal/rclonerc"
)

const chunkRemoteDir = "pdrive-chunks"

// Engine orchestrates file write and read operations.
type Engine struct {
	db     *metadata.DB
	dbPath string
	rc     *rclonerc.Client
	broker *broker.Broker
	encKey []byte // AES-256 key (32 bytes)
}

// NewEngine creates a new engine.
func NewEngine(db *metadata.DB, dbPath string, rc *rclonerc.Client, b *broker.Broker, encKey []byte) *Engine {
	return &Engine{
		db:     db,
		dbPath: dbPath,
		rc:     rc,
		broker: b,
		encKey: encKey,
	}
}

// WriteFile writes a file to the virtual filesystem, chunking and encrypting it.
// For small files or when data is already in memory.
func (e *Engine) WriteFile(virtualPath string, data []byte) error {
	return e.WriteFileStream(virtualPath, bytes.NewReader(data), int64(len(data)))
}

// WriteFileStream writes a file from a stream, processing one chunk at a time.
// This keeps memory usage constant (~8 MB) regardless of file size.
func (e *Engine) WriteFileStream(virtualPath string, r io.ReadSeeker, size int64) error {
	// Pass 1: compute full file hash by streaming through the reader.
	hasher := sha256.New()
	if _, err := io.Copy(hasher, r); err != nil {
		return fmt.Errorf("hashing file: %w", err)
	}
	fullHashStr := hex.EncodeToString(hasher.Sum(nil))

	// Rewind for pass 2.
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewinding reader: %w", err)
	}

	// Delete existing file if it exists (overwrite).
	existing, err := e.db.GetFileByPath(virtualPath)
	if err != nil {
		return fmt.Errorf("checking existing file: %w", err)
	}
	if existing != nil {
		if err := e.deleteFileChunks(existing.ID); err != nil {
			slog.Warn("failed to clean up old file chunks", "path", virtualPath, "error", err)
		}
		if err := e.db.DeleteFile(existing.ID); err != nil {
			return fmt.Errorf("deleting old file: %w", err)
		}
	}

	fileID := uuid.New().String()
	now := time.Now().Unix()

	// Insert the file record first so chunk FK references are valid.
	if err := e.db.InsertFile(&metadata.File{
		ID:          fileID,
		VirtualPath: virtualPath,
		SizeBytes:   size,
		CreatedAt:   now,
		ModifiedAt:  now,
		SHA256Full:  fullHashStr,
	}); err != nil {
		return fmt.Errorf("inserting file record: %w", err)
	}

	// Pass 2: stream through chunks — encrypt → assign → upload → metadata.
	cr := chunker.NewChunkReader(r, chunker.DefaultChunkSize)
	chunkCount := 0

	for {
		chunk, err := cr.Next()
		if err != nil {
			return fmt.Errorf("reading chunk %d: %w", chunkCount, err)
		}
		if chunk == nil {
			break // EOF
		}

		encrypted, err := chunker.Encrypt(e.encKey, chunk.Data)
		if err != nil {
			return fmt.Errorf("encrypting chunk %d: %w", chunk.Sequence, err)
		}
		chunk.Data = nil // free unencrypted data immediately

		providerID, err := e.broker.AssignChunk(int64(len(encrypted)))
		if err != nil {
			return fmt.Errorf("assigning chunk %d: %w", chunk.Sequence, err)
		}

		provider, err := e.db.GetProvider(providerID)
		if err != nil || provider == nil {
			return fmt.Errorf("getting provider %s: %w", providerID, err)
		}

		remotePath := chunkRemoteDir + "/" + chunk.ID
		encryptedSize := len(encrypted)
		if err := e.rc.PutFile(provider.RcloneRemote, remotePath, bytes.NewReader(encrypted)); err != nil {
			return fmt.Errorf("uploading chunk %d to %s: %w", chunk.Sequence, provider.DisplayName, err)
		}
		encrypted = nil // free after upload

		confirmTime := time.Now().Unix()
		if err := e.db.InsertChunk(&metadata.ChunkRecord{
			ID:            chunk.ID,
			FileID:        fileID,
			Sequence:      chunk.Sequence,
			SizeBytes:     chunk.Size,
			SHA256:        chunk.SHA256,
			EncryptedSize: encryptedSize,
		}); err != nil {
			return fmt.Errorf("inserting chunk record: %w", err)
		}

		if err := e.db.InsertChunkLocation(&metadata.ChunkLocation{
			ChunkID:           chunk.ID,
			ProviderID:        providerID,
			RemotePath:        remotePath,
			UploadConfirmedAt: &confirmTime,
		}); err != nil {
			return fmt.Errorf("inserting chunk location: %w", err)
		}

		slog.Debug("chunk uploaded", "seq", chunk.Sequence, "provider", provider.DisplayName, "size", chunk.Size)
		chunkCount++
	}

	slog.Info("file written", "path", virtualPath, "size", size, "chunks", chunkCount)
	e.scheduleBackup()
	return nil
}

// ReadFile reads a file from the virtual filesystem, downloading and decrypting chunks.
func (e *Engine) ReadFile(virtualPath string) ([]byte, error) {
	file, err := e.db.GetFileByPath(virtualPath)
	if err != nil {
		return nil, fmt.Errorf("looking up file: %w", err)
	}
	if file == nil {
		return nil, fmt.Errorf("file not found: %s", virtualPath)
	}

	chunks, err := e.db.GetChunksForFile(file.ID)
	if err != nil {
		return nil, fmt.Errorf("getting chunks: %w", err)
	}

	var decryptedChunks []chunker.DecryptedChunk

	for _, chunk := range chunks {
		locs, err := e.db.GetChunkLocations(chunk.ID)
		if err != nil {
			return nil, fmt.Errorf("getting chunk locations: %w", err)
		}
		if len(locs) == 0 {
			return nil, fmt.Errorf("no locations for chunk %s", chunk.ID)
		}

		loc := locs[0] // use first available location
		provider, err := e.db.GetProvider(loc.ProviderID)
		if err != nil || provider == nil {
			return nil, fmt.Errorf("getting provider for chunk %s: %w", chunk.ID, err)
		}

		encrypted, err := e.rc.GetFile(provider.RcloneRemote, loc.RemotePath)
		if err != nil {
			return nil, fmt.Errorf("downloading chunk %d from %s: %w", chunk.Sequence, provider.DisplayName, err)
		}

		decrypted, err := chunker.Decrypt(e.encKey, encrypted)
		if err != nil {
			return nil, fmt.Errorf("decrypting chunk %d: %w", chunk.Sequence, err)
		}

		decryptedChunks = append(decryptedChunks, chunker.DecryptedChunk{
			Sequence: chunk.Sequence,
			Data:     decrypted,
			SHA256:   chunk.SHA256,
		})
	}

	reader, err := chunker.Assemble(decryptedChunks)
	if err != nil {
		return nil, fmt.Errorf("assembling file: %w", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading assembled file: %w", err)
	}

	// Verify full file hash.
	fullHash := sha256.Sum256(result)
	if hex.EncodeToString(fullHash[:]) != file.SHA256Full {
		return nil, fmt.Errorf("file hash mismatch for %s", virtualPath)
	}

	slog.Info("file read", "path", virtualPath, "size", len(result))
	return result, nil
}

// DeleteFile removes a file, its chunks from the cloud, and all metadata.
func (e *Engine) DeleteFile(virtualPath string) error {
	file, err := e.db.GetFileByPath(virtualPath)
	if err != nil {
		return fmt.Errorf("looking up file: %w", err)
	}
	if file == nil {
		return fmt.Errorf("file not found: %s", virtualPath)
	}

	if err := e.deleteFileChunks(file.ID); err != nil {
		return err
	}

	if err := e.db.DeleteFile(file.ID); err != nil {
		return fmt.Errorf("deleting file metadata: %w", err)
	}

	slog.Info("file deleted", "path", virtualPath)
	e.scheduleBackup()
	return nil
}

// MkDir creates an explicit directory record.
func (e *Engine) MkDir(dirPath string) error {
	return e.db.CreateDirectory(dirPath)
}

// DeleteDir recursively deletes a directory: all files, cloud chunks, and directory records.
func (e *Engine) DeleteDir(dirPath string) error {
	files, err := e.db.GetFilesUnderDir(dirPath)
	if err != nil {
		return fmt.Errorf("listing files under %s: %w", dirPath, err)
	}
	for _, f := range files {
		if err := e.deleteFileChunks(f.ID); err != nil {
			slog.Warn("failed to delete chunks", "file", f.VirtualPath, "error", err)
		}
		if err := e.db.DeleteFile(f.ID); err != nil {
			slog.Warn("failed to delete file record", "file", f.VirtualPath, "error", err)
		}
	}
	if err := e.db.DeleteDirectoriesUnder(dirPath); err != nil {
		return fmt.Errorf("deleting directory records: %w", err)
	}
	slog.Info("directory deleted", "path", dirPath)
	e.scheduleBackup()
	return nil
}

// RenameDir renames a directory and all its contents in the metadata DB.
func (e *Engine) RenameDir(oldPath, newPath string) error {
	if err := e.db.RenameFilesUnderDir(oldPath, newPath); err != nil {
		return fmt.Errorf("renaming files: %w", err)
	}
	if err := e.db.RenameDirectoriesUnder(oldPath, newPath); err != nil {
		return fmt.Errorf("renaming directories: %w", err)
	}
	slog.Info("directory renamed", "old", oldPath, "new", newPath)
	e.scheduleBackup()
	return nil
}

func (e *Engine) deleteFileChunks(fileID string) error {
	locs, err := e.db.GetChunkLocationsForFile(fileID)
	if err != nil {
		return fmt.Errorf("getting chunk locations: %w", err)
	}

	for _, loc := range locs {
		provider, err := e.db.GetProvider(loc.ProviderID)
		if err != nil || provider == nil {
			slog.Warn("could not get provider for chunk cleanup", "providerID", loc.ProviderID)
			continue
		}
		if err := e.rc.DeleteFile(provider.RcloneRemote, loc.RemotePath); err != nil {
			slog.Warn("failed to delete chunk from provider", "chunk", loc.ChunkID, "provider", provider.DisplayName, "error", err)
		}
	}
	return nil
}

// Stat returns file metadata or nil if the file doesn't exist.
func (e *Engine) Stat(virtualPath string) (*metadata.File, error) {
	return e.db.GetFileByPath(virtualPath)
}

// ListDir returns files and subdirectory names directly under dirPath.
func (e *Engine) ListDir(dirPath string) ([]metadata.File, []string, error) {
	files, err := e.db.ListFiles(dirPath)
	if err != nil {
		return nil, nil, err
	}
	dirs, err := e.db.ListSubdirectories(dirPath)
	if err != nil {
		return nil, nil, err
	}
	return files, dirs, nil
}

// FileExists checks if a file exists at the given virtual path.
func (e *Engine) FileExists(virtualPath string) (bool, error) {
	return e.db.FileExists(virtualPath)
}

// IsDir checks if a path is a directory (has files underneath it).
func (e *Engine) IsDir(path string) (bool, error) {
	return e.db.PathIsDir(path)
}
