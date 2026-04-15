package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/smit-p/pdrive/internal/erasure"
	"github.com/smit-p/pdrive/internal/metadata"
)

// ReadFile reads a file from the virtual filesystem, downloading and decrypting chunks.
// Returns an error if the file is still uploading (upload_state='pending').
// For large files, prefer ReadFileToTempFile to avoid holding the entire file in memory.
func (e *Engine) ReadFile(virtualPath string) ([]byte, error) {
	tmp, err := e.ReadFileToTempFile(virtualPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()
	return io.ReadAll(tmp)
}

// downloadErasureChunk downloads shards for an erasure-coded chunk,
// reconstructs the original encrypted data using Reed-Solomon, and returns it.
// Tolerates up to parityShards missing/failed shard downloads.
func (e *Engine) downloadErasureChunk(chunk metadata.ChunkRecord, locs []metadata.ChunkLocation) ([]byte, error) {
	totalShards := chunk.DataShards + chunk.ParityShards
	shards := make([][]byte, totalShards)
	downloaded := 0

	for _, loc := range locs {
		if loc.ShardIndex >= totalShards {
			continue
		}
		provider, err := e.db.GetProvider(loc.ProviderID)
		if err != nil || provider == nil {
			slog.Warn("skipping shard download, provider error",
				"chunk", chunk.ID, "shard", loc.ShardIndex, "error", err)
			continue
		}
		rc, err := e.rc.GetFile(provider.RcloneRemote, loc.RemotePath)
		if err != nil {
			slog.Warn("skipping shard download, fetch error",
				"chunk", chunk.ID, "shard", loc.ShardIndex,
				"provider", provider.DisplayName, "error", err)
			continue
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			slog.Warn("skipping shard download, read error",
				"chunk", chunk.ID, "shard", loc.ShardIndex, "error", err)
			continue
		}
		shards[loc.ShardIndex] = data
		downloaded++
	}

	if downloaded < chunk.DataShards {
		return nil, fmt.Errorf("only %d of %d required shards available for chunk %s",
			downloaded, chunk.DataShards, chunk.ID)
	}

	enc, err := erasure.NewEncoder(chunk.DataShards, chunk.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("creating RS encoder for reconstruction: %w", err)
	}

	if err := enc.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("RS reconstruction failed for chunk %s: %w", chunk.ID, err)
	}

	return enc.Join(shards, chunk.SizeBytes)
}

// ReadFileToTempFile downloads a file to a temporary file, returning the open handle.
// The caller must close the file and remove it when done:
//
//	defer func() { f.Close(); os.Remove(f.Name()) }()
//
// Each chunk is downloaded, decrypted, verified, and written to disk sequentially
// so peak memory stays bounded to one chunk (~32–128 MB) regardless of file size.
func (e *Engine) ReadFileToTempFile(virtualPath string) (*os.File, error) {
	slog.Info("download started", "path", virtualPath)
	file, err := e.db.GetCompleteFileByPath(virtualPath)
	if err != nil {
		return nil, fmt.Errorf("looking up file: %w", err)
	}
	if file == nil {
		if any, _ := e.db.GetFileByPath(virtualPath); any != nil {
			return nil, fmt.Errorf("file upload in progress: %s", virtualPath)
		}
		return nil, fmt.Errorf("file not found: %s", virtualPath)
	}

	chunks, err := e.db.GetChunksForFile(file.ID)
	if err != nil {
		return nil, fmt.Errorf("getting chunks: %w", err)
	}
	slog.Info("download plan", "path", virtualPath, "size", file.SizeBytes, "chunks", len(chunks))

	// Validate chunk sequences are contiguous (0, 1, 2, ..., n-1).
	for i, c := range chunks {
		if c.Sequence != i {
			return nil, fmt.Errorf("chunk sequence gap at index %d: expected seq %d, got %d for %s",
				i, i, c.Sequence, virtualPath)
		}
	}

	tmp, err := os.CreateTemp("", "pdrive-read-*")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}
	abandon := func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}

	fullHasher := sha256.New()

	for _, chunk := range chunks {
		locs, err := e.db.GetChunkLocations(chunk.ID)
		if err != nil {
			abandon()
			return nil, fmt.Errorf("getting chunk locations: %w", err)
		}
		if len(locs) == 0 {
			abandon()
			return nil, fmt.Errorf("no locations for chunk %s", chunk.ID)
		}

		chunkHasher := sha256.New()
		mw := io.MultiWriter(tmp, fullHasher, chunkHasher)

		if chunk.DataShards > 1 || chunk.ParityShards > 0 {
			// Erasure-coded chunk: download shards, reconstruct.
			data, err := e.downloadErasureChunk(chunk, locs)
			if err != nil {
				abandon()
				return nil, fmt.Errorf("reconstructing chunk %d: %w", chunk.Sequence, err)
			}
			if _, err := mw.Write(data); err != nil {
				abandon()
				return nil, fmt.Errorf("writing chunk %d: %w", chunk.Sequence, err)
			}
		} else {
			// Non-erasure: download from first location.
			loc := locs[0]
			provider, err := e.db.GetProvider(loc.ProviderID)
			if err != nil || provider == nil {
				abandon()
				return nil, fmt.Errorf("getting provider for chunk %s: %w", chunk.ID, err)
			}

			rc, err := e.rc.GetFile(provider.RcloneRemote, loc.RemotePath)
			if err != nil {
				abandon()
				return nil, fmt.Errorf("downloading chunk %d from %s: %w", chunk.Sequence, provider.DisplayName, err)
			}
			slog.Debug("chunk download started", "path", virtualPath, "chunk", chunk.Sequence+1, "total", len(chunks), "provider", provider.DisplayName)

			if _, err := io.Copy(mw, rc); err != nil {
				rc.Close()
				abandon()
				return nil, fmt.Errorf("downloading chunk %d: %w", chunk.Sequence, err)
			}
			rc.Close()
		}

		if hex.EncodeToString(chunkHasher.Sum(nil)) != chunk.SHA256 {
			abandon()
			return nil, fmt.Errorf("chunk %d hash mismatch for %s", chunk.Sequence, virtualPath)
		}
	}

	if hex.EncodeToString(fullHasher.Sum(nil)) != file.SHA256Full {
		abandon()
		return nil, fmt.Errorf("file hash mismatch for %s", virtualPath)
	}

	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		abandon()
		return nil, err
	}

	slog.Info("file read", "path", virtualPath, "size", file.SizeBytes)
	e.incCounter(&e.filesDownloaded, "files_downloaded", 1)
	e.incCounter(&e.bytesDownloaded, "bytes_downloaded", file.SizeBytes)
	return tmp, nil
}

// StreamFile downloads and streams a file directly to w as bytes
// arrive from the cloud—giving the browser a smooth, continuous download
// instead of bursty chunk-sized jumps.
//
// Data flows through in chunks: cloud → rclone HTTP stream → browser.
// The per-chunk SHA-256 is verified after the whole chunk has been sent;
// a mismatch aborts the connection and is logged for investigation.
func (e *Engine) StreamFile(ctx context.Context, virtualPath string, w io.Writer) error {
	slog.Info("stream download started", "path", virtualPath)
	file, err := e.db.GetCompleteFileByPath(virtualPath)
	if err != nil {
		return fmt.Errorf("looking up file: %w", err)
	}
	if file == nil {
		if any, _ := e.db.GetFileByPath(virtualPath); any != nil {
			return fmt.Errorf("file upload in progress: %s", virtualPath)
		}
		return fmt.Errorf("file not found: %s", virtualPath)
	}

	chunks, err := e.db.GetChunksForFile(file.ID)
	if err != nil {
		return fmt.Errorf("getting chunks: %w", err)
	}
	slog.Info("stream download plan", "path", virtualPath, "size", file.SizeBytes, "chunks", len(chunks))

	for i, c := range chunks {
		if c.Sequence != i {
			return fmt.Errorf("chunk sequence gap at index %d: expected seq %d, got %d for %s",
				i, i, c.Sequence, virtualPath)
		}
	}

	// Wrap w so every Write is followed by an HTTP flush, pushing data
	// to the browser immediately instead of buffering in net/http.
	type flusher interface{ Flush() }
	fw := w
	if fl, ok := w.(flusher); ok {
		fw = &autoFlushWriter{w: w, fl: fl}
	}

	fullHasher := sha256.New()

	for i, chunk := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		locs, err := e.db.GetChunkLocations(chunk.ID)
		if err != nil {
			return fmt.Errorf("getting chunk locations: %w", err)
		}
		if len(locs) == 0 {
			return fmt.Errorf("no locations for chunk %s", chunk.ID)
		}

		chunkHasher := sha256.New()

		if chunk.DataShards > 1 || chunk.ParityShards > 0 {
			// Erasure-coded chunk: download shards, reconstruct.
			slog.Info("stream chunk download (erasure)", "path", virtualPath, "chunk", i+1, "total", len(chunks))
			data, err := e.downloadErasureChunk(chunk, locs)
			if err != nil {
				return fmt.Errorf("reconstructing chunk %d: %w", chunk.Sequence, err)
			}
			chunkHasher.Write(data)
			fullHasher.Write(data)
			if _, err := fw.Write(data); err != nil {
				return fmt.Errorf("writing chunk %d: %w", chunk.Sequence, err)
			}
		} else {
			// Non-erasure: stream from first location.
			loc := locs[0]
			provider, err := e.db.GetProvider(loc.ProviderID)
			if err != nil || provider == nil {
				return fmt.Errorf("getting provider for chunk %s: %w", chunk.ID, err)
			}

			slog.Info("stream chunk download", "path", virtualPath, "chunk", i+1, "total", len(chunks), "provider", provider.DisplayName)

			rc, err := e.rc.StreamGetFile(provider.RcloneRemote, loc.RemotePath)
			if err != nil {
				return fmt.Errorf("downloading chunk %d from %s: %w", chunk.Sequence, provider.DisplayName, err)
			}

			mw := io.MultiWriter(fw, chunkHasher, fullHasher)
			if _, err := io.Copy(mw, rc); err != nil {
				rc.Close()
				return fmt.Errorf("downloading chunk %d: %w", chunk.Sequence, err)
			}
			rc.Close()
		}

		if hex.EncodeToString(chunkHasher.Sum(nil)) != chunk.SHA256 {
			// AES-GCM passed but SHA-256 mismatch — very unlikely but log it.
			slog.Error("stream chunk hash mismatch (data already sent)",
				"path", virtualPath, "chunk", chunk.Sequence)
			return fmt.Errorf("chunk %d hash mismatch for %s", chunk.Sequence, virtualPath)
		}
		slog.Info("stream chunk verified", "path", virtualPath, "chunk", i+1, "total", len(chunks))
	}

	if hex.EncodeToString(fullHasher.Sum(nil)) != file.SHA256Full {
		slog.Error("stream download full-file hash mismatch (data already sent)", "path", virtualPath)
		return fmt.Errorf("file hash mismatch for %s", virtualPath)
	}

	slog.Info("stream download complete", "path", virtualPath, "size", file.SizeBytes)
	e.incCounter(&e.filesDownloaded, "files_downloaded", 1)
	e.incCounter(&e.bytesDownloaded, "bytes_downloaded", file.SizeBytes)
	return nil
}

// autoFlushWriter wraps an io.Writer and calls Flush after each Write,
// pushing data to the HTTP client immediately.
type autoFlushWriter struct {
	w  io.Writer
	fl interface{ Flush() }
}

func (a *autoFlushWriter) Write(p []byte) (int, error) {
	n, err := a.w.Write(p)
	if n > 0 {
		a.fl.Flush()
	}
	return n, err
}
