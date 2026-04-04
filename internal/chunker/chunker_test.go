package chunker

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"
)

func TestSplitAndAssemble(t *testing.T) {
	// Use an explicit small chunk size so the test doesn't depend on DefaultChunkSize.
	const testChunkSize = 4 * 1024 * 1024 // 4 MB
	// 20 MB of random data → 5 chunks
	data := make([]byte, 20*1024*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	chunks, err := Split(bytes.NewReader(data), testChunkSize)
	if err != nil {
		t.Fatal(err)
	}

	// 20 MB / 4 MB = 5 chunks
	if len(chunks) != 5 {
		t.Fatalf("expected 5 chunks, got %d", len(chunks))
	}

	for i, c := range chunks {
		if c.Sequence != i {
			t.Errorf("chunk %d has sequence %d", i, c.Sequence)
		}
		if c.Size != testChunkSize {
			t.Errorf("chunk %d size = %d, want %d", i, c.Size, testChunkSize)
		}
		hash := sha256.Sum256(c.Data)
		if hex.EncodeToString(hash[:]) != c.SHA256 {
			t.Errorf("chunk %d SHA256 mismatch", i)
		}
	}

	// Reassemble
	dc := make([]DecryptedChunk, len(chunks))
	for i, c := range chunks {
		dc[i] = DecryptedChunk{Sequence: c.Sequence, Data: c.Data, SHA256: c.SHA256}
	}
	reader, err := Assemble(dc)
	if err != nil {
		t.Fatal(err)
	}
	reassembled, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, reassembled) {
		t.Error("reassembled data does not match original")
	}
}

func TestSplitSmallFile(t *testing.T) {
	data := []byte("hello world")
	chunks, err := Split(bytes.NewReader(data), DefaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Size != len(data) {
		t.Errorf("chunk size = %d, want %d", chunks[0].Size, len(data))
	}
}

func TestSplitEmpty(t *testing.T) {
	chunks, err := Split(bytes.NewReader(nil), DefaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks, got %d", len(chunks))
	}
}

func TestEncryptDecryptRoundtrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("the quick brown fox jumps over the lazy dog")

	encrypted, err := Encrypt(key, plaintext)
	if err != nil {
		t.Fatal(err)
	}

	// Encrypted should be longer: nonce (12) + ciphertext + tag (16)
	if len(encrypted) <= len(plaintext) {
		t.Error("encrypted data should be longer than plaintext")
	}

	decrypted, err := Decrypt(key, encrypted)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(plaintext, decrypted) {
		t.Error("decrypted data does not match plaintext")
	}
}

func TestDecryptTamperedData(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	encrypted, _ := Encrypt(key, []byte("secret"))
	// Flip a byte in the ciphertext
	encrypted[len(encrypted)-1] ^= 0xff

	_, err := Decrypt(key, encrypted)
	if err == nil {
		t.Error("expected error when decrypting tampered data")
	}
}

func TestDecryptWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	rand.Read(key1)
	rand.Read(key2)

	encrypted, _ := Encrypt(key1, []byte("secret"))

	_, err := Decrypt(key2, encrypted)
	if err == nil {
		t.Error("expected error when decrypting with wrong key")
	}
}

func TestFullPipeline(t *testing.T) {
	key := make([]byte, 32)
	rand.Read(key)

	// 10 MB random file
	original := make([]byte, 10*1024*1024)
	rand.Read(original)

	// Split
	chunks, err := Split(bytes.NewReader(original), DefaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}

	// Encrypt each chunk
	encryptedChunks := make([][]byte, len(chunks))
	for i, c := range chunks {
		enc, err := Encrypt(key, c.Data)
		if err != nil {
			t.Fatalf("encrypt chunk %d: %v", i, err)
		}
		encryptedChunks[i] = enc
	}

	// Decrypt each chunk and build DecryptedChunks
	dc := make([]DecryptedChunk, len(chunks))
	for i, c := range chunks {
		dec, err := Decrypt(key, encryptedChunks[i])
		if err != nil {
			t.Fatalf("decrypt chunk %d: %v", i, err)
		}
		dc[i] = DecryptedChunk{
			Sequence: c.Sequence,
			Data:     dec,
			SHA256:   c.SHA256,
		}
	}

	// Assemble
	reader, err := Assemble(dc)
	if err != nil {
		t.Fatal(err)
	}
	reassembled, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(original, reassembled) {
		t.Error("full pipeline: reassembled data does not match original")
	}
}

func TestChunkSizeForFile(t *testing.T) {
	cases := []struct {
		name     string
		fileSize int64
		wantMin  int
		wantMax  int
	}{
		{"zero", 0, DefaultChunkSize, DefaultChunkSize},
		{"1 MB", 1 * 1024 * 1024, DefaultChunkSize, DefaultChunkSize},
		{"100 MB", 100 * 1024 * 1024, DefaultChunkSize, DefaultChunkSize},
		{"500 MB", 500 * 1024 * 1024, DefaultChunkSize, DefaultChunkSize},
		{"1 GB", 1024 * 1024 * 1024, DefaultChunkSize, MaxChunkSize},
		{"2 GB", 2 * 1024 * 1024 * 1024, DefaultChunkSize, MaxChunkSize},
		{"10 GB", 10 * 1024 * 1024 * 1024, MaxChunkSize, MaxChunkSize},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ChunkSizeForFile(tc.fileSize)
			if got < tc.wantMin || got > tc.wantMax {
				t.Errorf("ChunkSizeForFile(%d) = %d, want [%d, %d]",
					tc.fileSize, got, tc.wantMin, tc.wantMax)
			}
			// chunk count sanity: with target=25, never exceed targetChunkCount*4
			if tc.fileSize > 0 {
				chunkCount := int(tc.fileSize)/got + 1
				if chunkCount > targetChunkCount*4 {
					t.Errorf("too many chunks: fileSize=%d chunkSize=%d count=%d",
						tc.fileSize, got, chunkCount)
				}
			}
		})
	}
}

// ── ChunkReader tests ────────────────────────────────────────────────────────

// TestChunkReader_MultiChunk verifies streaming chunking produces the same
// result as buffered Split.
func TestChunkReader_MultiChunk(t *testing.T) {
	const chunkSize = 1024
	data := make([]byte, 3*chunkSize+500) // 3 full chunks + 1 partial
	rand.Read(data)

	cr := NewChunkReader(bytes.NewReader(data), chunkSize)
	var chunks []Chunk
	for {
		c, err := cr.Next()
		if err != nil {
			t.Fatalf("ChunkReader.Next: %v", err)
		}
		if c == nil {
			break
		}
		chunks = append(chunks, *c)
	}

	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}
	for i, c := range chunks {
		if c.Sequence != i {
			t.Errorf("chunk %d: sequence=%d, want %d", i, c.Sequence, i)
		}
	}
	// Last chunk should be the partial.
	if chunks[3].Size != 500 {
		t.Errorf("last chunk size=%d, want 500", chunks[3].Size)
	}

	// Reassemble and compare.
	var reassembled []byte
	for _, c := range chunks {
		reassembled = append(reassembled, c.Data...)
	}
	if !bytes.Equal(reassembled, data) {
		t.Error("ChunkReader data mismatch after reassembly")
	}
}

// TestChunkReader_Empty verifies an empty reader returns nil on first Next().
func TestChunkReader_Empty(t *testing.T) {
	cr := NewChunkReader(bytes.NewReader(nil), DefaultChunkSize)
	c, err := cr.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != nil {
		t.Error("expected nil chunk for empty reader")
	}
}

// TestChunkReader_SingleByte verifies a single byte produces one chunk.
func TestChunkReader_SingleByte(t *testing.T) {
	cr := NewChunkReader(bytes.NewReader([]byte{0x42}), DefaultChunkSize)
	c, err := cr.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if c == nil {
		t.Fatal("expected one chunk")
	}
	if c.Size != 1 || c.Data[0] != 0x42 {
		t.Errorf("unexpected chunk: size=%d data=%x", c.Size, c.Data)
	}
	// Second call should be nil.
	c2, _ := cr.Next()
	if c2 != nil {
		t.Error("expected nil after last chunk")
	}
}

// TestChunkReader_ExactMultiple verifies a file exactly divisible by chunk size.
func TestChunkReader_ExactMultiple(t *testing.T) {
	const chunkSize = 256
	data := make([]byte, chunkSize*3) // exactly 3 chunks
	rand.Read(data)

	cr := NewChunkReader(bytes.NewReader(data), chunkSize)
	var count int
	for {
		c, err := cr.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if c == nil {
			break
		}
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 chunks, got %d", count)
	}
}

// ── Assemble edge-case tests ────────────────────────────────────────────────

// TestAssemble_OutOfOrder verifies chunks are reordered by sequence.
func TestAssemble_OutOfOrder(t *testing.T) {
	// Create 3 chunks in order 2, 0, 1.
	parts := [][]byte{[]byte("AAAA"), []byte("BBBB"), []byte("CCCC")}
	var dcs []DecryptedChunk
	for i, p := range parts {
		h := sha256.Sum256(p)
		dcs = append(dcs, DecryptedChunk{
			Sequence: i, Data: p, SHA256: hex.EncodeToString(h[:]),
		})
	}
	// Shuffle: put in order 2, 0, 1.
	shuffled := []DecryptedChunk{dcs[2], dcs[0], dcs[1]}

	r, err := Assemble(shuffled)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	got, _ := io.ReadAll(r)
	want := []byte("AAAABBBBCCCC")
	if !bytes.Equal(got, want) {
		t.Errorf("out-of-order assemble: got %q, want %q", got, want)
	}
}

// TestAssemble_HashMismatch verifies a corrupted chunk is detected.
func TestAssemble_HashMismatch(t *testing.T) {
	dcs := []DecryptedChunk{{
		Sequence: 0,
		Data:     []byte("data"),
		SHA256:   "0000000000000000000000000000000000000000000000000000000000000000",
	}}
	_, err := Assemble(dcs)
	if err == nil {
		t.Fatal("expected hash mismatch error")
	}
	if !strings.Contains(err.Error(), "hash mismatch") {
		t.Errorf("error should mention hash mismatch, got: %v", err)
	}
}

// TestAssemble_Empty verifies empty input produces an empty reader.
func TestAssemble_Empty(t *testing.T) {
	r, err := Assemble(nil)
	if err != nil {
		t.Fatalf("Assemble(nil): %v", err)
	}
	got, _ := io.ReadAll(r)
	if len(got) != 0 {
		t.Errorf("expected empty output, got %d bytes", len(got))
	}
}
