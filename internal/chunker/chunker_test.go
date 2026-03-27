package chunker

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
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
