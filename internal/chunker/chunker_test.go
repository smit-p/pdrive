package chunker

import (
	"bytes"
	"crypto/aes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
		{"10 GB", 10 * 1024 * 1024 * 1024, DefaultChunkSize, MaxChunkSize},
		{"200 GB", 200 * 1024 * 1024 * 1024, MaxChunkSize, MaxChunkSize},
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

// --- Argon2id KDF tests ---

func TestDeriveKey_Deterministic(t *testing.T) {
	salt := make([]byte, SaltSize)
	copy(salt, "fixed-test-salt!")
	k1 := DeriveKey("hunter2", salt)
	k2 := DeriveKey("hunter2", salt)
	if !bytes.Equal(k1, k2) {
		t.Fatal("same password+salt must produce identical keys")
	}
}

func TestDeriveKey_Length(t *testing.T) {
	salt := make([]byte, SaltSize)
	key := DeriveKey("some-password", salt)
	if len(key) != 32 {
		t.Fatalf("expected 32-byte key, got %d", len(key))
	}
}

func TestDeriveKey_DifferentPasswords(t *testing.T) {
	salt := make([]byte, SaltSize)
	k1 := DeriveKey("password-a", salt)
	k2 := DeriveKey("password-b", salt)
	if bytes.Equal(k1, k2) {
		t.Fatal("different passwords must produce different keys")
	}
}

func TestDeriveKey_DifferentSalts(t *testing.T) {
	s1 := make([]byte, SaltSize)
	s2 := make([]byte, SaltSize)
	s2[0] = 0xFF
	k1 := DeriveKey("same-password", s1)
	k2 := DeriveKey("same-password", s2)
	if bytes.Equal(k1, k2) {
		t.Fatal("different salts must produce different keys")
	}
}

func TestGenerateSalt_Length(t *testing.T) {
	salt, err := GenerateSalt()
	if err != nil {
		t.Fatal(err)
	}
	if len(salt) != SaltSize {
		t.Fatalf("expected %d-byte salt, got %d", SaltSize, len(salt))
	}
}

func TestGenerateSalt_Unique(t *testing.T) {
	s1, _ := GenerateSalt()
	s2, _ := GenerateSalt()
	if bytes.Equal(s1, s2) {
		t.Fatal("two GenerateSalt calls should produce different salts")
	}
}

func TestPasswordDerivedKey_EncryptRoundtrip(t *testing.T) {
	salt, _ := GenerateSalt()
	key := DeriveKey("correct horse battery staple", salt)
	plaintext := []byte("top secret data")

	ct, err := Encrypt(key, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decrypt(key, ct)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatal("round-trip mismatch")
	}

	// Wrong password can't decrypt.
	badKey := DeriveKey("wrong password", salt)
	if _, err := Decrypt(badKey, ct); err == nil {
		t.Fatal("expected decryption failure with wrong password-derived key")
	}
}

// ── additional coverage tests ────────────────────────────────────────────────

// TestSplit_DefaultChunkSize verifies Split uses DefaultChunkSize when given 0.
func TestSplit_DefaultChunkSize(t *testing.T) {
	data := []byte("small")
	chunks, err := Split(bytes.NewReader(data), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Size != len(data) {
		t.Errorf("expected size %d, got %d", len(data), chunks[0].Size)
	}
}

// TestSplit_NegativeChunkSize verifies Split treats negative chunkSize as default.
func TestSplit_NegativeChunkSize(t *testing.T) {
	data := []byte("tiny")
	chunks, err := Split(bytes.NewReader(data), -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
}

// TestSplit_ReaderError verifies Split propagates reader errors.
func TestSplit_ReaderError(t *testing.T) {
	r := &errReader{err: io.ErrClosedPipe, afterN: 5}
	_, err := Split(r, 10)
	if err == nil {
		t.Fatal("expected error from Split")
	}
}

type errReader struct {
	err    error
	afterN int
	n      int
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.n >= r.afterN {
		return 0, r.err
	}
	n := len(p)
	if n > r.afterN-r.n {
		n = r.afterN - r.n
	}
	r.n += n
	return n, nil
}

// TestNewChunkReader_DefaultChunkSize tests NewChunkReader with 0 chunkSize.
func TestNewChunkReader_DefaultChunkSize(t *testing.T) {
	cr := NewChunkReader(bytes.NewReader([]byte("hello")), 0)
	if cr.chunkSize != DefaultChunkSize {
		t.Errorf("expected chunkSize=%d, got %d", DefaultChunkSize, cr.chunkSize)
	}
}

// TestChunkReader_MultiChunk tests ChunkReader returning multiple chunks.
func TestChunkReader_MultiChunk_Partial(t *testing.T) {
	data := []byte("0123456789abcdef") // 16 bytes
	cr := NewChunkReader(bytes.NewReader(data), 5)
	var chunks []*Chunk
	for {
		c, err := cr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			break
		}
		chunks = append(chunks, c)
	}
	// 16/5 = 3 full + 1 partial = 4 chunks (sizes: 5, 5, 5, 1)
	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks, got %d", len(chunks))
	}
	if chunks[3].Size != 1 {
		t.Errorf("last chunk size: expected 1, got %d", chunks[3].Size)
	}
}

// TestChunkReader_ReaderError tests ChunkReader error propagation.
func TestChunkReader_ReaderError(t *testing.T) {
	r := &errReader{err: io.ErrClosedPipe, afterN: 3}
	cr := NewChunkReader(r, 10)
	_, err := cr.Next()
	if err == nil {
		t.Fatal("expected error from ChunkReader.Next")
	}
}

// TestChunkReader_Empty tests ChunkReader on empty input.
func TestChunkReader_EmptyInput(t *testing.T) {
	cr := NewChunkReader(bytes.NewReader(nil), 10)
	c, err := cr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Error("expected nil chunk for empty reader")
	}
}

// TestAssemble_SequenceGap_NonContiguous tests that non-contiguous sequences cause an error.
func TestAssemble_SequenceGap_NonContiguous(t *testing.T) {
	h := sha256.Sum256([]byte("data"))
	hx := hex.EncodeToString(h[:])
	chunks := []DecryptedChunk{
		{Sequence: 0, Data: []byte("data"), SHA256: hx},
		{Sequence: 2, Data: []byte("data"), SHA256: hx}, // gap: missing 1
	}
	_, err := Assemble(chunks)
	if err == nil {
		t.Fatal("expected sequence gap error")
	}
	if !strings.Contains(err.Error(), "gap") {
		t.Errorf("expected 'gap' in error, got: %s", err)
	}
}

// TestAssemble_HashMismatch_Corruption tests that hash verification catches corruption.
func TestAssemble_HashMismatch_Corruption(t *testing.T) {
	chunks := []DecryptedChunk{
		{Sequence: 0, Data: []byte("real"), SHA256: "badhash"},
	}
	_, err := Assemble(chunks)
	if err == nil {
		t.Fatal("expected hash mismatch error")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Errorf("expected 'mismatch' in error, got: %s", err)
	}
}

// TestAssemble_NilSlice tests assembling nil chunks.
func TestAssemble_NilSlice(t *testing.T) {
	r, err := Assemble(nil)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(r)
	if len(data) != 0 {
		t.Errorf("expected empty, got %d bytes", len(data))
	}
}

// TestEncrypt_BadKey tests Encrypt with an invalid key size.
func TestEncrypt_BadKey(t *testing.T) {
	_, err := Encrypt([]byte("short"), []byte("data"))
	if err == nil {
		t.Fatal("expected error for short key")
	}
}

// TestDecrypt_BadKey tests Decrypt with an invalid key size.
func TestDecrypt_BadKey(t *testing.T) {
	// Create minimal valid-looking blob (nonce + something)
	blob := make([]byte, NonceSize+aes.BlockSize+1)
	_, err := Decrypt([]byte("short"), blob)
	if err == nil {
		t.Fatal("expected error for short key")
	}
}

// TestDecrypt_TooShort tests Decrypt with blob shorter than minimum.
func TestDecrypt_TooShort(t *testing.T) {
	key := make([]byte, 32)
	_, err := Decrypt(key, []byte("tiny"))
	if err == nil {
		t.Fatal("expected error for too-short blob")
	}
}

// TestChunkSizeForFile_Negative tests negative file size.
func TestChunkSizeForFile_Negative(t *testing.T) {
	s := ChunkSizeForFile(-1)
	if s != DefaultChunkSize {
		t.Errorf("expected DefaultChunkSize, got %d", s)
	}
}

// TestChunkSizeForFile_VeryLarge tests capping at MaxChunkSize.
func TestChunkSizeForFile_VeryLarge(t *testing.T) {
	s := ChunkSizeForFile(200 * 1024 * 1024 * 1024) // 200 GB
	if s != MaxChunkSize {
		t.Errorf("expected MaxChunkSize (%d), got %d", MaxChunkSize, s)
	}
}

// TestChunkReader_ReaderError_ZeroBytes tests error when reader returns 0 bytes
// with a non-EOF error — exercises the n==0, non-EOF branch in Next.
func TestChunkReader_ReaderError_ZeroBytes(t *testing.T) {
	r := &errReader{err: io.ErrClosedPipe, afterN: 0}
	cr := NewChunkReader(r, 10)
	_, err := cr.Next()
	if err == nil {
		t.Fatal("expected error from ChunkReader.Next with zero-byte error reader")
	}
	if err != io.ErrClosedPipe {
		t.Errorf("expected ErrClosedPipe, got %v", err)
	}
}

// TestChunkReader_PartialReadError tests that Next returns error (not chunk)
// when partial read + non-EOF error happens mid-chunk.
func TestChunkReader_PartialReadError(t *testing.T) {
	r := &errReader{err: fmt.Errorf("disk failure"), afterN: 5}
	cr := NewChunkReader(r, 10)
	c, err := cr.Next()
	if err == nil {
		t.Fatal("expected error")
	}
	if c != nil {
		t.Error("expected nil chunk on error")
	}
}

// TestEncrypt_EmptyPlaintext tests encryption of empty data.
func TestEncrypt_EmptyPlaintext(t *testing.T) {
	key := make([]byte, 32)
	ct, err := Encrypt(key, []byte{})
	if err != nil {
		t.Fatal(err)
	}
	pt, err := Decrypt(key, ct)
	if err != nil {
		t.Fatal(err)
	}
	if len(pt) != 0 {
		t.Errorf("expected empty, got %d bytes", len(pt))
	}
}

func TestEncrypt_InvalidKeyLength(t *testing.T) {
	_, err := Encrypt([]byte("short"), []byte("data"))
	if err == nil {
		t.Fatal("expected error for invalid key length")
	}
}

func TestDecrypt_InvalidKeyLength(t *testing.T) {
	// Need valid-looking ciphertext (at least NonceSize+BlockSize bytes).
	blob := make([]byte, NonceSize+aes.BlockSize+1)
	_, err := Decrypt([]byte("short"), blob)
	if err == nil {
		t.Fatal("expected error for invalid key length")
	}
}

// ── PlanChunks tests ─────────────────────────────────────────────────────────

func TestPlanChunks_FitsOnOneRemote_SmallFile(t *testing.T) {
	// 100 MB file, one remote with 1 GB free → single chunk = whole file.
	s := PlanChunks(100*1024*1024, []int64{1024 * 1024 * 1024})
	est := s.EstimateChunks(100 * 1024 * 1024)
	if est != 1 {
		t.Fatalf("expected 1 chunk, got %d", est)
	}
	if s.MaxSize() != 100*1024*1024 {
		t.Fatalf("expected chunk size = file size (100 MB), got %d", s.MaxSize())
	}
}

func TestPlanChunks_FitsOnOneRemote_LargeFile(t *testing.T) {
	// 8 GiB file, one remote with 20 GiB free → chunks capped at MaxChunkSize.
	fileSize := int64(8) * 1024 * 1024 * 1024
	s := PlanChunks(fileSize, []int64{20 * 1024 * 1024 * 1024})
	est := s.EstimateChunks(fileSize)
	if est != 2 {
		t.Fatalf("expected 2 chunks (8 GiB / 4 GiB cap), got %d", est)
	}
	if s.MaxSize() != MaxChunkSize {
		t.Fatalf("expected MaxChunkSize, got %d", s.MaxSize())
	}
}

func TestPlanChunks_SpansMultipleRemotes(t *testing.T) {
	// 5 GiB file, remotes: [3 GiB, 3 GiB]. Must span both.
	gib := int64(1024 * 1024 * 1024)
	fileSize := 5 * gib
	s := PlanChunks(fileSize, []int64{3 * gib, 3 * gib})
	est := s.EstimateChunks(fileSize)
	if est != 2 {
		t.Fatalf("expected 2 chunks, got %d", est)
	}
}

func TestPlanChunks_GreedyFillsLargestFirst(t *testing.T) {
	// 10 GiB file, remotes: [6 GiB, 3 GiB, 2 GiB].
	// Greedy: 6 GiB-overhead on r1, 3 GiB-overhead on r2, ~1 GiB on r3.
	gib := int64(1024 * 1024 * 1024)
	fileSize := 10 * gib
	freeSpaces := []int64{6 * gib, 3 * gib, 2 * gib}
	s := PlanChunks(fileSize, freeSpaces)
	est := s.EstimateChunks(fileSize)
	// Should use 3 remotes with chunks close to their capacity.
	if est < 3 || est > 4 {
		t.Fatalf("expected 3-4 chunks for 10 GiB across [6,3,2] GiB remotes, got %d", est)
	}
}

func TestPlanChunks_UniformCollapse(t *testing.T) {
	// 20 GiB file, all remotes have plenty of space.
	// All chunks should be MaxChunkSize (4 GiB), collapsed to one tier.
	gib := int64(1024 * 1024 * 1024)
	fileSize := 20 * gib
	s := PlanChunks(fileSize, []int64{50 * gib, 30 * gib})
	if len(s.Tiers) != 1 {
		t.Fatalf("expected 1 collapsed tier, got %d tiers", len(s.Tiers))
	}
	if s.Tiers[0].Size != MaxChunkSize {
		t.Fatalf("expected tier size %d, got %d", MaxChunkSize, s.Tiers[0].Size)
	}
	est := s.EstimateChunks(fileSize)
	if est != 5 {
		t.Fatalf("expected 5 chunks (20 GiB / 4 GiB), got %d", est)
	}
}

func TestPlanChunks_NoUsableRemotes(t *testing.T) {
	// No remotes with space → fallback to DefaultChunkSize.
	s := PlanChunks(1024*1024, []int64{0, 10})
	if s.MaxSize() != DefaultChunkSize {
		t.Fatalf("expected DefaultChunkSize fallback, got %d", s.MaxSize())
	}
}

func TestPlanChunks_ZeroFileSize(t *testing.T) {
	s := PlanChunks(0, []int64{1024 * 1024 * 1024})
	if s.MaxSize() != DefaultChunkSize {
		t.Fatalf("expected DefaultChunkSize for zero-size file, got %d", s.MaxSize())
	}
}
