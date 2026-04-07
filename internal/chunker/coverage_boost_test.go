package chunker

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

// ── EncryptStream write-error paths ─────────────────────────────────────────

type failWriter struct {
	failAfter int
	written   int
}

func (fw *failWriter) Write(p []byte) (int, error) {
	if fw.written >= fw.failAfter {
		return 0, errors.New("write failed")
	}
	fw.written += len(p)
	return len(p), nil
}

// Covers crypto.go:121 — magic write error
func TestEncryptStream_MagicWriteError(t *testing.T) {
	key := make([]byte, 32)
	fw := &failWriter{failAfter: 0}
	_, err := EncryptStream(key, strings.NewReader("hello"), fw)
	if err == nil {
		t.Fatal("expected error from magic write")
	}
}

// Covers crypto.go:150 — lenBuf write error
func TestEncryptStream_LenBufWriteError(t *testing.T) {
	key := make([]byte, 32)
	// Allow magic (4 bytes) but fail on length prefix
	fw := &failWriter{failAfter: 4}
	_, err := EncryptStream(key, strings.NewReader("hello"), fw)
	if err == nil {
		t.Fatal("expected error from length write")
	}
}

// Covers crypto.go:153 — sealed data write error
func TestEncryptStream_SealedWriteError(t *testing.T) {
	key := make([]byte, 32)
	// Allow magic (4) + lenBuf (4) = 8, but fail on sealed data write
	fw := &failWriter{failAfter: 8}
	_, err := EncryptStream(key, strings.NewReader("hello"), fw)
	if err == nil {
		t.Fatal("expected error from sealed write")
	}
}

// Covers crypto.go:135 — ReadFull returns n==0 with non-EOF/non-ErrUnexpectedEOF error
func TestEncryptStream_ReadZeroBytesNonEOFError(t *testing.T) {
	key := make([]byte, 32)
	r := iotest.ErrReader(errors.New("custom read error"))
	var buf bytes.Buffer
	_, err := EncryptStream(key, r, &buf)
	if err == nil || err.Error() != "custom read error" {
		t.Fatalf("expected custom read error, got: %v", err)
	}
}

// Covers crypto.go:161 — post-block non-EOF read error (returned from ReadFull on next iteration)
func TestEncryptStream_ReadErrorAfterFirstBlock(t *testing.T) {
	key := make([]byte, 32)
	// Create data exactly 1 block, followed by an error
	data := make([]byte, encBlockSize) // exactly one full block
	r := io.MultiReader(bytes.NewReader(data), iotest.ErrReader(errors.New("late error")))
	var buf bytes.Buffer
	_, err := EncryptStream(key, r, &buf)
	if err == nil || err.Error() != "late error" {
		t.Fatalf("expected late error, got: %v", err)
	}
}

// ── DecryptStream error paths ───────────────────────────────────────────────

// Covers crypto.go:183 — reading magic from empty reader
func TestDecryptStream_EmptyReader(t *testing.T) {
	key := make([]byte, 32)
	var buf bytes.Buffer
	err := DecryptStream(key, strings.NewReader(""), &buf)
	if err == nil || !strings.Contains(err.Error(), "stream magic") {
		t.Fatalf("expected stream magic error, got: %v", err)
	}
}

// Covers crypto.go:196 — block length read error mid-stream
func TestDecryptStream_PartialBlockLengthRead(t *testing.T) {
	key := make([]byte, 32)
	// Valid magic + partial length (only 2 bytes instead of 4)
	data := append(streamMagic[:], 0x00, 0x01)
	err := DecryptStream(key, bytes.NewReader(data), &bytes.Buffer{})
	// Should get EOF since we stop normally on partial length read
	if err != nil {
		t.Fatalf("expected nil (EOF on partial length) but got: %v", err)
	}
}

// Covers crypto.go:217 — block data read error (truncated block)
func TestDecryptStream_TruncatedBlockData(t *testing.T) {
	key := make([]byte, 32)
	// Valid magic + block length says 100 bytes but only 5 bytes follow
	var data bytes.Buffer
	data.Write(streamMagic[:])
	data.Write([]byte{0, 0, 0, 100}) // says 100 bytes
	data.Write([]byte{1, 2, 3, 4, 5})
	err := DecryptStream(key, &data, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "reading encrypted block") {
		t.Fatalf("expected block read error, got: %v", err)
	}
}

// Covers crypto.go:219 — block decryption failure (tampered data)
func TestDecryptStream_DecryptionError(t *testing.T) {
	key := make([]byte, 32)
	// Encrypt something, then tamper with the first encrypted block
	var encrypted bytes.Buffer
	EncryptStream(key, strings.NewReader("test data"), &encrypted)

	// Tamper with ciphertext (byte after magic + length + nonce)
	raw := encrypted.Bytes()
	if len(raw) > 4+4+NonceSize+1 {
		raw[4+4+NonceSize+1] ^= 0xFF
	}

	err := DecryptStream(key, bytes.NewReader(raw), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "decrypting block") {
		t.Fatalf("expected decryption error, got: %v", err)
	}
}

// ── Encrypt/Decrypt edge cases ──────────────────────────────────────────────

// Covers crypto.go:62 — NewCipher error in Encrypt with bad key
func TestEncrypt_BadKeyLength(t *testing.T) {
	_, err := Encrypt([]byte("short"), []byte("data"))
	if err == nil {
		t.Fatal("expected error for bad key length")
	}
}

// Covers crypto.go:88 — NewCipher error in Decrypt with bad key
func TestDecrypt_BadKeyLen(t *testing.T) {
	_, err := Decrypt([]byte("short"), make([]byte, 100))
	if err == nil {
		t.Fatal("expected error for bad key length")
	}
}

// Covers crypto.go:67 — impossible GCM creation error (can't easily trigger, but test NewCipher error path)
// Already covered by TestEncrypt_BadKeyLength above since cipher fails first.

// ── GenerateSalt ────────────────────────────────────────────────────────────

// Covers crypto.go:47 — can't easily trigger rand.Reader failure, but ensure normal path works
func TestGenerateSalt_Determinism(t *testing.T) {
	s1, err := GenerateSalt()
	if err != nil {
		t.Fatal(err)
	}
	s2, err := GenerateSalt()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(s1, s2) {
		t.Error("two salts should not be equal")
	}
}

// ── IsStreamFormat ──────────────────────────────────────────────────────────

func TestIsStreamFormat_TooShort(t *testing.T) {
	if IsStreamFormat([]byte{0x50}) {
		t.Error("should return false for 1-byte input")
	}
	if IsStreamFormat(nil) {
		t.Error("should return false for nil input")
	}
}

// ── EncStreamOverhead edge cases ────────────────────────────────────────────

func TestEncStreamOverhead_Zero(t *testing.T) {
	if EncStreamOverhead(0) != 0 {
		t.Error("expected 0 overhead for 0 size")
	}
}

func TestEncStreamOverhead_Negative(t *testing.T) {
	if EncStreamOverhead(-100) != 0 {
		t.Error("expected 0 overhead for negative size")
	}
}

func TestEncStreamOverhead_OneBlock(t *testing.T) {
	overhead := EncStreamOverhead(1)
	expected := int64(4) + 1*(4+int64(NonceSize)+int64(gcmTagLen))
	if overhead != expected {
		t.Errorf("got %d, want %d", overhead, expected)
	}
}

// ── DecryptStream write error ───────────────────────────────────────────────

func TestDecryptStream_WriteError(t *testing.T) {
	key := make([]byte, 32)
	// Encrypt valid data
	var encrypted bytes.Buffer
	_, err := EncryptStream(key, strings.NewReader("hello world"), &encrypted)
	if err != nil {
		t.Fatal(err)
	}

	// Decrypt to a writer that fails
	fw := &failWriter{failAfter: 0}
	err = DecryptStream(key, &encrypted, fw)
	if err == nil || err.Error() != "write failed" {
		t.Fatalf("expected write error, got: %v", err)
	}
}
