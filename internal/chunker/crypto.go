package chunker

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

const (
	NonceSize = 12 // AES-GCM standard nonce size
	SaltSize  = 16 // Argon2id salt size
	gcmTagLen = 16 // AES-GCM authentication tag

	// encBlockSize is the plaintext block size for streaming encryption.
	// Each block is independently GCM-encrypted so peak memory stays bounded.
	encBlockSize = 16 * 1024 * 1024 // 16 MB
)

// streamMagic identifies the streaming encryption format.
// Legacy format starts with a random 12-byte nonce; the probability of a
// random nonce matching this prefix is negligible.
var streamMagic = [4]byte{'P', 'D', 'S', 0x01}

// Argon2id parameters — OWASP recommended minimums for interactive logins.
const (
	argon2Time    = 3         // iterations
	argon2Memory  = 64 * 1024 // 64 MB
	argon2Threads = 4
	argon2KeyLen  = 32 // AES-256
)

// DeriveKey derives a 32-byte AES-256 key from a password and salt using Argon2id.
// The salt must be exactly SaltSize (16) bytes.
func DeriveKey(password string, salt []byte) []byte {
	return argon2.IDKey([]byte(password), salt, argon2Time, argon2Memory, argon2Threads, argon2KeyLen)
}

// GenerateSalt returns a cryptographically random salt for use with DeriveKey.
func GenerateSalt() ([]byte, error) {
	salt := make([]byte, SaltSize)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, err
	}
	return salt, nil
}

// Encrypt encrypts plaintext using AES-256-GCM.
// Returns: [12-byte nonce][ciphertext + 16-byte GCM tag]
func Encrypt(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Seal appends the ciphertext+tag to nonce
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts a blob produced by Encrypt (legacy single-block format).
// Expects: [12-byte nonce][ciphertext + 16-byte GCM tag]
func Decrypt(key, blob []byte) ([]byte, error) {
	if len(blob) < NonceSize+aes.BlockSize {
		return nil, errors.New("ciphertext too short")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := blob[:NonceSize]
	ciphertext := blob[NonceSize:]

	return gcm.Open(nil, nonce, ciphertext, nil)
}

// EncryptStream reads plaintext from src in encBlockSize (16 MB) blocks,
// encrypts each block independently with AES-256-GCM, and writes them to dst.
//
// Wire format:
//
//	[4-byte magic "PDS\x01"]
//	repeated {
//	    [4-byte big-endian: encrypted block length = NonceSize + plainLen + gcmTagLen]
//	    [12-byte nonce]
//	    [ciphertext + 16-byte GCM tag]
//	}
//
// Peak memory: ~encBlockSize + encryption overhead.
func EncryptStream(key []byte, src io.Reader, dst io.Writer) (int64, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return 0, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return 0, err
	}

	if _, err := dst.Write(streamMagic[:]); err != nil {
		return 0, err
	}
	written := int64(len(streamMagic))

	buf := make([]byte, encBlockSize)
	var lenBuf [4]byte

	for {
		n, readErr := io.ReadFull(src, buf)
		if n == 0 {
			if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
				break
			}
			if readErr != nil {
				return written, readErr
			}
			break
		}

		nonce := make([]byte, NonceSize)
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return written, err
		}

		sealed := gcm.Seal(nonce, nonce, buf[:n], nil) // nonce || ciphertext+tag
		encLen := uint32(len(sealed))
		binary.BigEndian.PutUint32(lenBuf[:], encLen)

		if _, err := dst.Write(lenBuf[:]); err != nil {
			return written, err
		}
		if _, err := dst.Write(sealed); err != nil {
			return written, err
		}
		written += int64(4 + len(sealed))

		if readErr == io.ErrUnexpectedEOF {
			break // last block was short
		}
		if readErr != nil && readErr != io.EOF {
			return written, readErr
		}
	}
	return written, nil
}

// DecryptStream reads a stream-encrypted blob from src, decrypts each block,
// and writes plaintext to dst. Also computes and returns the SHA-256 hex digest
// of the plaintext for integrity verification.
func DecryptStream(key []byte, src io.Reader, dst io.Writer) error {
	block, err := aes.NewCipher(key)
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	// Read and verify magic.
	var magic [4]byte
	if _, err := io.ReadFull(src, magic[:]); err != nil {
		return fmt.Errorf("reading stream magic: %w", err)
	}
	if magic != streamMagic {
		return errors.New("not a streaming-encrypted blob (bad magic)")
	}

	var lenBuf [4]byte
	for {
		_, err := io.ReadFull(src, lenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // no more blocks
		}
		if err != nil {
			return fmt.Errorf("reading block length: %w", err)
		}

		encLen := binary.BigEndian.Uint32(lenBuf[:])
		if encLen < uint32(NonceSize+gcmTagLen) {
			return fmt.Errorf("encrypted block too short: %d bytes", encLen)
		}

		sealed := make([]byte, encLen)
		if _, err := io.ReadFull(src, sealed); err != nil {
			return fmt.Errorf("reading encrypted block: %w", err)
		}

		nonce := sealed[:NonceSize]
		ciphertext := sealed[NonceSize:]
		plain, err := gcm.Open(nil, nonce, ciphertext, nil)
		if err != nil {
			return fmt.Errorf("decrypting block: %w", err)
		}

		if _, err := dst.Write(plain); err != nil {
			return err
		}
	}
	return nil
}

// IsStreamFormat returns true if the blob starts with the streaming encryption
// magic bytes. Used to distinguish stream-encrypted data from legacy
// single-block GCM format during download.
func IsStreamFormat(header []byte) bool {
	if len(header) < 4 {
		return false
	}
	return header[0] == streamMagic[0] &&
		header[1] == streamMagic[1] &&
		header[2] == streamMagic[2] &&
		header[3] == streamMagic[3]
}

// EncStreamOverhead returns the encryption overhead (in bytes) that
// EncryptStream adds to a plaintext of the given size.
func EncStreamOverhead(plainSize int64) int64 {
	if plainSize <= 0 {
		return 0
	}
	blocks := (plainSize + encBlockSize - 1) / encBlockSize
	// magic + per-block: 4-byte length prefix + 12-byte nonce + 16-byte tag
	return int64(len(streamMagic)) + blocks*(4+NonceSize+gcmTagLen)
}
