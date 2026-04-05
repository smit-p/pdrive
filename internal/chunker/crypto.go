package chunker

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"

	"golang.org/x/crypto/argon2"
)

const (
	NonceSize = 12 // AES-GCM standard nonce size
	SaltSize  = 16 // Argon2id salt size
)

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

// Decrypt decrypts a blob produced by Encrypt.
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
