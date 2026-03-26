package chunker

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

const (
	NonceSize = 12 // AES-GCM standard nonce size
)

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
