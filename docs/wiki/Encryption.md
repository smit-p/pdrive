# Encryption

pdrive uses client-side encryption so that cloud providers never see your plaintext data. Every chunk is encrypted independently before upload.

## Key Derivation

- **Algorithm:** Argon2id (memory-hard, resistant to GPU/ASIC attacks)
- **Salt:** 16 bytes from `crypto/rand`, stored in `~/.config/pdrive/salt`
- **Parameters:** Time=1, Memory=64 MB, Threads=4, KeyLen=32 bytes
- **Output:** 256-bit key used for all AES-256-GCM operations

The salt is generated once on first run and persists across sessions. The passphrase is prompted at daemon startup and never stored on disk.

## Chunk Encryption

Each chunk is encrypted independently using AES-256-GCM:

```
┌─────────────────────────────────────────────┐
│  Encrypted chunk layout                     │
│                                              │
│  [12-byte nonce][ciphertext][16-byte tag]    │
│                                              │
│  • Nonce: crypto/rand, unique per chunk     │
│  • Ciphertext: AES-256-GCM encrypted data  │
│  • Tag: GCM authentication tag              │
└─────────────────────────────────────────────┘
```

- **Nonce:** 12 bytes from `crypto/rand` (never reused)
- **Mode:** AES-256-GCM (authenticated encryption with associated data)
- **Authentication:** GCM tag ensures both confidentiality and integrity

## Integrity Verification

Each chunk has a SHA-256 hash computed **before** encryption. On download:

1. Chunk is decrypted using the same key
2. SHA-256 of the plaintext is recomputed
3. Hash is compared against the stored value
4. Mismatch → error, data is rejected

## Metadata Backup Encryption

The SQLite metadata database is periodically backed up to all cloud providers. The backup is encrypted with the same AES-256-GCM key:

```
┌──────────────────────────────────────────────────────────────┐
│  Backup payload (before encryption)                          │
│                                                               │
│  [8-byte magic "pdriveDB"][8-byte timestamp][SQLite data]     │
└──────────────────────────────────────────────────────────────┘
```

The magic bytes allow verification that a decrypted backup is valid before attempting to restore it.

## Security Properties

| Property | Guarantee |
|----------|-----------|
| Confidentiality | AES-256-GCM — cloud providers cannot read your data |
| Integrity | GCM tag + SHA-256 hash — any tampering is detected |
| Key strength | 256-bit key from Argon2id — memory-hard against brute force |
| Nonce uniqueness | 12 bytes from crypto/rand per chunk — astronomically unlikely collision |
| Forward secrecy | Not applicable (single symmetric key) — re-encrypt if key is compromised |
