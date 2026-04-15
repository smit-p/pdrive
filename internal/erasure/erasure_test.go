package erasure

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestEncodeAndJoin(t *testing.T) {
	enc, err := NewEncoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 1024*1024) // 1 MB
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	shards, err := enc.Encode(data)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(shards) != 6 {
		t.Fatalf("expected 6 shards, got %d", len(shards))
	}

	for i := 1; i < len(shards); i++ {
		if len(shards[i]) != len(shards[0]) {
			t.Fatalf("shard %d size %d != shard 0 size %d", i, len(shards[i]), len(shards[0]))
		}
	}

	got, err := enc.Join(shards, len(data))
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("Join did not recover original data")
	}
}

func TestReconstructMissing(t *testing.T) {
	enc, err := NewEncoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	shards, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	shards[1] = nil
	shards[4] = nil

	if err := enc.Reconstruct(shards); err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}

	got, err := enc.Join(shards, len(data))
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("reconstructed data does not match original")
	}
}

func TestReconstructTooManyMissing(t *testing.T) {
	enc, err := NewEncoder(3, 2)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 30000)
	for i := range data {
		data[i] = byte(i)
	}
	shards, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	shards[0] = nil
	shards[2] = nil
	shards[4] = nil

	err = enc.Reconstruct(shards)
	if err == nil {
		t.Error("expected error when too many shards are missing")
	}
}

func TestShardSize(t *testing.T) {
	enc, err := NewEncoder(4, 2)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		dataLen   int
		wantShard int
	}{
		{0, 0},
		{1, 1},
		{4, 1},
		{5, 2},
		{100, 25},
		{1000000, 250000},
	}
	for _, tt := range tests {
		got := enc.ShardSize(tt.dataLen)
		if got != tt.wantShard {
			t.Errorf("ShardSize(%d) = %d, want %d", tt.dataLen, got, tt.wantShard)
		}
	}
}

func TestNewEncoderValidation(t *testing.T) {
	if _, err := NewEncoder(0, 1); err == nil {
		t.Error("expected error for dataShards=0")
	}
	if _, err := NewEncoder(1, 0); err == nil {
		t.Error("expected error for parityShards=0")
	}
}

func TestVerify(t *testing.T) {
	enc, err := NewEncoder(2, 1)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello, erasure coding!")
	shards, err := enc.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	ok, err := enc.Verify(shards)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !ok {
		t.Error("Verify returned false for valid shards")
	}

	shards[0][0] ^= 0xff
	ok, _ = enc.Verify(shards)
	if ok {
		t.Error("Verify returned true for corrupted shards")
	}
}
