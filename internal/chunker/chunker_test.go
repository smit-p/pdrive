package chunker

import (
	"bytes"
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

// ── SizeForSeq ──────────────────────────────────────────────────────────────

func TestSizeForSeq_EmptyTiers(t *testing.T) {
	s := &ChunkSchedule{Tiers: nil}
	if got := s.SizeForSeq(0); got != DefaultChunkSize {
		t.Errorf("empty tiers: got %d, want %d", got, DefaultChunkSize)
	}
}

func TestSizeForSeq_SingleUnlimitedTier(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: 8 * 1024 * 1024}}}
	for _, seq := range []int{0, 1, 100} {
		if got := s.SizeForSeq(seq); got != 8*1024*1024 {
			t.Errorf("seq=%d: got %d, want 8MB", seq, got)
		}
	}
}

func TestSizeForSeq_MultiTier(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 2, Size: 4 * 1024 * 1024},  // seq 0,1
		{Count: 3, Size: 16 * 1024 * 1024}, // seq 2,3,4
		{Count: 0, Size: 32 * 1024 * 1024}, // seq 5+
	}}
	tests := []struct{ seq, want int }{
		{0, 4 * 1024 * 1024},
		{1, 4 * 1024 * 1024},
		{2, 16 * 1024 * 1024},
		{4, 16 * 1024 * 1024},
		{5, 32 * 1024 * 1024},
		{100, 32 * 1024 * 1024},
	}
	for _, tt := range tests {
		if got := s.SizeForSeq(tt.seq); got != tt.want {
			t.Errorf("seq=%d: got %d, want %d", tt.seq, got, tt.want)
		}
	}
}

func TestSizeForSeq_AllBounded(t *testing.T) {
	// All tiers have Count > 0, seq exceeds total → last tier size.
	s := &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 1, Size: 1000},
		{Count: 1, Size: 2000},
	}}
	if got := s.SizeForSeq(5); got != 2000 {
		t.Errorf("beyond all tiers: got %d, want 2000", got)
	}
}

// ── ScheduleForFile ─────────────────────────────────────────────────────────

func TestScheduleForFile(t *testing.T) {
	s := ScheduleForFile(100 * 1024 * 1024)
	if len(s.Tiers) != 1 {
		t.Fatalf("expected 1 tier, got %d", len(s.Tiers))
	}
	if s.Tiers[0].Size != DefaultChunkSize {
		t.Errorf("expected DefaultChunkSize, got %d", s.Tiers[0].Size)
	}
	if s.Tiers[0].Count != 0 {
		t.Errorf("expected unlimited count, got %d", s.Tiers[0].Count)
	}
}

// ── NewVariableChunkReader ──────────────────────────────────────────────────

func TestVariableChunkReader_Basic(t *testing.T) {
	data := make([]byte, 10*1024*1024) // 10 MB
	rand.Read(data)

	schedule := &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 0, Size: 4 * 1024 * 1024}, // 4 MB chunks
	}}

	vr := NewVariableChunkReader(bytes.NewReader(data), schedule)
	var chunks []*Chunk
	for {
		c, err := vr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			break
		}
		chunks = append(chunks, c)
	}

	// 10 MB / 4 MB = 3 chunks (4+4+2)
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	if chunks[0].Size != 4*1024*1024 {
		t.Errorf("chunk 0 size = %d", chunks[0].Size)
	}
	if chunks[2].Size != 2*1024*1024 {
		t.Errorf("last chunk size = %d, want 2MB", chunks[2].Size)
	}
	for i, c := range chunks {
		if c.Sequence != i {
			t.Errorf("chunk %d sequence = %d", i, c.Sequence)
		}
		hash := sha256.Sum256(c.Data)
		if hex.EncodeToString(hash[:]) != c.SHA256 {
			t.Errorf("chunk %d SHA256 mismatch", i)
		}
	}
}

func TestVariableChunkReader_NilSchedule(t *testing.T) {
	data := []byte("hello")
	vr := NewVariableChunkReader(bytes.NewReader(data), nil)
	c, err := vr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("expected a chunk")
	}
	if !bytes.Equal(c.Data, data) {
		t.Errorf("data mismatch")
	}
	// Next call should return nil (EOF).
	c2, err := vr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if c2 != nil {
		t.Error("expected nil after EOF")
	}
}

func TestVariableChunkReader_EmptyReader(t *testing.T) {
	vr := NewVariableChunkReader(bytes.NewReader(nil), nil)
	c, err := vr.Next()
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Error("expected nil for empty reader")
	}
}

func TestVariableChunkReader_MultiTier(t *testing.T) {
	// 12 MB total, schedule: 2 chunks of 4MB then unlimited 8MB
	data := make([]byte, 12*1024*1024)
	rand.Read(data)
	schedule := &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 2, Size: 4 * 1024 * 1024},
		{Count: 0, Size: 8 * 1024 * 1024},
	}}
	vr := NewVariableChunkReader(bytes.NewReader(data), schedule)
	var sizes []int
	for {
		c, err := vr.Next()
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			break
		}
		sizes = append(sizes, c.Size)
	}
	// seq 0: 4MB, seq 1: 4MB, seq 2: 4MB (remaining, < 8MB tier size)
	if len(sizes) != 3 {
		t.Fatalf("expected 3 chunks, got %d: %v", len(sizes), sizes)
	}
	if sizes[0] != 4*1024*1024 || sizes[1] != 4*1024*1024 {
		t.Errorf("first two chunks should be 4MB: %v", sizes)
	}
}

func TestVariableChunkReader_ReadError(t *testing.T) {
	er := &errReader{err: fmt.Errorf("disk error")}
	vr := NewVariableChunkReader(er, nil)
	_, err := vr.Next()
	if err == nil {
		t.Fatal("expected error from reader")
	}
}

// ── Additional coverage tests ───────────────────────────────────────────────

func TestMaxSize_EmptyTiers(t *testing.T) {
	s := &ChunkSchedule{Tiers: nil}
	if got := s.MaxSize(); got != DefaultChunkSize {
		t.Errorf("MaxSize empty = %d, want %d", got, DefaultChunkSize)
	}
}

func TestMaxSize_ZeroSizeTier(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: 0}}}
	if got := s.MaxSize(); got != DefaultChunkSize {
		t.Errorf("MaxSize zero = %d, want %d", got, DefaultChunkSize)
	}
}

func TestEstimateChunks_ZeroFileSize(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: DefaultChunkSize}}}
	if got := s.EstimateChunks(0); got != 0 {
		t.Errorf("EstimateChunks(0) = %d, want 0", got)
	}
}

func TestEstimateChunks_NegativeFileSize(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: DefaultChunkSize}}}
	if got := s.EstimateChunks(-1); got != 0 {
		t.Errorf("EstimateChunks(-1) = %d, want 0", got)
	}
}

func TestEstimateChunks_SingleUnlimitedTier(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: 10}}}
	// 25 bytes / 10 = 3 chunks
	if got := s.EstimateChunks(25); got != 3 {
		t.Errorf("EstimateChunks(25) = %d, want 3", got)
	}
}

func TestEstimateChunks_MultiTierExact(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 2, Size: 10},
		{Count: 0, Size: 20},
	}}
	// 2 chunks of 10 = 20 bytes, remaining 30 bytes → 2 chunks of 20 = total 4
	if got := s.EstimateChunks(50); got != 4 {
		t.Errorf("EstimateChunks(50) = %d, want 4", got)
	}
}

func TestEstimateChunks_FitsInFirstTier(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 5, Size: 10},
		{Count: 0, Size: 20},
	}}
	// 25 bytes / 10 = 3 chunks (all within first tier)
	if got := s.EstimateChunks(25); got != 3 {
		t.Errorf("EstimateChunks(25) = %d, want 3", got)
	}
}

func TestEstimateChunks_AllBoundedTiersExhausted(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 2, Size: 10},
		{Count: 2, Size: 20},
	}}
	// Tier1: 2*10=20, Tier2: 2*20=40, total covered = 60
	// File = 100, remaining = 40, last tier size = 20 → 2 more chunks
	// Total: 2+2+2 = 6
	if got := s.EstimateChunks(100); got != 6 {
		t.Errorf("EstimateChunks(100) = %d, want 6", got)
	}
}

func TestEstimateChunks_DataExhaustedDuringBoundedTier(t *testing.T) {
	s := &ChunkSchedule{Tiers: []ChunkTier{
		{Count: 2, Size: 10},
		{Count: 5, Size: 20},
	}}
	// Tier1: 2*10=20 bytes (2 chunks), remaining = 25
	// Tier2: 25/20 = 2 chunks (ceil)
	// Total: 4
	if got := s.EstimateChunks(45); got != 4 {
		t.Errorf("EstimateChunks(45) = %d, want 4", got)
	}
}

func TestPlanChunks_AllTinyRemotes(t *testing.T) {
	// Remotes too small to hold even encryption overhead
	spaces := []int64{10, 10, 10}
	s := PlanChunks(1000, spaces)
	if len(s.Tiers) == 0 {
		t.Fatal("expected at least 1 tier")
	}
}

func TestVariableChunkReader_DoneAfterEOF(t *testing.T) {
	data := []byte("short")
	schedule := &ChunkSchedule{Tiers: []ChunkTier{{Count: 0, Size: 100}}}
	vr := NewVariableChunkReader(bytes.NewReader(data), schedule)

	chunk, err := vr.Next()
	if err != nil || chunk == nil {
		t.Fatalf("first Next: err=%v, chunk=%v", err, chunk)
	}
	// Second call should return nil
	chunk, err = vr.Next()
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	if chunk != nil {
		t.Error("expected nil chunk after EOF")
	}
}

func TestChunkReader_DoneAfterEOF(t *testing.T) {
	data := []byte("short")
	cr := NewChunkReader(bytes.NewReader(data), 100)
	chunk, err := cr.Next()
	if err != nil || chunk == nil {
		t.Fatalf("first Next: err=%v, chunk=%v", err, chunk)
	}
	chunk, err = cr.Next()
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	if chunk != nil {
		t.Error("expected nil chunk after EOF")
	}
}
