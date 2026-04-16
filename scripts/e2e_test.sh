#!/usr/bin/env bash
# e2e_test.sh — End-to-end tests against the live pdrive daemon.
# Uses real cloud providers (gdrive, gdrive2). Runs destructive operations
# under a test-only path (/e2e-test/) that is cleaned up on exit.
#
# Usage: ./scripts/e2e_test.sh
# Requirements: pdrive daemon running at 127.0.0.1:8765
set -euo pipefail

BASE_URL="http://127.0.0.1:8765"
TEST_DIR="/e2e-test"
PASS=0
FAIL=0
ERRORS=()

# ── helpers ────────────────────────────────────────────────────────────────

RED='\033[0;31m'
GRN='\033[0;32m'
YLW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "  $*"; }
pass() { PASS=$((PASS+1)); echo -e "  ${GRN}PASS${NC} $*"; }
fail() { FAIL=$((FAIL+1)); ERRORS+=("$*"); echo -e "  ${RED}FAIL${NC} $*"; }
banner() { echo -e "\n${YLW}=== $* ===${NC}"; }

api_get()  { curl -sf "$BASE_URL$1"; }
api_post() { curl -sf -X POST "$BASE_URL$1"; }

# Upload a file via multipart POST. Args: local_path remote_dir
api_upload() {
    local lpath="$1" rdir="$2"
    curl -sf -X POST "$BASE_URL/api/upload" \
        -F "file=@$lpath" \
        -F "dir=$rdir"
}

wait_upload_done() {
    # Poll /api/uploads until the path disappears (upload complete) or timeout.
    local vpath="$1"
    local deadline=$((SECONDS + 120))
    while (( SECONDS < deadline )); do
        local left
        left=$(api_get "/api/uploads" | python3 -c "
import sys, json
ups = json.load(sys.stdin)
print(sum(1 for u in ups if u.get('VirtualPath','') == '$vpath' and not u.get('Failed', False)))
" 2>/dev/null || echo "0")
        if [[ "$left" == "0" ]]; then return 0; fi
        sleep 2
    done
    return 1
}

sha256_of() { shasum -a 256 "$1" | awk '{print $1}'; }

cleanup() {
    banner "Cleanup"
    api_post "/api/delete?path=$TEST_DIR" >/dev/null 2>&1 || true
    rm -rf "$TMP_DIR" 2>/dev/null || true
    log "Cleaned $TEST_DIR and temp files."
}
trap cleanup EXIT

# ── pre-flight ─────────────────────────────────────────────────────────────

banner "Pre-flight"
if ! api_get "/api/health" | python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get('status')=='ok' else 1)" 2>/dev/null; then
    echo -e "${RED}ABORT: daemon not healthy. Start pdrive first.${NC}"
    exit 1
fi
log "Daemon healthy."

# Temp dir with properly-named files so uploads have predictable names
TMP_DIR=$(mktemp -d)

# Create test directory
api_post "/api/mkdir?path=$TEST_DIR" >/dev/null
log "Created $TEST_DIR"

# ── test 1: small file upload + ls + info ─────────────────────────────────

banner "Test 1: Small file upload (< 4 MB)"
# Use a subdirectory with the desired filename so the upload name is predictable
SMALL_FILE="$TMP_DIR/small.txt"
printf '%.0spdrive-e2e-test-data\n' {1..5000} > "$SMALL_FILE"
SMALL_HASH=$(sha256_of "$SMALL_FILE")
SMALL_SIZE=$(wc -c < "$SMALL_FILE" | tr -d ' ')
SMALL_VP="$TEST_DIR/small.txt"

api_upload "$SMALL_FILE" "$TEST_DIR" >/dev/null
log "Uploaded small.txt ($SMALL_SIZE bytes)"

# Even small files use WriteFileAsync — wait for upload_state=complete
if wait_upload_done "$SMALL_VP"; then
    log "Upload confirmed complete"
else
    log "Upload still in progress? Proceeding anyway..."
fi
sleep 1

# Verify appears in ls
LS_HAS=$(api_get "/api/ls?path=$TEST_DIR" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print('yes' if any(f['name']=='small.txt' for f in d.get('files',[])) else 'no')
" 2>/dev/null)
if [[ "$LS_HAS" == "yes" ]]; then
    pass "small.txt appears in ls"
else
    fail "small.txt missing from ls (got: $LS_HAS)"
fi

# Verify info returns correct size
INFO_SIZE=$(api_get "/api/info?path=$SMALL_VP" | python3 -c "
import sys, json; print(json.load(sys.stdin).get('size_bytes', -1))" 2>/dev/null)
if [[ "$INFO_SIZE" == "$SMALL_SIZE" ]]; then
    pass "info size matches ($INFO_SIZE bytes)"
else
    fail "info size mismatch: got $INFO_SIZE, want $SMALL_SIZE"
fi

# Verify download content matches
DL_FILE="$TMP_DIR/small_dl.txt"
curl -sf "$BASE_URL/api/download?path=$SMALL_VP" -o "$DL_FILE"
DL_HASH=$(sha256_of "$DL_FILE")
if [[ "$DL_HASH" == "$SMALL_HASH" ]]; then
    pass "download hash matches original"
else
    fail "download hash mismatch: got $DL_HASH, want $SMALL_HASH"
fi

# ── test 2: large file upload (multi-chunk, > 32 MB) ──────────────────────

banner "Test 2: Large file upload (> 32 MB, multi-chunk)"
LARGE_FILE="$TMP_DIR/large.bin"
# Generate 50 MB of deterministic data
dd if=/dev/urandom of="$LARGE_FILE" bs=1M count=50 2>/dev/null
LARGE_HASH=$(sha256_of "$LARGE_FILE")
LARGE_SIZE=$(wc -c < "$LARGE_FILE" | tr -d ' ')
LARGE_VP="$TEST_DIR/large.bin"

log "Uploading large.bin ($LARGE_SIZE bytes)..."
api_upload "$LARGE_FILE" "$TEST_DIR" >/dev/null

# Wait for async upload to complete
if wait_upload_done "$LARGE_VP"; then
    pass "large file upload completed"
else
    fail "large file upload timed out after 120s"
fi

# Brief settle time
sleep 3

# Verify appears in ls
LS_LARGE=$(api_get "/api/ls?path=$TEST_DIR" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print('yes' if any(f['name']=='large.bin' for f in d.get('files',[])) else 'no')
" 2>/dev/null)
if [[ "$LS_LARGE" == "yes" ]]; then
    pass "large.bin appears in ls"
else
    fail "large.bin missing from ls"
fi

# Verify info shows chunk count (≥1 always; multiple only when file spans providers)
INFO_CHUNKS=$(api_get "/api/info?path=$LARGE_VP" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print(len(d.get('chunks', [])))" 2>/dev/null)
if [[ "$INFO_CHUNKS" -ge 1 ]] 2>/dev/null; then
    pass "large.bin has $INFO_CHUNKS chunk(s)"
else
    fail "large.bin has no chunks in metadata"
fi

# Verify download matches
DL_LARGE="$TMP_DIR/large_dl.bin"
log "Downloading large.bin..."
curl -sf "$BASE_URL/api/download?path=$LARGE_VP" -o "$DL_LARGE"
DL_LARGE_HASH=$(sha256_of "$DL_LARGE")
if [[ "$DL_LARGE_HASH" == "$LARGE_HASH" ]]; then
    pass "large file download hash matches original"
else
    fail "large file download hash mismatch: got $DL_LARGE_HASH, want $LARGE_HASH"
fi

# ── test 3: move / rename ──────────────────────────────────────────────────

banner "Test 3: Move / rename"
NEW_VP="$TEST_DIR/small-renamed.txt"
api_post "/api/mv?src=$SMALL_VP&dst=$NEW_VP" >/dev/null
sleep 2

OLD_GONE=$(api_get "/api/ls?path=$TEST_DIR" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print('yes' if any(f['name']=='small.txt' for f in d.get('files',[])) else 'no')
" 2>/dev/null)
NEW_EXISTS=$(api_get "/api/ls?path=$TEST_DIR" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print('yes' if any(f['name']=='small-renamed.txt' for f in d.get('files',[])) else 'no')
" 2>/dev/null)

if [[ "$OLD_GONE" == "no" ]]; then
    pass "old path gone after rename"
else
    fail "old path still present after rename"
fi
if [[ "$NEW_EXISTS" == "yes" ]]; then
    pass "new path exists after rename"
else
    fail "new path missing after rename"
fi

# Verify content still correct after rename
DL_RENAMED="$TMP_DIR/renamed.txt"
curl -sf "$BASE_URL/api/download?path=$NEW_VP" -o "$DL_RENAMED"
DL_RENAMED_HASH=$(sha256_of "$DL_RENAMED")
if [[ "$DL_RENAMED_HASH" == "$SMALL_HASH" ]]; then
    pass "renamed file content intact"
else
    fail "renamed file content mismatch"
fi

# ── test 4: delete ─────────────────────────────────────────────────────────

banner "Test 4: Delete file"
api_post "/api/delete?path=$NEW_VP" >/dev/null
sleep 2

DELETED=$(api_get "/api/ls?path=$TEST_DIR" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print('yes' if any(f['name']=='small-renamed.txt' for f in d.get('files',[])) else 'no')
" 2>/dev/null)
if [[ "$DELETED" == "no" ]]; then
    pass "file gone from ls after delete"
else
    fail "file still in ls after delete"
fi

# ── test 5: deduplication ──────────────────────────────────────────────────

banner "Test 5: Content-hash deduplication"
# Copy large.bin to a new name — same content, different path → should dedup
DUP_FILE="$TMP_DIR/large-dup.bin"
cp "$LARGE_FILE" "$DUP_FILE"
DUP_VP="$TEST_DIR/large-dup.bin"
log "Uploading duplicate large file (same content, new name)..."

DEDUP_BEFORE=$(api_get "/api/metrics" | python3 -c "
import sys, json; print(json.load(sys.stdin).get('dedup_hits', 0))" 2>/dev/null || echo "0")

curl -sf -X POST "$BASE_URL/api/upload" \
    -F "file=@$DUP_FILE" \
    -F "dir=$TEST_DIR" >/dev/null

# Dedup is synchronous (hash match found before any async goroutine starts)
sleep 3
DEDUP_AFTER=$(api_get "/api/metrics" | python3 -c "
import sys, json; print(json.load(sys.stdin).get('dedup_hits', 0))" 2>/dev/null || echo "0")
if [[ "$DEDUP_AFTER" -gt "$DEDUP_BEFORE" ]] 2>/dev/null; then
    pass "dedup hit recorded (before=$DEDUP_BEFORE after=$DEDUP_AFTER)"
else
    fail "dedup not detected (before=$DEDUP_BEFORE after=$DEDUP_AFTER)"
fi

# ── test 6: verify integrity ───────────────────────────────────────────────

banner "Test 6: Chunk integrity verification"
VERIFY_RESULT=$(api_get "/api/verify?path=$LARGE_VP" 2>/dev/null)
VERIFY_OK=$(echo "$VERIFY_RESULT" | python3 -c "
import sys, json; d = json.load(sys.stdin)
print('yes' if d.get('ok') == True or d.get('status') == 'ok' else 'no')" 2>/dev/null)
if [[ "$VERIFY_OK" == "yes" ]]; then
    pass "verify: all chunks intact"
else
    fail "verify: reported corruption or error ($VERIFY_RESULT)"
fi

# ── test 7: du ─────────────────────────────────────────────────────────────

banner "Test 7: du (disk usage)"
DU=$(api_get "/api/du?path=$TEST_DIR" 2>/dev/null)
DU_COUNT=$(echo "$DU" | python3 -c "import sys,json; print(json.load(sys.stdin).get('file_count', 0))" 2>/dev/null || echo 0)
DU_BYTES=$(echo "$DU" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_bytes', 0))" 2>/dev/null || echo 0)
if [[ "$DU_COUNT" -ge 1 ]] 2>/dev/null; then
    pass "du reports $DU_COUNT file(s), $DU_BYTES bytes"
else
    fail "du returned unexpected count: $DU_COUNT"
fi

# ── test 8: find ───────────────────────────────────────────────────────────

banner "Test 8: find (glob search)"
FIND_RESULT=$(api_get "/api/find?path=$TEST_DIR&pattern=*.bin" 2>/dev/null || echo '[]')
FIND_COUNT=$(echo "$FIND_RESULT" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
if [[ "$FIND_COUNT" -ge 1 ]] 2>/dev/null; then
    pass "find returned $FIND_COUNT result(s) for *.bin"
else
    fail "find returned 0 results for *.bin (raw: $FIND_RESULT)"
fi

# ── test 9: tree ─────────────────────────────────────────────────────────

banner "Test 9: tree (recursive listing)"
TREE_RESULT=$(api_get "/api/tree?path=$TEST_DIR" 2>/dev/null || echo '[]')
TREE_COUNT=$(echo "$TREE_RESULT" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo 0)
if [[ "$TREE_COUNT" -ge 1 ]] 2>/dev/null; then
    pass "tree returned $TREE_COUNT entry(ies) under $TEST_DIR"
else
    fail "tree returned 0 entries (raw: $TREE_RESULT)"
fi

# ── test 10: HEAD download ─────────────────────────────────────────────────

banner "Test 10: HEAD download (headers only)"
HEAD_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" -I "$BASE_URL/api/download?path=$LARGE_VP" 2>/dev/null)
if [[ "$HEAD_STATUS" == "200" ]]; then
    pass "HEAD /api/download returns 200"
else
    fail "HEAD /api/download returned $HEAD_STATUS"
fi

# ── summary ────────────────────────────────────────────────────────────────

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  Results: ${GRN}$PASS passed${NC}, ${RED}$FAIL failed${NC}"
if (( ${#ERRORS[@]} > 0 )); then
    echo ""
    echo "  Failures:"
    for e in "${ERRORS[@]}"; do
        echo -e "    ${RED}✗${NC} $e"
    done
fi
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

(( FAIL == 0 ))
