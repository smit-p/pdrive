package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// ---------------------------------------------------------------------------
// newBrowseModel
// ---------------------------------------------------------------------------

func TestNewBrowseModel(t *testing.T) {
	m := newBrowseModel("localhost:8765", "/tmp/config")
	if m.addr != "localhost:8765" {
		t.Errorf("addr = %q", m.addr)
	}
	if m.configDir != "/tmp/config" {
		t.Errorf("configDir = %q", m.configDir)
	}
	if m.path != "/" {
		t.Errorf("path = %q, want /", m.path)
	}
	if !m.loading {
		t.Error("should start loading")
	}
	if m.quitting {
		t.Error("should not start quitting")
	}
}

func TestBrowseModel_Init(t *testing.T) {
	m := newBrowseModel("localhost:8765", "/tmp/config")
	cmd := m.Init()
	if cmd == nil {
		t.Fatal("Init() returned nil cmd")
	}
}

// ---------------------------------------------------------------------------
// Update: WindowSizeMsg
// ---------------------------------------------------------------------------

func TestBrowseModel_WindowSizeMsg(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	newM, cmd := m.Update(tea.WindowSizeMsg{Width: 120, Height: 40})
	bm := newM.(browseModel)
	if bm.width != 120 || bm.height != 40 {
		t.Errorf("size = %dx%d, want 120x40", bm.width, bm.height)
	}
	if cmd != nil {
		t.Error("WindowSizeMsg should not return a command")
	}
}

// ---------------------------------------------------------------------------
// Update: lsResultMsg (success)
// ---------------------------------------------------------------------------

func TestBrowseModel_LsResultMsg(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	m.loading = true

	msg := lsResultMsg{
		path: "/docs",
		items: []browseItem{
			{Name: "notes", IsDir: true},
			{Name: "readme.md", IsDir: false, SizeBytes: 512, State: "local"},
		},
		parents: []string{"/"},
	}

	newM, _ := m.Update(msg)
	bm := newM.(browseModel)

	if bm.loading {
		t.Error("loading should be false after lsResultMsg")
	}
	if bm.path != "/docs" {
		t.Errorf("path = %q, want /docs", bm.path)
	}
	if len(bm.items) != 2 {
		t.Fatalf("items = %d, want 2", len(bm.items))
	}
	if bm.items[0].Name != "notes" || !bm.items[0].IsDir {
		t.Errorf("item[0] = %+v", bm.items[0])
	}
	if bm.cursor != 0 {
		t.Errorf("cursor = %d, want 0", bm.cursor)
	}
	if bm.fileInfo != nil {
		t.Error("fileInfo should be cleared")
	}
}

// ---------------------------------------------------------------------------
// Update: lsResultMsg (error)
// ---------------------------------------------------------------------------

func TestBrowseModel_LsResultMsg_Error(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	m.loading = true

	msg := lsResultMsg{err: fmt.Errorf("connection refused")}
	newM, _ := m.Update(msg)
	bm := newM.(browseModel)

	if bm.loading {
		t.Error("loading should be false")
	}
	if bm.err == nil {
		t.Error("err should be set")
	}
}

// ---------------------------------------------------------------------------
// Update: fileInfoMsg
// ---------------------------------------------------------------------------

func TestBrowseModel_FileInfoMsg(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	info := &cliFileInfo{Path: "/docs/readme.md", SizeBytes: 1024, UploadState: "complete"}

	newM, _ := m.Update(fileInfoMsg{info: info})
	bm := newM.(browseModel)

	if bm.fileInfo == nil {
		t.Fatal("fileInfo should be set")
	}
	if bm.fileInfo.Path != "/docs/readme.md" {
		t.Errorf("fileInfo.Path = %q", bm.fileInfo.Path)
	}
}

func TestBrowseModel_FileInfoMsg_Error(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	newM, _ := m.Update(fileInfoMsg{err: fmt.Errorf("not found")})
	bm := newM.(browseModel)

	if bm.infoErr == nil {
		t.Error("infoErr should be set")
	}
}

// ---------------------------------------------------------------------------
// Update: actionDoneMsg
// ---------------------------------------------------------------------------

func TestBrowseModel_ActionDoneMsg_Success(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	m.path = "/docs"

	newM, cmd := m.Update(actionDoneMsg{msg: "delete /docs/file.txt"})
	bm := newM.(browseModel)

	if !bm.loading {
		t.Error("should be loading (refresh after action)")
	}
	if cmd == nil {
		t.Error("should return refresh command")
	}
}

func TestBrowseModel_ActionDoneMsg_Error(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	newM, _ := m.Update(actionDoneMsg{err: fmt.Errorf("permission denied")})
	bm := newM.(browseModel)

	if bm.err == nil {
		t.Error("err should be set")
	}
}

// ---------------------------------------------------------------------------
// Update: KeyMsg — navigation
// ---------------------------------------------------------------------------

func testModelWithItems() browseModel {
	m := newBrowseModel("localhost:8765", "/tmp")
	m.loading = false
	m.width = 80
	m.height = 40
	m.path = "/docs"
	m.items = []browseItem{
		{Name: "folder", IsDir: true},
		{Name: "readme.md", IsDir: false, SizeBytes: 1024, State: "local"},
		{Name: "photo.jpg", IsDir: false, SizeBytes: 2048, State: "stub"},
	}
	return m
}

func TestBrowseModel_KeyDown(t *testing.T) {
	m := testModelWithItems()
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyDown})
	bm := newM.(browseModel)
	if bm.cursor != 1 {
		t.Errorf("cursor = %d, want 1", bm.cursor)
	}
}

func TestBrowseModel_KeyUp(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 2
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyDown}) // no change at end
	bm := newM.(browseModel)
	// cursor was 2, down moves nowhere since it's at the end
	if bm.cursor != 2 {
		t.Errorf("cursor = %d, want 2 (at end)", bm.cursor)
	}

	// Now test up
	newM, _ = bm.Update(tea.KeyMsg{Type: tea.KeyUp})
	bm = newM.(browseModel)
	if bm.cursor != 1 {
		t.Errorf("cursor after up = %d, want 1", bm.cursor)
	}
}

func TestBrowseModel_KeyUp_AtTop(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 0
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyUp})
	bm := newM.(browseModel)
	if bm.cursor != 0 {
		t.Errorf("cursor = %d, want 0 (should not go below 0)", bm.cursor)
	}
}

// ---------------------------------------------------------------------------
// Update: KeyMsg — enter (open dir)
// ---------------------------------------------------------------------------

func TestBrowseModel_EnterDir(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 0 // "folder" (dir)

	newM, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	bm := newM.(browseModel)

	if !bm.loading {
		t.Error("should be loading after opening dir")
	}
	if cmd == nil {
		t.Error("should return fetchDir command")
	}
}

func TestBrowseModel_EnterFile(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1 // "readme.md" (file)

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd == nil {
		t.Error("should return fetchInfo command for file")
	}
}

func TestBrowseModel_EnterEmptyDir(t *testing.T) {
	m := testModelWithItems()
	m.items = nil

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd != nil {
		t.Error("should return nil for empty dir enter")
	}
}

// ---------------------------------------------------------------------------
// Update: KeyMsg — back and root
// ---------------------------------------------------------------------------

func TestBrowseModel_Back(t *testing.T) {
	m := testModelWithItems()
	m.path = "/docs/notes"

	newM, cmd := m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	bm := newM.(browseModel)

	if !bm.loading {
		t.Error("should be loading after back")
	}
	if cmd == nil {
		t.Error("should return fetchDir command")
	}
}

func TestBrowseModel_BackAtRoot(t *testing.T) {
	m := testModelWithItems()
	m.path = "/"

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyBackspace})
	if cmd != nil {
		t.Error("back at root should be no-op")
	}
}

func TestBrowseModel_Root(t *testing.T) {
	m := testModelWithItems()
	m.path = "/docs/deep/nested"

	newM, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'~'}})
	bm := newM.(browseModel)

	if !bm.loading {
		t.Error("should be loading after root")
	}
	if cmd == nil {
		t.Error("should return fetchDir command")
	}
	_ = bm
}

func TestBrowseModel_RootAtRoot(t *testing.T) {
	m := testModelWithItems()
	m.path = "/"

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'~'}})
	if cmd != nil {
		t.Error("root at root should be no-op")
	}
}

// ---------------------------------------------------------------------------
// Update: KeyMsg — quit
// ---------------------------------------------------------------------------

func TestBrowseModel_Quit(t *testing.T) {
	m := testModelWithItems()
	newM, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	bm := newM.(browseModel)

	if !bm.quitting {
		t.Error("should be quitting")
	}
	if cmd == nil {
		t.Error("should return tea.Quit")
	}
}

// ---------------------------------------------------------------------------
// Update: KeyMsg — info
// ---------------------------------------------------------------------------

func TestBrowseModel_Info(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1 // readme.md (file)

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'i'}})
	if cmd == nil {
		t.Error("info on file should return fetchInfo command")
	}
}

func TestBrowseModel_InfoOnDir(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 0 // folder (dir)

	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'i'}})
	if cmd != nil {
		t.Error("info on dir should be no-op")
	}
}

// ---------------------------------------------------------------------------
// Update: KeyMsg — pin/unpin/delete (confirmation flow)
// ---------------------------------------------------------------------------

func TestBrowseModel_Pin_ConfirmFlow(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1 // readme.md (file)

	// Press 'p' to start pin confirmation
	newM, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	bm := newM.(browseModel)

	if bm.confirmMsg == "" {
		t.Fatal("confirmMsg should be set")
	}
	if !strings.Contains(bm.confirmMsg, "Pin") {
		t.Errorf("confirmMsg = %q", bm.confirmMsg)
	}
	if bm.confirmAction != "pin" {
		t.Errorf("confirmAction = %q", bm.confirmAction)
	}
	if cmd != nil {
		t.Error("confirmation prompt should not emit a command")
	}

	// Press 'y' to confirm
	newM, cmd = bm.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'y'}})
	bm = newM.(browseModel)

	if bm.confirmMsg != "" {
		t.Error("confirmMsg should be cleared")
	}
	if cmd == nil {
		t.Error("confirming should emit action command")
	}
}

func TestBrowseModel_Pin_Cancel(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1

	// Press 'p' for pin
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	bm := newM.(browseModel)

	// Press 'n' to cancel
	newM, cmd := bm.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'n'}})
	bm = newM.(browseModel)

	if bm.confirmMsg != "" {
		t.Error("confirmMsg should be cleared on cancel")
	}
	if cmd != nil {
		t.Error("cancel should not emit command")
	}
}

func TestBrowseModel_Unpin(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1

	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'u'}})
	bm := newM.(browseModel)

	if !strings.Contains(bm.confirmMsg, "Unpin") {
		t.Errorf("confirmMsg = %q, want Unpin", bm.confirmMsg)
	}
	if bm.confirmAction != "unpin" {
		t.Errorf("confirmAction = %q", bm.confirmAction)
	}
}

func TestBrowseModel_Delete(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 2

	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	bm := newM.(browseModel)

	if !strings.Contains(bm.confirmMsg, "Delete") {
		t.Errorf("confirmMsg = %q, want Delete", bm.confirmMsg)
	}
	if bm.confirmAction != "delete" {
		t.Errorf("confirmAction = %q", bm.confirmAction)
	}
}

func TestBrowseModel_Download(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 1 // file

	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'d'}})
	bm := newM.(browseModel)

	if bm.confirmMsg == "" {
		t.Error("download should show confirmation")
	}
	if bm.confirmAction != "pin" {
		t.Errorf("download confirmAction = %q, want pin", bm.confirmAction)
	}
}

func TestBrowseModel_PinOnDir(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 0 // dir

	newM, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	bm := newM.(browseModel)
	if bm.confirmMsg != "" {
		t.Error("pin on dir should be no-op")
	}
	if cmd != nil {
		t.Error("pin on dir should not emit command")
	}
}

func TestBrowseModel_DeleteOnEmptyItems(t *testing.T) {
	m := testModelWithItems()
	m.items = nil

	newM, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}})
	bm := newM.(browseModel)
	if bm.confirmMsg != "" {
		t.Error("delete on empty should be no-op")
	}
	if cmd != nil {
		t.Error("delete on empty should not emit command")
	}
}

// ---------------------------------------------------------------------------
// View rendering
// ---------------------------------------------------------------------------

func TestBrowseModel_View_Quitting(t *testing.T) {
	m := testModelWithItems()
	m.quitting = true
	if v := m.View(); v != "" {
		t.Errorf("quitting view should be empty, got %q", v)
	}
}

func TestBrowseModel_View_Loading(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	m.loading = true
	v := m.View()
	if !strings.Contains(v, "Loading") {
		t.Errorf("loading view should contain 'Loading':\n%s", v)
	}
}

func TestBrowseModel_View_Error(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	m.loading = false
	m.err = fmt.Errorf("connection refused")
	v := m.View()
	if !strings.Contains(v, "Error") {
		t.Errorf("error view should contain 'Error':\n%s", v)
	}
}

func TestBrowseModel_View_EmptyDir(t *testing.T) {
	m := newBrowseModel("localhost:8765", "")
	m.loading = false
	m.items = nil
	v := m.View()
	if !strings.Contains(v, "empty directory") {
		t.Errorf("empty dir view should contain 'empty directory':\n%s", v)
	}
}

func TestBrowseModel_View_WithItems(t *testing.T) {
	m := testModelWithItems()
	v := m.View()

	// Should contain directory and file names
	if !strings.Contains(v, "folder") {
		t.Errorf("view should contain 'folder':\n%s", v)
	}
	if !strings.Contains(v, "readme.md") {
		t.Errorf("view should contain 'readme.md':\n%s", v)
	}
	// Help bar should be present
	if !strings.Contains(v, "quit") {
		t.Errorf("view should contain help bar:\n%s", v)
	}
}

func TestBrowseModel_View_Confirmation(t *testing.T) {
	m := testModelWithItems()
	m.confirmMsg = "Delete 'file.txt'? (y/n)"
	v := m.View()
	if !strings.Contains(v, "Delete") {
		t.Errorf("view should show confirmation:\n%s", v)
	}
}

func TestBrowseModel_View_FileInfo(t *testing.T) {
	m := testModelWithItems()
	m.fileInfo = &cliFileInfo{
		Path: "/docs/readme.md", SizeBytes: 1024, UploadState: "complete",
		Chunks: []cliChunkInfo{{Sequence: 0, SizeBytes: 1024, EncryptedSize: 1040, Providers: []string{"gdrive"}}},
	}
	v := m.View()
	if !strings.Contains(v, "readme.md") {
		t.Errorf("view should show file info:\n%s", v)
	}
	if !strings.Contains(v, "complete") {
		t.Errorf("view should show upload state:\n%s", v)
	}
}

func TestBrowseModel_View_InfoError(t *testing.T) {
	m := testModelWithItems()
	m.infoErr = fmt.Errorf("not found")
	v := m.View()
	if !strings.Contains(v, "not found") {
		t.Errorf("view should show info error:\n%s", v)
	}
}

func TestBrowseModel_View_HelpBarAtRoot(t *testing.T) {
	m := testModelWithItems()
	m.path = "/"
	v := m.View()
	// At root, should not show "back" instruction for ~
	if strings.Contains(v, "back") {
		t.Errorf("root view should not show back instruction:\n%s", v)
	}
}

func TestBrowseModel_View_HelpBarInSubdir(t *testing.T) {
	m := testModelWithItems()
	m.path = "/docs"
	v := m.View()
	// In subdir, should show "back" instruction
	if !strings.Contains(v, "back") {
		t.Errorf("subdir view should show back instruction:\n%s", v)
	}
}

// ---------------------------------------------------------------------------
// renderInfoPanel
// ---------------------------------------------------------------------------

func TestRenderInfoPanel(t *testing.T) {
	info := &cliFileInfo{
		Path:        "/docs/readme.md",
		SizeBytes:   1024,
		UploadState: "complete",
		Chunks: []cliChunkInfo{
			{Sequence: 0, SizeBytes: 512, EncryptedSize: 528, Providers: []string{"gdrive", "dropbox"}},
			{Sequence: 1, SizeBytes: 512, EncryptedSize: 528, Providers: []string{"gdrive"}},
		},
	}
	panel := renderInfoPanel(info)

	if !strings.Contains(panel, "/docs/readme.md") {
		t.Errorf("panel missing path:\n%s", panel)
	}
	if !strings.Contains(panel, "1024") {
		t.Errorf("panel missing size:\n%s", panel)
	}
	if !strings.Contains(panel, "complete") {
		t.Errorf("panel missing state:\n%s", panel)
	}
	if !strings.Contains(panel, "2") { // 2 chunks
		t.Errorf("panel missing chunk count:\n%s", panel)
	}
}

func TestRenderInfoPanel_NoChunks(t *testing.T) {
	info := &cliFileInfo{Path: "/test.txt", SizeBytes: 0, UploadState: "uploading"}
	panel := renderInfoPanel(info)
	if strings.Contains(panel, "Chunks") {
		t.Errorf("panel should not show chunks section:\n%s", panel)
	}
}

// ---------------------------------------------------------------------------
// ensureVisible
// ---------------------------------------------------------------------------

func TestEnsureVisible_CursorAboveOffset(t *testing.T) {
	m := testModelWithItems()
	m.offset = 5
	m.cursor = 2
	m.ensureVisible()
	if m.offset != 2 {
		t.Errorf("offset = %d, want 2 (cursor)", m.offset)
	}
}

func TestEnsureVisible_CursorBelowViewport(t *testing.T) {
	m := testModelWithItems()
	m.height = 10
	m.offset = 0
	m.cursor = 20 // way beyond viewport
	m.ensureVisible()
	if m.offset <= 0 {
		t.Errorf("offset = %d, should have scrolled down", m.offset)
	}
}

// ---------------------------------------------------------------------------
// fetchDir / fetchInfo / doAction (execute commands against mock server)
// ---------------------------------------------------------------------------

func browseMockDaemon(t *testing.T) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ls", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(cliLsResponse{
			Path: r.URL.Query().Get("path"),
			Dirs: []string{"sub"},
			Files: []cliLsFile{
				{Name: "file.txt", Path: "/file.txt", Size: 100, LocalState: "local"},
			},
		})
	})
	mux.HandleFunc("/api/info", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(cliFileInfo{
			Path: r.URL.Query().Get("path"), SizeBytes: 100, UploadState: "complete",
		})
	})
	mux.HandleFunc("/api/pin", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("/api/unpin", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("/api/delete", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return strings.TrimPrefix(srv.URL, "http://")
}

func TestBrowseModel_FetchDir(t *testing.T) {
	addr := browseMockDaemon(t)
	m := newBrowseModel(addr, t.TempDir())

	cmd := m.fetchDir("/")
	msg := cmd()
	ls, ok := msg.(lsResultMsg)
	if !ok {
		t.Fatalf("expected lsResultMsg, got %T", msg)
	}
	if ls.err != nil {
		t.Fatalf("fetchDir error: %v", ls.err)
	}
	if ls.path != "/" {
		t.Errorf("path = %q", ls.path)
	}
	if len(ls.items) != 2 {
		t.Errorf("items = %d, want 2", len(ls.items))
	}
}

func TestBrowseModel_FetchInfo(t *testing.T) {
	addr := browseMockDaemon(t)
	m := newBrowseModel(addr, t.TempDir())
	m.path = "/docs"
	m.items = []browseItem{{Name: "file.txt", IsDir: false}}

	cmd := m.fetchInfo(0)
	msg := cmd()
	info, ok := msg.(fileInfoMsg)
	if !ok {
		t.Fatalf("expected fileInfoMsg, got %T", msg)
	}
	if info.err != nil {
		t.Fatalf("fetchInfo error: %v", info.err)
	}
	if info.info.Path != "/docs/file.txt" {
		t.Errorf("path = %q", info.info.Path)
	}
}

func TestBrowseModel_DoAction_Pin(t *testing.T) {
	addr := browseMockDaemon(t)
	m := newBrowseModel(addr, t.TempDir())
	m.path = "/docs"
	m.items = []browseItem{{Name: "file.txt", IsDir: false}}

	cmd := m.doAction("pin", 0)
	msg := cmd()
	done, ok := msg.(actionDoneMsg)
	if !ok {
		t.Fatalf("expected actionDoneMsg, got %T", msg)
	}
	if done.err != nil {
		t.Errorf("doAction error: %v", done.err)
	}
}

func TestBrowseModel_DoAction_Delete(t *testing.T) {
	addr := browseMockDaemon(t)
	m := newBrowseModel(addr, t.TempDir())
	m.path = "/"
	m.items = []browseItem{{Name: "file.txt", IsDir: false}}

	cmd := m.doAction("delete", 0)
	msg := cmd()
	done := msg.(actionDoneMsg)
	if done.err != nil {
		t.Errorf("delete error: %v", done.err)
	}
}

func TestBrowseModel_DoAction_UnknownAction(t *testing.T) {
	m := newBrowseModel("localhost:1", t.TempDir())
	m.items = []browseItem{{Name: "file.txt", IsDir: false}}

	cmd := m.doAction("unknown", 0)
	msg := cmd()
	done := msg.(actionDoneMsg)
	if done.err == nil {
		t.Error("unknown action should return error")
	}
}

// ---------------------------------------------------------------------------
// Scroll indicator
// ---------------------------------------------------------------------------

func TestBrowseModel_View_ScrollIndicator(t *testing.T) {
	m := testModelWithItems()
	m.height = 10
	// Add many items to trigger scroll
	for i := 0; i < 50; i++ {
		m.items = append(m.items, browseItem{Name: fmt.Sprintf("file%d.txt", i), IsDir: false, SizeBytes: 100, State: "local"})
	}
	v := m.View()
	if !strings.Contains(v, "/") { // "1/53"
		t.Errorf("view should contain scroll indicator:\n%s", v)
	}
}

// ---------------------------------------------------------------------------
// j/k navigation (vi-style)
// ---------------------------------------------------------------------------

func TestBrowseModel_KeyJ(t *testing.T) {
	m := testModelWithItems()
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	bm := newM.(browseModel)
	if bm.cursor != 1 {
		t.Errorf("j: cursor = %d, want 1", bm.cursor)
	}
}

func TestBrowseModel_KeyK(t *testing.T) {
	m := testModelWithItems()
	m.cursor = 2
	newM, _ := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	bm := newM.(browseModel)
	if bm.cursor != 1 {
		t.Errorf("k: cursor = %d, want 1", bm.cursor)
	}
}

// ---------------------------------------------------------------------------
// key.Matches helper test for coverage of browseKeys
// ---------------------------------------------------------------------------

func TestBrowseKeyBindings(t *testing.T) {
	tests := []struct {
		binding key.Binding
		keys    []string
	}{
		{browseKeys.Up, []string{"up", "k"}},
		{browseKeys.Down, []string{"down", "j"}},
		{browseKeys.Enter, []string{"enter", "l", "right"}},
		{browseKeys.Back, []string{"backspace", "h", "left"}},
		{browseKeys.Quit, []string{"q", "ctrl+c"}},
		{browseKeys.Root, []string{"~"}},
		{browseKeys.Download, []string{"d"}},
		{browseKeys.Pin, []string{"p"}},
		{browseKeys.Unpin, []string{"u"}},
		{browseKeys.Delete, []string{"x"}},
		{browseKeys.Info, []string{"i"}},
	}
	for _, tt := range tests {
		if tt.binding.Help().Key == "" {
			t.Errorf("binding %v has no help key", tt.keys)
		}
	}
}
