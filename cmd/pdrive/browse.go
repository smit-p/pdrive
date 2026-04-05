package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// ── styles ──────────────────────────────────────────────────────────────────

var (
	styleDir     = lipgloss.NewStyle().Foreground(lipgloss.Color("12")).Bold(true)                                 // blue
	styleFile    = lipgloss.NewStyle()                                                                             // default
	styleCursor  = lipgloss.NewStyle().Foreground(lipgloss.Color("0")).Background(lipgloss.Color("14")).Bold(true) // black on cyan
	stylePath    = lipgloss.NewStyle().Foreground(lipgloss.Color("11")).Bold(true)                                 // yellow
	styleHelp    = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))                                             // gray
	styleSize    = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))                                             // gray
	styleState   = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))                                            // green
	styleErr     = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true)                                  // red
	styleEmpty   = lipgloss.NewStyle().Foreground(lipgloss.Color("8")).Italic(true)
	styleInfoKey = lipgloss.NewStyle().Foreground(lipgloss.Color("14")).Bold(true)
	styleInfoVal = lipgloss.NewStyle()
)

// ── model ───────────────────────────────────────────────────────────────────

type browseItem struct {
	Name       string
	IsDir      bool
	SizeBytes  int64
	ModifiedAt int64
	State      string // local, cloud, syncing …
}

type browseModel struct {
	addr      string
	configDir string

	path    string       // current directory path
	items   []browseItem // items in current listing
	parents []string     // breadcrumb trail

	cursor int // highlighted index in items
	offset int // scroll offset for viewport

	width  int
	height int

	err     error
	loading bool

	// info panel (right side or bottom) for selected file
	fileInfo *cliFileInfo
	infoErr  error

	// confirmation
	confirmMsg    string
	confirmTarget int // index of item to act on
	confirmAction string

	quitting bool
}

// ── messages ────────────────────────────────────────────────────────────────

type lsResultMsg struct {
	path    string
	items   []browseItem
	parents []string
	err     error
}

type fileInfoMsg struct {
	info *cliFileInfo
	err  error
}

type actionDoneMsg struct {
	msg string
	err error
}

// ── key bindings ────────────────────────────────────────────────────────────

type browseKeyMap struct {
	Up       key.Binding
	Down     key.Binding
	Enter    key.Binding
	Back     key.Binding
	Quit     key.Binding
	Root     key.Binding
	Download key.Binding
	Pin      key.Binding
	Unpin    key.Binding
	Delete   key.Binding
	Info     key.Binding
}

var browseKeys = browseKeyMap{
	Up:       key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "up")),
	Down:     key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "down")),
	Enter:    key.NewBinding(key.WithKeys("enter", "l", "right"), key.WithHelp("enter/l/→", "open")),
	Back:     key.NewBinding(key.WithKeys("backspace", "h", "left"), key.WithHelp("bksp/h/←", "back")),
	Quit:     key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),
	Root:     key.NewBinding(key.WithKeys("~"), key.WithHelp("~", "root")),
	Download: key.NewBinding(key.WithKeys("d"), key.WithHelp("d", "download")),
	Pin:      key.NewBinding(key.WithKeys("p"), key.WithHelp("p", "pin")),
	Unpin:    key.NewBinding(key.WithKeys("u"), key.WithHelp("u", "unpin")),
	Delete:   key.NewBinding(key.WithKeys("x"), key.WithHelp("x", "delete")),
	Info:     key.NewBinding(key.WithKeys("i"), key.WithHelp("i", "info")),
}

// ── init ────────────────────────────────────────────────────────────────────

func newBrowseModel(addr, configDir string) browseModel {
	return browseModel{
		addr:      addr,
		configDir: configDir,
		path:      "/",
		loading:   true,
	}
}

func (m browseModel) Init() tea.Cmd {
	return m.fetchDir("/")
}

// ── update ──────────────────────────────────────────────────────────────────

func (m browseModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case lsResultMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.err = nil
		m.path = msg.path
		m.items = msg.items
		m.parents = msg.parents
		m.cursor = 0
		m.offset = 0
		m.fileInfo = nil
		m.infoErr = nil
		m.confirmMsg = ""
		return m, nil

	case fileInfoMsg:
		m.infoErr = msg.err
		m.fileInfo = msg.info
		return m, nil

	case actionDoneMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		// Refresh the directory after an action
		m.loading = true
		return m, m.fetchDir(m.path)

	case tea.KeyMsg:
		// Handle confirmation mode
		if m.confirmMsg != "" {
			switch msg.String() {
			case "y", "Y":
				idx := m.confirmTarget
				action := m.confirmAction
				m.confirmMsg = ""
				return m, m.doAction(action, idx)
			default:
				m.confirmMsg = ""
				return m, nil
			}
		}

		switch {
		case key.Matches(msg, browseKeys.Quit):
			m.quitting = true
			return m, tea.Quit

		case key.Matches(msg, browseKeys.Up):
			if m.cursor > 0 {
				m.cursor--
				m.fileInfo = nil
				m.infoErr = nil
			}
			m.ensureVisible()
			return m, nil

		case key.Matches(msg, browseKeys.Down):
			if m.cursor < len(m.items)-1 {
				m.cursor++
				m.fileInfo = nil
				m.infoErr = nil
			}
			m.ensureVisible()
			return m, nil

		case key.Matches(msg, browseKeys.Enter):
			if len(m.items) == 0 {
				return m, nil
			}
			item := m.items[m.cursor]
			if item.IsDir {
				m.loading = true
				target := path.Join(m.path, item.Name)
				return m, m.fetchDir(target)
			}
			// File: show info
			return m, m.fetchInfo(m.cursor)

		case key.Matches(msg, browseKeys.Back):
			if m.path == "/" {
				return m, nil
			}
			m.loading = true
			return m, m.fetchDir(path.Dir(m.path))

		case key.Matches(msg, browseKeys.Root):
			if m.path == "/" {
				return m, nil
			}
			m.loading = true
			return m, m.fetchDir("/")

		case key.Matches(msg, browseKeys.Info):
			if len(m.items) == 0 || m.items[m.cursor].IsDir {
				return m, nil
			}
			return m, m.fetchInfo(m.cursor)

		case key.Matches(msg, browseKeys.Pin):
			if len(m.items) == 0 || m.items[m.cursor].IsDir {
				return m, nil
			}
			m.confirmMsg = fmt.Sprintf("Pin %q? (y/n)", m.items[m.cursor].Name)
			m.confirmTarget = m.cursor
			m.confirmAction = "pin"
			return m, nil

		case key.Matches(msg, browseKeys.Unpin):
			if len(m.items) == 0 || m.items[m.cursor].IsDir {
				return m, nil
			}
			m.confirmMsg = fmt.Sprintf("Unpin %q? (y/n)", m.items[m.cursor].Name)
			m.confirmTarget = m.cursor
			m.confirmAction = "unpin"
			return m, nil

		case key.Matches(msg, browseKeys.Delete):
			if len(m.items) == 0 {
				return m, nil
			}
			m.confirmMsg = fmt.Sprintf("Delete %q? (y/n)", m.items[m.cursor].Name)
			m.confirmTarget = m.cursor
			m.confirmAction = "delete"
			return m, nil

		case key.Matches(msg, browseKeys.Download):
			if len(m.items) == 0 || m.items[m.cursor].IsDir {
				return m, nil
			}
			m.confirmMsg = fmt.Sprintf("Pin (download) %q? (y/n)", m.items[m.cursor].Name)
			m.confirmTarget = m.cursor
			m.confirmAction = "pin"
			return m, nil
		}
	}
	return m, nil
}

// ── view ────────────────────────────────────────────────────────────────────

func (m browseModel) View() string {
	if m.quitting {
		return ""
	}

	var b strings.Builder

	// Title bar: path
	titleBar := stylePath.Render("  " + m.path)
	b.WriteString(titleBar)
	b.WriteString("\n")

	if m.loading {
		b.WriteString("\n  Loading…\n")
		return b.String()
	}

	if m.err != nil {
		b.WriteString("\n  " + styleErr.Render("Error: "+m.err.Error()) + "\n")
		b.WriteString("\n" + styleHelp.Render("  Press backspace to go back, q to quit") + "\n")
		return b.String()
	}

	if len(m.items) == 0 {
		b.WriteString("\n  " + styleEmpty.Render("(empty directory)") + "\n")
		b.WriteString("\n" + styleHelp.Render("  ←/bksp back  q quit") + "\n")
		return b.String()
	}

	// Calculate available height for the list
	// Title(1) + blank(1) + list + blank(1) + info/help(variable) + help bar(1)
	listHeight := m.height - 6 // reserve space for chrome
	if m.fileInfo != nil || m.infoErr != nil {
		listHeight = m.height - 14 // make room for info panel
	}
	if listHeight < 3 {
		listHeight = 3
	}

	b.WriteString("\n")

	// Render visible items
	for i := m.offset; i < len(m.items) && i < m.offset+listHeight; i++ {
		item := m.items[i]
		isCursor := i == m.cursor

		// Build the line
		name := item.Name
		var meta string
		if item.IsDir {
			name = styleDir.Render(name + "/")
			meta = ""
		} else {
			size := styleSize.Render(fmtSize(item.SizeBytes))
			age := styleSize.Render(fmtAge(item.ModifiedAt))
			state := styleState.Render(stateLabel(item.State))
			meta = fmt.Sprintf("  %s  %s  %s", size, age, state)
			name = styleFile.Render(name)
		}

		line := fmt.Sprintf("  %s%s", name, meta)

		if isCursor {
			// Render the cursor line with highlight — strip existing styles for highlight
			plainName := item.Name
			if item.IsDir {
				plainName += "/"
			}
			if item.IsDir {
				line = fmt.Sprintf("  %s%s", plainName, meta)
			} else {
				plainMeta := fmt.Sprintf("  %s  %s  %s", fmtSize(item.SizeBytes), fmtAge(item.ModifiedAt), stateLabel(item.State))
				line = fmt.Sprintf("  %s%s", plainName, plainMeta)
			}
			// Pad to width for full-row highlight
			if m.width > 0 && len(line) < m.width {
				line += strings.Repeat(" ", m.width-len(line))
			}
			line = styleCursor.Render(line)
		}

		b.WriteString(line)
		b.WriteString("\n")
	}

	// Scroll indicator
	if len(m.items) > listHeight {
		pct := 0
		if len(m.items)-listHeight > 0 {
			pct = m.offset * 100 / (len(m.items) - listHeight)
		}
		b.WriteString(styleHelp.Render(fmt.Sprintf("  ── %d/%d (%d%%) ──", m.cursor+1, len(m.items), pct)))
		b.WriteString("\n")
	}

	// File info panel
	if m.fileInfo != nil {
		b.WriteString("\n")
		b.WriteString(renderInfoPanel(m.fileInfo))
		b.WriteString("\n")
	} else if m.infoErr != nil {
		b.WriteString("\n  " + styleErr.Render("Info: "+m.infoErr.Error()) + "\n")
	}

	// Confirmation bar
	if m.confirmMsg != "" {
		b.WriteString("\n  " + styleErr.Render(m.confirmMsg) + "\n")
	}

	// Help bar
	b.WriteString("\n")
	if m.path != "/" {
		b.WriteString(styleHelp.Render("  ↑↓/jk navigate  enter/→ open  ←/bksp back  ~ root  i info  d get  p pin  u unpin  x delete  q quit"))
	} else {
		b.WriteString(styleHelp.Render("  ↑↓/jk navigate  enter/→ open  i info  d get  p pin  u unpin  x delete  q quit"))
	}
	b.WriteString("\n")

	return b.String()
}

func renderInfoPanel(info *cliFileInfo) string {
	var b strings.Builder
	b.WriteString("  " + styleInfoKey.Render("Path:  ") + styleInfoVal.Render(info.Path) + "\n")
	b.WriteString("  " + styleInfoKey.Render("Size:  ") + styleInfoVal.Render(fmt.Sprintf("%s (%d bytes)", fmtSize(info.SizeBytes), info.SizeBytes)) + "\n")
	b.WriteString("  " + styleInfoKey.Render("State: ") + styleInfoVal.Render(info.UploadState) + "\n")
	if len(info.Chunks) > 0 {
		providers := map[string]bool{}
		for _, c := range info.Chunks {
			for _, p := range c.Providers {
				providers[p] = true
			}
		}
		var names []string
		for p := range providers {
			names = append(names, p)
		}
		b.WriteString("  " + styleInfoKey.Render("Chunks:") + styleInfoVal.Render(fmt.Sprintf(" %d across %s", len(info.Chunks), strings.Join(names, ", "))) + "\n")
	}
	return b.String()
}

// ── commands ────────────────────────────────────────────────────────────────

func (m browseModel) fetchDir(dirPath string) tea.Cmd {
	addr := m.addr
	currentPath := m.path
	currentParents := m.parents
	return func() tea.Msg {
		body, err := daemonGet(addr, "/api/ls", url.Values{"path": {dirPath}})
		if err != nil {
			return lsResultMsg{err: err}
		}
		var resp cliLsResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return lsResultMsg{err: fmt.Errorf("invalid response: %w", err)}
		}

		var items []browseItem
		for _, d := range resp.Dirs {
			items = append(items, browseItem{Name: d, IsDir: true})
		}
		for _, f := range resp.Files {
			items = append(items, browseItem{
				Name:       f.Name,
				IsDir:      false,
				SizeBytes:  f.Size,
				ModifiedAt: f.ModifiedAt,
				State:      f.LocalState,
			})
		}

		// Build parents breadcrumb
		parents := currentParents
		if strings.HasPrefix(resp.Path, currentPath+"/") {
			parents = append(append([]string{}, parents...), currentPath)
		} else if len(parents) > 0 {
			for len(parents) > 0 && !strings.HasPrefix(resp.Path, parents[len(parents)-1]) {
				parents = parents[:len(parents)-1]
			}
		}
		if resp.Path == "/" {
			parents = nil
		}

		return lsResultMsg{
			path:    resp.Path,
			items:   items,
			parents: parents,
		}
	}
}

func (m browseModel) fetchInfo(idx int) tea.Cmd {
	addr := m.addr
	p := path.Join(m.path, m.items[idx].Name)
	return func() tea.Msg {
		body, err := daemonGet(addr, "/api/info", url.Values{"path": {p}})
		if err != nil {
			return fileInfoMsg{err: err}
		}
		var info cliFileInfo
		if err := json.Unmarshal(body, &info); err != nil {
			return fileInfoMsg{err: fmt.Errorf("invalid response: %w", err)}
		}
		return fileInfoMsg{info: &info}
	}
}

func (m browseModel) doAction(action string, idx int) tea.Cmd {
	addr := m.addr
	p := path.Join(m.path, m.items[idx].Name)
	return func() tea.Msg {
		var apiPath string
		switch action {
		case "pin":
			apiPath = "/api/pin"
		case "unpin":
			apiPath = "/api/unpin"
		case "delete":
			apiPath = "/api/delete"
		default:
			return actionDoneMsg{err: fmt.Errorf("unknown action: %s", action)}
		}
		apiURL := daemonURL(addr, apiPath, url.Values{"path": {p}})
		resp, err := http.Post(apiURL, "", nil)
		if err != nil {
			return actionDoneMsg{err: fmt.Errorf("cannot reach daemon: %w", err)}
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return actionDoneMsg{err: fmt.Errorf("%s", strings.TrimSpace(string(body)))}
		}
		return actionDoneMsg{msg: action + " " + p}
	}
}

// ensureVisible adjusts the scroll offset so the cursor is within the viewport.
func (m *browseModel) ensureVisible() {
	listHeight := m.height - 6
	if m.fileInfo != nil || m.infoErr != nil {
		listHeight = m.height - 14
	}
	if listHeight < 3 {
		listHeight = 3
	}
	if m.cursor < m.offset {
		m.offset = m.cursor
	}
	if m.cursor >= m.offset+listHeight {
		m.offset = m.cursor - listHeight + 1
	}
}

// ── entry point ─────────────────────────────────────────────────────────────

func runBrowse(addr, configDir string) {
	m := newBrowseModel(addr, configDir)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
