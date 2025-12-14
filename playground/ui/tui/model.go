package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/suyash-sneo/rendezgo/playground/sim"
)

type tickMsg struct{}
type snapshotMsg struct{ snap sim.Snapshot }
type eventsMsg struct {
	events []sim.Event
	latest uint64
}
type execMsg struct{ err error }

type Model struct {
	eng      *sim.Engine
	snapshot sim.Snapshot
	lastSeq  uint64

	logLines []string
	vp       viewport.Model

	input     textinput.Model
	inputMode bool
	showHelp  bool

	history []string
	histIdx int

	width  int
	height int

	err error
}

func Run(eng *sim.Engine) error {
	m := initialModel(eng)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func initialModel(eng *sim.Engine) Model {
	vp := viewport.New(0, 12)
	vp.SetContent("")
	input := textinput.New()
	input.Prompt = ":"
	return Model{eng: eng, vp: vp, input: input, inputMode: false, histIdx: -1}
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(tick(), fetchSnapshot(m.eng), fetchEvents(m.eng, m.lastSeq))
}

func tick() tea.Cmd {
	return tea.Tick(300*time.Millisecond, func(time.Time) tea.Msg { return tickMsg{} })
}

func fetchSnapshot(eng *sim.Engine) tea.Cmd {
	return func() tea.Msg { return snapshotMsg{snap: eng.Snapshot()} }
}

func fetchEvents(eng *sim.Engine, seq uint64) tea.Cmd {
	return func() tea.Msg {
		evs, latest := eng.EventsSince(seq)
		return eventsMsg{events: evs, latest: latest}
	}
}

func execCommand(eng *sim.Engine, line string) tea.Cmd {
	return func() tea.Msg {
		_, err := eng.ExecCommand(context.Background(), line)
		return execMsg{err: err}
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if m.inputMode {
			switch msg.Type {
			case tea.KeyEsc:
				m.inputMode = false
				m.input.Blur()
				m.input.SetValue("")
				m.histIdx = len(m.history)
				return m, nil
			case tea.KeyEnter:
				line := strings.TrimSpace(m.input.Value())
				m.input.SetValue("")
				m.inputMode = false
				m.input.Blur()
				if line != "" {
					m.history = append(m.history, line)
					m.histIdx = len(m.history)
					return m, execCommand(m.eng, line)
				}
				return m, nil
			case tea.KeyUp:
				if len(m.history) == 0 {
					return m, nil
				}
				if m.histIdx <= 0 {
					m.histIdx = 0
				} else {
					m.histIdx--
				}
				m.input.SetValue(m.history[m.histIdx])
				m.input.CursorEnd()
				return m, nil
			case tea.KeyDown:
				if len(m.history) == 0 {
					return m, nil
				}
				if m.histIdx < len(m.history)-1 {
					m.histIdx++
				}
				m.input.SetValue(m.history[m.histIdx])
				m.input.CursorEnd()
				return m, nil
			}
			var cmd tea.Cmd
			m.input, cmd = m.input.Update(msg)
			return m, cmd
		}
		switch msg.String() {
		case ":":
			m.inputMode = true
			m.input.Focus()
			m.histIdx = len(m.history)
			return m, nil
		case "?":
			m.showHelp = !m.showHelp
			return m, nil
		case "q", "ctrl+c":
			return m, tea.Quit
		}
		var cmd tea.Cmd
		m.vp, cmd = m.vp.Update(msg)
		return m, cmd
	case tickMsg:
		return m, tea.Batch(tick(), fetchSnapshot(m.eng), fetchEvents(m.eng, m.lastSeq))
	case snapshotMsg:
		m.snapshot = msg.snap
		return m, nil
	case eventsMsg:
		m.lastSeq = msg.latest
		if len(msg.events) > 0 {
			follow := m.vp.AtBottom()
			for _, ev := range msg.events {
				m.logLines = append(m.logLines, formatEvent(ev))
			}
			m.vp.SetContent(strings.Join(m.logLines, "\n"))
			if follow {
				m.vp.GotoBottom()
			}
		}
		return m, nil
	case execMsg:
		if msg.err != nil {
			m.err = msg.err
		}
		return m, nil
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.vp.Width = msg.Width
		logHeight := msg.Height / 3
		if logHeight < 8 {
			logHeight = 8
		}
		m.vp.Height = logHeight
		m.vp.SetContent(strings.Join(m.logLines, "\n"))
		return m, nil
	}
	return m, nil
}

func (m Model) View() string {
	header := m.renderHeader()
	nodes := m.renderNodes()
	units := m.renderUnits()
	log := m.renderLog()
	help := ""
	if m.showHelp {
		help = m.renderHelp()
	}
	input := ""
	if m.inputMode {
		input = "\n" + m.input.View()
	}
	layout := lipgloss.JoinVertical(lipgloss.Left, header, lipgloss.JoinHorizontal(lipgloss.Top, nodes, units), log)
	if help != "" {
		layout = lipgloss.JoinVertical(lipgloss.Left, layout, help)
	}
	if input != "" {
		layout = layout + input
	}
	return layout
}

func (m Model) renderHeader() string {
	s := m.snapshot
	return lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("212")).
		Render(fmt.Sprintf("redis:%s mode:%s nodes:%d churn/min:%d conv:%.1f%% focus:%s topK:%d caps node:%d wl:%d min:%d shedding:%v/%d",
			s.RedisAddr, s.Mode, len(s.Nodes), s.ChurnPerMinute, s.Convergence, valueOrDash(s.Focus), s.TopK, s.Caps.MaxPerNode, s.Caps.MaxPerWorkload, s.Caps.MinPerNode, s.Caps.SheddingEnabled, s.Caps.SheddingRelease))
}

func (m Model) renderNodes() string {
	width := m.width / 3
	if width < 48 {
		width = 48
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Nodes\n")
	for _, n := range m.snapshot.Nodes {
		state := stateStyle(n.State).Render(n.State)
		wls := make([]string, 0, len(n.Workload))
		for name, counts := range n.Workload {
			wls = append(wls, fmt.Sprintf("%s %d/%d", name, counts.Owned, counts.Desired))
		}
		sort.Strings(wls)
		fmt.Fprintf(&b, "%-14s w=%.2f state=%s owned=%d desired=%d miss=%d extra=%d\n   %s\n",
			n.ID, n.Weight, state, n.Owned, n.Desired, n.Missing, n.Extra, strings.Join(wls, " "))
	}
	return panelStyle.Width(width).Render(b.String())
}

func (m Model) renderUnits() string {
	width := m.width - (m.width / 3)
	if width < 64 {
		width = 64
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Units")
	if m.snapshot.Focus != "" {
		fmt.Fprintf(&b, " (focus %s)", m.snapshot.Focus)
	}
	fmt.Fprintf(&b, "\n")
	maxRows := 120
	if m.height > 0 {
		maxRows = (m.height - m.vp.Height) - 6
		if maxRows < 12 {
			maxRows = 12
		}
	}
	for i, u := range m.snapshot.Units {
		if i >= maxRows {
			fmt.Fprintf(&b, "... (%d more)\n", len(m.snapshot.Units)-i)
			break
		}
		ranks := formatRanking(u.Ranking)
		flags := []string{}
		if u.Unowned {
			flags = append(flags, "unowned")
		}
		if u.Cooldown {
			flags = append(flags, "cooldown")
		}
		align := "misaligned"
		if u.Aligned {
			align = "aligned"
		}
		fmt.Fprintf(&b, "%s[%d] own=%s want=%s ttl=%s cd=%s %s %s\n   rank:%s\n", u.Workload, u.Unit, short(u.CurrentOwner), short(u.DesiredOwner), dur(u.LeaseTTL), dur(u.CooldownTTL), align, strings.Join(flags, ","), ranks)
	}
	return panelStyle.Width(width).Render(b.String())
}

func (m Model) renderLog() string {
	if m.width > 0 {
		m.vp.Width = m.width
	}
	return panelStyle.Render(m.vp.View())
}

func (m Model) renderHelp() string {
	lines := []string{
		"Keys: : command, ? help, q quit, scroll with arrows/PageUp/PageDown, history with Up/Down in command mode",
		"Commands: add [n] [w], remove <id>, restart <id>, kill <id>, weight <id> <w>, fail <id> on|off, health <id> on|off, shedding on|off, release <n>, focus <workload|none>, predict down <node>, explain <workload> <unit>, scenario <name>, load <file>",
	}
	return lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Render(strings.Join(lines, "\n"))
}

func formatEvent(ev sim.Event) string {
	ts := ev.At.Format("15:04:05")
	typ := strings.ToUpper(string(ev.Type))
	fields := formatFields(ev.Fields)
	if fields != "" {
		return fmt.Sprintf("%s [%s] %s %s", ts, typ, ev.Message, fields)
	}
	return fmt.Sprintf("%s [%s] %s", ts, typ, ev.Message)
}

func formatFields(fields map[string]interface{}) string {
	if len(fields) == 0 {
		return ""
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, fields[k]))
	}
	return strings.Join(parts, " ")
}

func formatRanking(r []sim.CandidateScore) string {
	if len(r) == 0 {
		return "-"
	}
	parts := make([]string, 0, len(r))
	for _, c := range r {
		parts = append(parts, fmt.Sprintf("%s(%.3f)", short(c.ID), c.Score))
	}
	return strings.Join(parts, " ")
}

func short(id string) string {
	if id == "" {
		return "-"
	}
	parts := strings.Split(id, ":")
	return parts[len(parts)-1]
}

func dur(d time.Duration) string {
	if d <= 0 {
		return "-"
	}
	if d > time.Second {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.String()
}

func valueOrDash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func stateStyle(state string) lipgloss.Style {
	switch state {
	case "ok":
		return lipgloss.NewStyle().Foreground(lipgloss.Color("42")).Bold(true)
	case "backoff":
		return lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Bold(true)
	case "unhealthy":
		return lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true)
	default:
		return lipgloss.NewStyle()
	}
}

var panelStyle = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).Padding(0, 1)
