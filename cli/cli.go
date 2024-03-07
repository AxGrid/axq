package cli

import (
	"github.com/axgrid/axq/cli/models"
	"github.com/axgrid/axq/cli/ui"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"strings"
)

type CLI struct {
	columns  []string
	updateCh chan tea.Msg
	rows     []ui.Row
}

func (c *CLI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q":
			return c, tea.Quit
		}
	case models.PerformanceUpdMsg:
		return c, waitUpdates(c.updateCh)
	}
	return c, nil
}

var (
	defaultStyle = lipgloss.NewStyle().Width(10).Align(lipgloss.Center)
	footerStyle  = lipgloss.NewStyle().Height(10)
)

func (c *CLI) View() string {
	view := strings.Builder{}
	var renderColumns []string
	for _, col := range c.columns {
		renderColumns = append(renderColumns, defaultStyle.Render(col))
	}
	view.WriteString(lipgloss.JoinHorizontal(lipgloss.Left, renderColumns...) + "\n")

	var renderRows []string
	for _, r := range c.rows {
		renderRows = append(renderRows, r.View())
	}
	view.WriteString(lipgloss.JoinVertical(lipgloss.Left, renderRows...))
	view.WriteString(footerStyle.Render(""))
	return view.String()
}

func (c *CLI) Init() tea.Cmd {
	var cmds []tea.Cmd
	for _, r := range c.rows {
		cmds = append(cmds, r.Init())
	}
	cmds = append(cmds, waitUpdates(c.updateCh))

	return tea.Batch(cmds...)
}

func NewCLI(updCh chan tea.Msg, rows []ui.Row) *CLI {
	c := &CLI{
		columns:  []string{"TYPE", "NAME", "OP/SEC", "STATUS"},
		rows:     rows,
		updateCh: updCh,
	}
	return c
}

func waitUpdates(ch chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		return <-ch
	}
}
