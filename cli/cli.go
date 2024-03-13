package cli

import (
	"fmt"
	"github.com/axgrid/axq/cli/models"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/utils"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/olekukonko/tablewriter"
	"strings"
	"time"
)

type CLI struct {
	updateCh chan tea.Msg
	table    string
	timeOut  int
	dbInfo   domain.DBInfo
	groups   []domain.Group
}

func (c *CLI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q":
			return c, tea.Quit
		}
	case models.PerformanceUpdMsg:
		c.table = c.updateTable()
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
	view.WriteString(c.headerView() + "\n")
	view.WriteString(c.table + "\n")
	return view.String()
}

func (c *CLI) headerView() string {
	binLog := "off"
	if c.dbInfo.BinLogSize > 0 {
		binLog = "on"
	}
	return fmt.Sprintf("%s v.%s.\nName: %s. Host: %s:%d\nBuffer Size: %dMB. Binlog: %s. Flush: %s",
		c.dbInfo.VersionComment,
		c.dbInfo.Version,
		c.dbInfo.DBName,
		c.dbInfo.Host, c.dbInfo.Port,
		c.dbInfo.BufferSize/1024/1024,
		binLog,
		c.dbInfo.FlushMethod,
	)
}

func (c *CLI) Init() tea.Cmd {
	return tea.Batch(waitUpdates(c.updateCh), ticker(c.updateCh, c.timeOut))
}

func NewCLI(updCh chan tea.Msg, timeOut int, groups []domain.Group, db domain.DBInfo) *CLI {
	c := &CLI{
		updateCh: updCh,
		timeOut:  timeOut,
		groups:   groups,
		dbInfo:   db,
	}
	c.table = c.updateTable()
	return c
}

var (
	lagStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("#FF0000"))
	semiLagStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#FFFF00"))
	normalStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("#228B22"))
)

func (c *CLI) updateTable() string {
	var data [][]string
	for _, g := range c.groups {
		var writerLastId uint64
		for _, s := range g {
			if s.GetOpts().GetType() == "Writer" {
				writerLastId, _ = s.LastID()
			}
			lastFid, _ := s.LastFID()
			lastId, _ := s.LastID()

			// Check readers lag
			lastIdRender := fmt.Sprintf("%d", lastId)
			if lastId < writerLastId-(writerLastId/100*10) {
				lastIdRender = lagStyle.Render(lastIdRender)
			} else if lastId < writerLastId-(writerLastId/100*5) {
				lastIdRender = semiLagStyle.Render(lastIdRender)
			} else {
				lastIdRender = normalStyle.Render(lastIdRender)
			}
			data = append(data, []string{
				s.GetOpts().GetName(),
				utils.Shortener(s.GetOpts().GetReaderName()),
				s.GetOpts().GetType(),
				fmt.Sprintf("%d", s.Performance()),
				fmt.Sprintf("%d", lastFid),
				lastIdRender,
			})
		}
	}
	tableStr := &strings.Builder{}
	table := tablewriter.NewWriter(tableStr)

	// если ридер сильно отстает от райтера подсвечивать красным (вместо статуса)
	// Древовидная структура
	// Имена ридера
	table.SetHeader([]string{"NAME", "READER NAME", "TYPE", "OP/SEC", "FID", "LAST ID"})
	table.SetAutoMergeCellsByColumnIndex([]int{0})
	table.AppendBulk(data)
	table.Render()
	return tableStr.String()
}

//├

func ticker(ch chan tea.Msg, timeOut int) tea.Cmd {
	return func() tea.Msg {
		for {
			time.Sleep(time.Second * time.Duration(timeOut))
			ch <- models.PerformanceUpdMsg{}
		}
	}
}

func waitUpdates(ch chan tea.Msg) tea.Cmd {
	return func() tea.Msg {
		return <-ch
	}
}
