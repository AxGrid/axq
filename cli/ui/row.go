package ui

import (
	"context"
	"fmt"
	"github.com/axgrid/axq/cli/models"
	"github.com/axgrid/axq/domain"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"strings"
	"time"
)

type row struct {
	service     domain.Service
	updateCh    chan tea.Msg
	performance uint64
}

type Row interface {
	View() string
	Init() tea.Cmd
}

var (
	defaultStyle = lipgloss.NewStyle().Width(10).Align(lipgloss.Center)
)

func (r *row) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	return r, nil
}

func (r *row) View() string {
	view := strings.Builder{}
	view.WriteString(defaultStyle.Render(r.service.GetOpts().GetType()))
	view.WriteString(defaultStyle.Render(r.service.GetOpts().GetName()))
	view.WriteString(defaultStyle.Render(fmt.Sprintf("%d", r.service.Performance())))
	view.WriteString(defaultStyle.Render("OK"))
	return view.String()
}

func (r *row) Init() tea.Cmd {
	return nil
}

func NewRow(ch chan tea.Msg, service domain.Service) Row {
	r := &row{
		service:  service,
		updateCh: ch,
	}
	go r.updatePerformance(context.TODO())
	return r
}

func (r *row) updatePerformance(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.NewTicker(time.Second).C:
			r.performance = r.service.Performance()
			r.updateCh <- models.PerformanceUpdMsg{}
		}
	}
}
