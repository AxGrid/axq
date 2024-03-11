package main

import (
	"github.com/axgrid/axq"
	"github.com/axgrid/axq/cli"
	"github.com/axgrid/axq/domain"
	tea "github.com/charmbracelet/bubbletea"
)

func main() {

	// Group 1
	var g1 []domain.Service
	w1, err := axq.Writer().WithPartitionsCount(4).Build()
	if err != nil {
		panic(err)
	}

	r1, err := axq.Reader().Build()
	if err != nil {
		panic(err)
	}

	r2, err := axq.Reader().Build()
	if err != nil {
		panic(err)
	}

	r3, err := axq.Reader().Build()
	if err != nil {
		panic(err)
	}
	g1 = append(g1, w1, r1, r2, r3)

	// Group 2
	var g2 []domain.Service
	w2, err := axq.Writer().WithName("unnamed2").Build()
	if err != nil {
		panic(err)
	}

	r4, err := axq.Reader().WithName("unnamed2").Build()
	if err != nil {
		panic(err)
	}

	r5, err := axq.Reader().WithName("unnamed2").Build()
	if err != nil {
		panic(err)
	}

	r6, err := axq.Reader().WithName("unnamed2").Build()
	if err != nil {
		panic(err)
	}
	g2 = append(g2, w2, r4, r5, r6)

	updCh := make(chan tea.Msg)
	//TODO groups
	if _, err = tea.NewProgram(cli.NewCLI(updCh, 2, []domain.Group{g1, g2})).Run(); err != nil {
		panic(err)
	}
}
