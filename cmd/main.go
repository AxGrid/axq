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
	w1, err := axq.WriterBuild().WithPartitionsCount(4).Build()
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

	// Подсвечивать желтым если отстает больше чем на 5%
	// Шапка
	// Версия бд, буффер сайз, операции
	// Флаги запуска с коннектом к другой бд
	updCh := make(chan tea.Msg)
	//TODO groups
	if _, err = tea.NewProgram(cli.NewCLI(updCh, 2, []domain.Group{g1})).Run(); err != nil {
		panic(err)
	}
}
