package utils

import (
	"fmt"
	"testing"
)

func TestNewLastId(t *testing.T) {
	lastId := NewLastId[uint64](100)
	go func() {
		for {
			<-lastId.C()
		}
	}()
	lastId.Add(101)
	lastId.Add(102)
	lastId.Add(103)
	lastId.Add(107)
	lastId.Add(106)
	lastId.Add(105)
	lastId.Add(104)
	fmt.Println(lastId.Current())
}
