package tests

import (
	"fmt"
	"github.com/axgrid/axq"
	"github.com/axgrid/axq/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
	"time"
)

func Test_SharedWriter(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq_queue?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{})

	testId := uuid.New()
	name := fmt.Sprintf("test_writer_%x", testId[0:8])
	eventsWriter, err := axq.NewWriter().
		WithDB(db).
		WithName(name).
		Build()
	assert.Nil(t, err)

	l := utils.InitLogger("debug")
	reader, err := axq.NewReader().
		WithName(name).
		WithReaderName(fmt.Sprintf("test_reader_%x", testId[0:8])).
		WithLogger(l).
		WithLoaderCount(1).
		WithWaiterCount(2).
		WithDB(db).
		Build()
	reader = reader
	go func() {
		for {
			msg := <-reader.C()
			fmt.Println(msg.Id())
			msg.Done()
		}
	}()
	go func() {
		for i := 0; i < 10000; i++ {
			msg := []byte("hello world")
			err = eventsWriter.Push(msg)
			assert.Nil(t, err)
			time.Sleep(1 * time.Millisecond)
		}
	}()
	go func() {
		for i := 0; i < 10000; i++ {
			msg := []byte("bye world")
			err = eventsWriter.Push(msg)
			assert.Nil(t, err)
			time.Sleep(2 * time.Millisecond)
		}
	}()
	time.Sleep(10 * time.Second)
}
