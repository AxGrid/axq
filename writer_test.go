package axq

import (
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"sync"
	"testing"
	"time"
)

func TestWriter_Push(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{})

	testId := uuid.New()
	wr, err := NewWriter().
		WithDB(db).
		WithName(fmt.Sprintf("test_writer_%x", testId[0:8])).
		Build()
	assert.Nil(t, err)

	msg := []byte("hello world")
	count := 2_000_000
	wg := sync.WaitGroup{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = wr.Push(msg)
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
	lastID, err := wr.LastID()
	assert.Nil(t, err)
	assert.Equal(t, count, int(lastID))
}

func TestWriter_WithCut(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq_queue?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{})

	testId := uuid.New()
	wr, err := NewWriter().
		WithDB(db).
		WithName(fmt.Sprintf("test_writer_%x", testId[0:8])).
		WithCutFrequency(time.Second).
		WithCutSize(1000).
		Build()
	assert.Nil(t, err)

	msg := []byte("hello world")
	count := 10000
	wg := sync.WaitGroup{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = wr.Push(msg)
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
	time.Sleep(2 * time.Second)

	var blobs []*domain.Blob
	err = db.Table(fmt.Sprintf("axq_test_writer_%x", testId[0:8])).Where("to_id < ?", 9000).Find(&blobs).Error
	assert.Nil(t, err)
	assert.Equal(t, 0, len(blobs))
}

// Todo asserts
func TestWriter_UniqueNameAndUUID(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq_queue?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{})

	testId := uuid.New()
	wr, err := NewWriter().
		WithDB(db).
		WithName(fmt.Sprintf("test_writer_%x", testId[0:8])).
		//WithName("test_writer_7d365933c3df4e03").
		WithUUID(testId).
		WithCutFrequency(time.Second).
		WithCutSize(1000).
		Build()
	assert.Nil(t, err)
	assert.NotNil(t, wr)
}

func Test_WriterLoseConnect(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq_test?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	assert.Nil(t, err)

	testId := uuid.New()
	name := fmt.Sprintf("test_writer_%x", testId[0:8])
	existMap := make(map[uint64]bool)
	initReaderAndRestartDB := func() {
		wr, err := NewWriter().
			WithDB(db).
			WithName(name).
			WithUUID(testId).
			Build()
		assert.Nil(t, err)
		assert.NotNil(t, wr)
		retries := 5
		for i := 0; i < 1000; i++ {
			err = wr.Push([]byte("hello world"))
			if err != nil {
				retries--
			}
			if retries == 0 {
				return
			}
			lastId, err := wr.LastID()
			if err != nil {
				continue
			}
			existMap[lastId] = true
			time.Sleep(50 * time.Millisecond)
		}
	}

	continueWriteAfterRestart := func() {
		wr, err := NewWriter().
			WithDB(db).
			WithName(name).
			WithUUID(testId).
			Build()
		assert.Nil(t, err)
		assert.NotNil(t, wr)
		for i := 0; i < 150; i++ {
			err = wr.Push([]byte("hello world"))
			if err != nil {
				continue
			}
			lastId, err := wr.LastID()
			assert.Nil(t, err)
			existMap[lastId] = true
			time.Sleep(50 * time.Millisecond)
		}
	}

	initReaderAndRestartDB()
	time.Sleep(10 * time.Second)
	continueWriteAfterRestart()
	for i := 1; i <= len(existMap); i++ {
		assert.True(t, existMap[uint64(i)])
	}
}
