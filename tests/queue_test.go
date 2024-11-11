package tests

import (
	"context"
	"fmt"
	"github.com/axgrid/axq"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
	"testing"
	"time"
)

func Test_WriterWithCutAndReaderStart(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq_queue?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{})

	testId := uuid.New()
	name := fmt.Sprintf("test_writer_%x", testId[0:8])
	wr, err := axq.NewWriter().
		WithDB(db).
		WithName(name).
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
	time.Sleep(4 * time.Second)

	var blobs []*domain.Blob
	err = db.Table("axq_"+name).Where("to_id > ?", 9000).Order("fid desc").Find(&blobs).Error
	assert.Nil(t, err)
	assert.NotNil(t, blobs)
	assert.NotEmpty(t, blobs)

	minId, err := wr.MinimalID()
	assert.Nil(t, err)

	minFid, err := wr.MinimalFID()
	assert.Nil(t, err)

	l := utils.InitLogger("debug")
	r, err := axq.NewReader().
		WithName(name).
		WithReaderName(fmt.Sprintf("test_reader_%x", testId[0:8])).
		WithLastId(&domain.LastIdOptions{
			FID:    minFid,
			LastId: minId,
		}).
		WithLogger(l).
		WithDB(db).
		Build()
	assert.Nil(t, err)

	readMap := make(map[uint64]bool)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
readLoop:
	for {
		select {
		case <-ctx.Done():
			break readLoop
		case msg := <-r.C():
			msg.Done()
			readMap[msg.Id()] = true
		}
	}
	fmt.Println(blobs[0].ToId-blobs[len(blobs)-1].FromId, len(readMap))
	assert.Equal(t, blobs[0].ToId-blobs[len(blobs)-1].FromId, uint64(len(readMap)))
}

func Test_SharedWriter(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq_queue?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{})

	testId := uuid.New()
	name := fmt.Sprintf("test_writer_%x", testId[0:8])
	eventsWriter, err := axq.NewWriter().
		WithDB(db).
		WithName(name).
		WithCutFrequency(time.Second).
		WithCutSize(1000).
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
