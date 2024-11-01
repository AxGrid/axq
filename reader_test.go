package axq

import (
	"fmt"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/protobuf"
	"github.com/axgrid/axq/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"testing"
)

func TestReader_Pop(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq_queue?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	assert.Nil(t, err)
	l := utils.InitLogger("default")

	testId := uuid.New()
	testTableName := fmt.Sprintf("test_table_%x", testId[0:8])
	testReaderName := fmt.Sprintf("test_reader_%x", testId[0:8])
	r, err := NewReader().
		WithDB(db).
		WithName(testTableName).
		WithReaderName(testReaderName).
		WithLogger(l).
		Build()
	assert.Nil(t, err)

	count := 1000
	err = prepareData(db, testTableName, count)
	assert.Nil(t, err)

	uniqueMap := make(map[uint64]bool)
	for {
		msg := r.Pop()
		msg.Done()
		uniqueMap[msg.Id()] = true
		lastId, _ := r.LastID()
		if lastId == uint64(count) {
			break
		}
	}
	for k, v := range uniqueMap {
		assert.Truef(t, v, fmt.Sprintf("not exists %d", k))
	}
}

func prepareData(db *gorm.DB, tableName string, count int) error {
	msg := []byte("test_msg")
	for i := 0; i < count; i++ {
		blobMsg := &protobuf.BlobMessage{
			Id:      uint64(i + 1),
			Message: msg,
		}
		list := protobuf.BlobMessageList{
			Fid:      uint64(i + 1),
			Messages: []*protobuf.BlobMessage{blobMsg},
		}
		blobBytes, err := proto.Marshal(&list)
		if err != nil {
			return err
		}
		err = db.Table("axq_" + tableName).Create(&domain.Blob{Total: 1, FromId: list.Messages[0].Id, ToId: list.Messages[len(list.Messages)-1].Id, Message: blobBytes}).Error
		if err != nil {
			return err
		}
	}
	return nil
}
