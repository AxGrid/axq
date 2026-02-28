package axq

import (
	"testing"
	"time"

	"github.com/axgrid/axq/utils"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestArchiver(t *testing.T) {
	connectionString := "root:@tcp(localhost:3306)/axq_queue?charset=utf8&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	assert.Nil(t, err)
	l := utils.InitLogger("debug")

	testTableName := "test_writer_0812ce2d85864d43"
	_, err = NewArchiver().
		WithDB(db).
		WithName(testTableName).
		WithLogger(l).
		Build()
	assert.Nil(t, err)
	timer := time.NewTimer(time.Second * 30)
	<-timer.C
}
