package service

import (
	"fmt"
	"github.com/axgrid/axq/utils"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"testing"
)

var testDataBase *gorm.DB

var (
	b2KeyId    = os.Getenv("AX_TEST_B2_KEY_ID")
	b2AppKey   = os.Getenv("AX_TEST_B2_APP_KEY")
	b2Endpoint = getenv("AX_TEST_B2_ENDPOINT", "s3.us-east-005.backblazeb2.com")
	dbUser     = getenv("AX_TEST_DB_USER", "root")
	dbPassword = getenv("AX_TEST_DB_PASSWORD", "")
	dbName     = getenv("AX_TEST_DB_NAME", "axq")
	dbHost     = getenv("AX_TEST_DB_HOST", "127.0.0.1")
	dbPort     = getenv("AX_TEST_DB_PORT", "3306")
)

func TestMain(m *testing.M) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"}).Level(zerolog.ErrorLevel)
	gLogger := utils.NewGLogger(log.Logger, true).LogMode(logger.Info)
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local", dbUser, dbPassword, dbHost, dbPort, dbName)
	var err error
	testDataBase, err = gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: gLogger, DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
