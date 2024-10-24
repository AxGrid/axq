package main

import (
	"flag"
	"fmt"
	"github.com/axgrid/axq"
	"github.com/axgrid/axq/cli"
	"github.com/axgrid/axq/domain"
	"github.com/axgrid/axq/utils"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/rs/zerolog"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type BinLog struct {
	File string `gorm:"column:Log_name"`
	Size uint64 `gorm:"column:File_size"`
}

func main() {
	axq.NewWriter()
	dbParams := dbParamsFlags()
	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local",
		dbParams.User, dbParams.Password, dbParams.Host, dbParams.Port, dbParams.DBName,
	)
	gLogger := utils.NewGLogger(zerolog.Nop(), true).LogMode(logger.Warn)
	db, err := gorm.Open(mysql.Open(connectionString), &gorm.Config{Logger: gLogger, DisableForeignKeyConstraintWhenMigrating: true})
	if err != nil {
		panic(err)
	}

	dbInfo, err := getDBInfo(db)
	if err != nil {
		panic(err)
	}
	dbInfo.Host = dbParams.Host
	dbInfo.Port = dbParams.Port
	dbInfo.DBName = dbParams.DBName

	var g1 []domain.Service
	w1, err := axq.NewWriter().WithDB(db).WithPartitionsCount(4).Build()
	if err != nil {
		panic(err)
	}

	//go func() {
	//	for i := 0; i < 10000; i++ {
	//		go func(i int) {
	//			for {
	//				err = w1.Push([]byte(fmt.Sprintf("test_%d", i)))
	//				if err != nil {
	//					panic(err)
	//				}
	//			}
	//		}(i)
	//	}
	//}()
	//time.Sleep(10 * time.Second)

	r1, err := axq.NewReader().WithDB(db).WithReaderName("not_default_name").Build()
	if err != nil {
		panic(err)
	}

	r2, err := axq.NewReader().WithDB(db).WithBufferSize(100_000).Build()
	if err != nil {
		panic(err)
	}

	r3, err := axq.NewReader().WithDB(db).Build()
	if err != nil {
		panic(err)
	}
	g1 = append(g1, w1, r1, r2, r3)

	updCh := make(chan tea.Msg)
	if _, err = tea.NewProgram(cli.NewCLI(updCh, 1, []domain.Group{g1}, dbInfo)).Run(); err != nil {
		panic(err)
	}
}

func getDBInfo(db *gorm.DB) (domain.DBInfo, error) {
	var versionComment string
	err := db.Raw("SELECT @@version_comment;").First(&versionComment).Error
	if err != nil {
		return domain.DBInfo{}, err
	}

	var flush string
	err = db.Raw("SELECT @@innodb_flush_method;").First(&flush).Error
	if err != nil {
		return domain.DBInfo{}, err
	}

	var ver string
	err = db.Raw("SELECT VERSION();").First(&ver).Error
	if err != nil {
		return domain.DBInfo{}, err
	}

	var bufferSize uint64
	err = db.Raw("SELECT @@innodb_buffer_pool_size;").First(&bufferSize).Error
	if err != nil {
		return domain.DBInfo{}, err
	}

	var log BinLog
	err = db.Raw("SHOW BINARY LOGS;").First(&log).Error
	if err != nil {
		return domain.DBInfo{}, err
	}

	return domain.DBInfo{
		Version:        ver,
		VersionComment: versionComment,
		BufferSize:     bufferSize,
		BinLogSize:     log.Size,
		FlushMethod:    flush,
	}, nil
}

func dbParamsFlags() domain.DBParams {
	user := flag.String("db-user", "root", "db user")
	pass := flag.String("db-pass", "", "db pass")
	host := flag.String("db-host", "localhost", "")
	port := flag.Int("db-port", 3306, "db port")
	name := flag.String("db-name", "axq", "db name")
	flag.Parse()

	return domain.DBParams{
		User:     *user,
		Password: *pass,
		Host:     *host,
		Port:     *port,
		DBName:   *name,
	}
}
