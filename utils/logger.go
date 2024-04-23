package utils

import (
	"context"
	"fmt"
	"github.com/go-errors/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"os"
	"time"
)

type GLogger struct {
	zerolog.Logger
	level                     logger.LogLevel
	IgnoreRecordNotFoundError bool
}

func NewGLogger(zlog zerolog.Logger, ignoreRecordNotFoundError bool) GLogger {
	return GLogger{
		Logger:                    zlog,
		level:                     logger.Info,
		IgnoreRecordNotFoundError: ignoreRecordNotFoundError,
	}
}

func (G GLogger) LogMode(level logger.LogLevel) logger.Interface {
	G.level = level
	return G
}

func (G GLogger) Info(ctx context.Context, s string, i ...interface{}) {
	if G.level >= logger.Info {
		G.Logger.Info().Msgf(s, i...)
	}
}

func (G GLogger) Warn(ctx context.Context, s string, i ...interface{}) {
	if G.level >= logger.Warn {
		G.Logger.Warn().Msgf(s, i...)
	}
}

func (G GLogger) Error(ctx context.Context, s string, i ...interface{}) {
	if G.level >= logger.Error {
		G.Logger.Error().Msgf(s, i...)
	}
}

func (G GLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if G.level <= logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	switch {
	case err != nil && G.level >= logger.Error && (!errors.Is(err, gorm.ErrRecordNotFound) || !G.IgnoreRecordNotFoundError):
		sql, rows := fc()
		if rows == -1 {
			G.Logger.Error().Err(err).Str("time", fmt.Sprintf("%.3fms", float64(elapsed.Nanoseconds())/1e6)).Msg(sql)
		} else {
			G.Logger.Error().Err(err).Str("time", fmt.Sprintf("%.3fms", float64(elapsed.Nanoseconds())/1e6)).Int64("rows", rows).Msg(sql)
		}
	case G.level >= logger.Info:
		sql, rows := fc()
		if rows == -1 {
			G.Logger.Debug().Str("time", fmt.Sprintf("%.3fms", float64(elapsed.Nanoseconds())/1e6)).Msg(sql)
		} else {
			G.Logger.Debug().Str("time", fmt.Sprintf("%.3fms", float64(elapsed.Nanoseconds())/1e6)).Int64("rows", rows).Msg(sql)
		}
	}
}

func GetLogLevel(levelString string) zerolog.Level {
	switch levelString {
	default:
		return zerolog.DebugLevel
	case "trace":
		return zerolog.TraceLevel
	case "info":
		return zerolog.InfoLevel
	case "warn":
		return zerolog.WarnLevel
	case "err":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	}
}

const LogTimeFormat = "2006-01-02 15:04:05.00000"
const LogTraceTimeFormat = "2006-01-02 15:04:05.00000"

func InitLogger(level string) zerolog.Logger {
	if level == "" {
		level = "debug"
	}
	if level == "trace" {

		zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: LogTraceTimeFormat}).Level(GetLogLevel(level))
	} else {
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMicro
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: LogTimeFormat}).Level(GetLogLevel(level))
	}
	return log.Logger
}
