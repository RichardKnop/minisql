package logging

import (
	"strconv"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func DefaultConfig() zap.Config {
	logConf := zap.NewProductionConfig()
	logConf.Sampling = nil
	logConf.EncoderConfig.TimeKey = "time"
	logConf.EncoderConfig.LevelKey = "severity"
	logConf.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logConf.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	return logConf
}

func ParseLevel(l string) (zapcore.Level, error) {
	l = strings.ToLower(strings.TrimSpace(l))
	switch l {
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	case "dpanic":
		return zapcore.DPanicLevel, nil
	case "panic":
		return zapcore.PanicLevel, nil
	case "fatal":
		return zapcore.FatalLevel, nil
	default:
		level, err := strconv.ParseInt(l, 10, 8)
		if err != nil {
			return 0, err
		}
		return zapcore.Level(level), nil
	}
}
