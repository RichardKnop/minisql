package main

import (
	"context"
	"os"

	"go.uber.org/zap"

	"github.com/RichardKnop/minisql/internal/core/minisql"
	"github.com/RichardKnop/minisql/internal/core/parser"
	"github.com/RichardKnop/minisql/internal/pkg/logging"
)

const defaultDbFileName = "db"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logConf := logging.DefaultConfig()

	level := os.Getenv("LOG_LEVEL")
	if level == "" {
		level = "info"
	}

	l, err := logging.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	logConf.Level = zap.NewAtomicLevelAt(l)

	logger, err := logConf.Build()
	if err != nil {
		panic(err)
	}

	defer logger.Sync() // flushes buffer, if any

	// TODO - hardcoded database for now
	dbFile, err := os.OpenFile("db", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer dbFile.Close()

	aPager, err := minisql.NewPager(dbFile, minisql.PageSize)
	if err != nil {
		panic(err)
	}
	_, err = minisql.NewDatabase(ctx, logger, defaultDbFileName, parser.New(), aPager)
	if err != nil {
		panic(err)
	}
}
