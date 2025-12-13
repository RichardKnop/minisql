package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/RichardKnop/minisql/internal/parser"
	"github.com/RichardKnop/minisql/internal/pkg/logging"
	"github.com/RichardKnop/minisql/internal/protocol"
)

const defaultDbFileName = "db"

var (
	dbNameFlag string
	portFlag   int
)

func init() {
	flag.StringVar(&dbNameFlag, "db", defaultDbFileName, "Filename of the database to use")
	flag.IntVar(&portFlag, "port", 8080, "Port to listen on")
}

func main() {
	flag.Parse()

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

	dbFile, err := os.OpenFile(dbNameFlag, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer dbFile.Close()

	aPager, err := minisql.NewPager(dbFile, minisql.PageSize)
	if err != nil {
		panic(err)
	}
	aDatabase, err := minisql.NewDatabase(ctx, logger, "db", parser.New(), aPager, aPager, aPager)
	if err != nil {
		panic(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	srv, err := protocol.NewServer(aDatabase, logger, portFlag)
	if err != nil {
		panic(err)
	}

	srv.Serve(ctx)

	<-sigChan

	srv.Stop()
	if err := aDatabase.Close(ctx); err != nil {
		fmt.Printf("error closing database: %s\n", err)
	}
	cancel()

	os.Exit(0)
}
