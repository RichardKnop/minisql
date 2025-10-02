package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"go.uber.org/zap"

	"github.com/RichardKnop/minisql/internal/core/database"
	"github.com/RichardKnop/minisql/internal/core/minisql"
	"github.com/RichardKnop/minisql/internal/core/parser"
	"github.com/RichardKnop/minisql/internal/pkg/logging"
	"github.com/RichardKnop/minisql/internal/pkg/util"
)

const (
	cliName string = "minisql"
)

func printPrompt() {
	fmt.Print(cliName, "> ")
}

func sanitizeReplInput(input string) string {
	output := strings.TrimSpace(input)
	output = strings.ToLower(output)
	return output
}

type metaCommand int

const (
	Unknown metaCommand = iota + 1
	Help
	Exit
	ListTables
)

func isMetaCommand(inputBuffer string) bool {
	return len(inputBuffer) > 0 && inputBuffer[:1] == "."
}

func doMetaCommand(inputBuffer string) metaCommand {
	switch inputBuffer {
	case "help":
		return Help
	case "exit":
		return Exit
	case "tables":
		return ListTables
	default:
		return Unknown
	}
}

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
	dbFile, err := os.OpenFile(defaultDbFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer dbFile.Close()

	aPager, err := minisql.NewPager(dbFile, minisql.PageSize, minisql.SchemaTableName)
	if err != nil {
		panic(err)
	}
	aDatabase, err := database.New(ctx, logger, "db", parser.New(), aPager)
	if err != nil {
		panic(err)
	}
	aDatabase.CreateTestTable()

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()
		reader := bufio.NewScanner(os.Stdin)
		printPrompt()

		// REPL (Read-eval-print loop) start
		for reader.Scan() {
			if ctx.Err() != nil {
				break
			}

			inputBuffer := sanitizeReplInput(reader.Text())
			if isMetaCommand(inputBuffer) {
				switch doMetaCommand(inputBuffer[1:]) {
				case Help:
					fmt.Println(".help    - Show available commands")
					fmt.Println(".exit    - Closes program")
					fmt.Println(".tables  - List all tables in the current database")
				case Exit:
					// Return exits with code 0 by default, os.Exit(0)
					// would exit immediately without any defers
					return
				case ListTables:
					for _, table := range aDatabase.ListTableNames(ctx) {
						fmt.Println(table)
					}
				case Unknown:
					fmt.Printf("Unrecognized meta command: %s\n", inputBuffer)
				}
			} else {
				stmt, err := aDatabase.PrepareStatement(ctx, inputBuffer)
				if err != nil {
					// Parser logs error internally
				} else {
					aResult, err := aDatabase.ExecuteStatement(ctx, stmt)
					if err != nil {
						fmt.Printf("Error executing statement: %s\n", err)
					} else if stmt.Kind == minisql.Insert || stmt.Kind == minisql.Update || stmt.Kind == minisql.Delete {
						fmt.Printf("Rows affected: %d\n", aResult.RowsAffected)
					} else if stmt.Kind == minisql.Select {
						util.PrintTableHeader(os.Stdout, aResult.Columns)
						aRow, err := aResult.Rows(ctx)
						for ; err == nil; aRow, err = aResult.Rows(ctx) {
							util.PrintTableRow(os.Stdout, aResult.Columns, aRow.Values)
						}
					}
				}
			}
			printPrompt()
		}
		// Print an additional line if we encountered an EOF character
		fmt.Println()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	if err := aDatabase.Close(ctx); err != nil {
		fmt.Printf("error closing database: %s\n", err)
	}

	cancel()

	wg.Wait()

	// TODO - cleanup
	os.Exit(1)
}
