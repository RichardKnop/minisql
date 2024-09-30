package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/RichardKnop/minisql/internal/pkg/minisql"
	"github.com/RichardKnop/minisql/internal/pkg/pager"
	"github.com/RichardKnop/minisql/internal/pkg/parser"
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

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// TODO - hardcoded database for now
	dbFile, err := os.OpenFile("db", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	defer dbFile.Close()

	aPager, err := pager.New(dbFile, minisql.SchemaTableName)
	if err != nil {
		panic(err)
	}
	aDatabase, err := minisql.NewDatabase(ctx, "db", parser.New(), aPager)
	if err != nil {
		panic(err)
	}
	aDatabase.CreateTestTable()

	reader := bufio.NewScanner(os.Stdin)
	printPrompt()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		if err := aDatabase.Close(ctx); err != nil {
			fmt.Printf("error closing database: %s\n", err)
		}
		cancel()
		// TODO - cleanup
		os.Exit(1)
	}()

	// REPL (Read-eval-print loop) start
	for reader.Scan() {
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
					aRow, err := aResult.Rows(ctx)
					for ; err == nil; aRow, err = aResult.Rows(ctx) {
						fmt.Println(aRow.Values)
					}
				}
			}
		}
		printPrompt()
	}
	// Print an additional line if we encountered an EOF character
	fmt.Println()
}
