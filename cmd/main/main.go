package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
	default:
		return Unknown
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	reader := bufio.NewScanner(os.Stdin)
	printPrompt()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
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
			case Exit:
				// Return exits with code 0 by default, os.Exit(0)
				// would exit immediately without any defers
				return
			case Unknown:
				fmt.Printf("Unrecognized meta command: %s\n", inputBuffer)
			}
		} else {
			aParser := parser.New(inputBuffer)
			aStatement, err := aParser.Parse(ctx)
			if err != nil {
				// Parser logs error internally
			} else if err := aStatement.Execute(ctx); err != nil {
				fmt.Printf("Error executing statement: %s", err)
			}
		}
		printPrompt()
	}
	// Print an additional line if we encountered an EOF character
	fmt.Println()
}