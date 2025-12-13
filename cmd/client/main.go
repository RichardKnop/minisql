package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/RichardKnop/minisql/internal/minisql"
	"github.com/RichardKnop/minisql/internal/protocol"
)

const (
	cliName string = "minisql"
)

var (
	addressFlag string
)

func init() {
	flag.StringVar(&addressFlag, "a", ":8080", "Address to dial")
}

func printPrompt() {
	fmt.Print(cliName, "> ")
}

func main() {
	flag.Parse()

	aClient, err := protocol.NewClient(addressFlag)
	if err != nil {
		log.Fatal(err)
	}
	defer aClient.Close()

	reader := bufio.NewReader(os.Stdin)

	printPrompt()
	var input string
	for {
		r, _, err := reader.ReadRune()
		if err != nil {
			log.Fatal(err)
		}
		input += string(r)
		if input == "." {
			command, err := reader.ReadString('\n')
			if err != nil {
				log.Fatal(err)
			}
			input += command
		} else {
			query, err := reader.ReadString(';')
			if err != nil {
				log.Fatal(err)
			}
			input += query
		}

		input = strings.TrimSpace(input)
		switch input {
		case ".help":
			fmt.Println(".help    - Show available commands")
			fmt.Println(".exit    - Closes program")
			fmt.Println(".tables  - List all tables in the current database")
			fmt.Println(".ping    - Check if the server is alive")
			printPrompt()
			continue
		case ".exit":
			fmt.Println("Goodbye!")
			return
		case "":
			printPrompt()
			continue
		}

		var resp protocol.Response
		switch input {
		case ".ping":
			resp, err = aClient.SendMetaCommand("ping")
		case ".tables":
			resp, err = aClient.SendMetaCommand("list_tables")
		default:
			resp, err = aClient.SendQuery(input)
		}
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		printResponse(resp)

		printPrompt()
		input = ""
		reader.Reset(os.Stdin)
	}
}

func printResponse(resp protocol.Response) {
	if !resp.Success {
		fmt.Printf("Error: %s\n", resp.Error)
		return
	}

	if resp.Message != "" {
		fmt.Println(resp.Message)
	}

	if resp.Kind == minisql.Select {
		protocol.PrintTableHeader(os.Stdout, resp.Columns)
	}
	if len(resp.Rows) > 0 {
		for _, row := range resp.Rows {
			protocol.PrintTableRow(os.Stdout, resp.Columns, row)
		}
	}

	if resp.RowsAffected > 0 {
		fmt.Printf("Rows affected: %d\n", resp.RowsAffected)
	}
}
