package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/RichardKnop/minisql/internal/core/minisql"
	"github.com/RichardKnop/minisql/internal/core/protocol"
	"github.com/RichardKnop/minisql/internal/pkg/util"
)

const (
	cliName string = "minisql"
)

func printPrompt() {
	fmt.Print(cliName, "> ")
}

func main() {
	conn, err := net.Dial("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	reader := bufio.NewReader(os.Stdin)

	printPrompt()
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			break
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

		var req protocol.Request
		switch input {
		case ".ping":
			req = protocol.Request{Type: "ping"}
		case ".tables":
			req = protocol.Request{Type: "list_tables"}
		default:
			req = protocol.Request{Type: "sql", SQL: input}
		}

		if err := sendRequest(conn, req); err != nil {
			fmt.Printf("Error: %v\n", err)
		}

		// Read response
		if scanner.Scan() {
			var resp protocol.Response
			if err := json.Unmarshal(scanner.Bytes(), &resp); err == nil {
				printResponse(resp)
			}
		}

		printPrompt()
	}
}

func sendRequest(conn net.Conn, req protocol.Request) error {
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}

	_, err = conn.Write(append(data, '\n'))
	return err
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
		util.PrintTableHeader(os.Stdout, resp.Columns)
	}
	if len(resp.Rows) > 0 {
		for _, row := range resp.Rows {
			util.PrintTableRow(os.Stdout, resp.Columns, row)
		}
	}

	if resp.RowsAffected > 0 {
		fmt.Printf("Rows affected: %d\n", resp.RowsAffected)
	}
}
