package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/RichardKnop/minisql"
)

const usage = `Usage: minisql <database-file>

Opens (or creates) a MiniSQL database and starts an interactive SQL shell.
SQL statements must be terminated with a semicolon (;).
Enter ".help" for dot command reference.
`

func main() {
	if len(os.Args) != 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	filePath := os.Args[1]

	db, err := sql.Open("minisql", filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening %s: %v\n", filePath, err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		_ = db.Close()
		fmt.Fprintf(os.Stderr, "Error connecting to %s: %v\n", filePath, err)
		os.Exit(1)
	}
	defer db.Close()

	newShell(db, filePath).run()
}
