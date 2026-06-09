package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/RichardKnop/minisql"
)

// Set by goreleaser via -ldflags at release time.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const usage = `Usage: minisql [options] <database-file>

Opens (or creates) a MiniSQL database and starts an interactive SQL shell.
SQL statements must be terminated with a semicolon (;).
Enter ".help" for dot command reference.

Options:
  -c <query>      Execute a single SQL statement and exit (no shell).
                  May be specified multiple times to run several statements.
  -csv            Set output mode to CSV (default: table).
  -version        Print version information and exit.
  -h, --help      Show this message.

Examples:
  minisql my.db
  minisql -c 'select * from "users"' my.db
  minisql -c 'create table "t" (id int8)' -c 'insert into "t" values (1)' my.db
  minisql -csv -c 'select * from "users"' my.db
`

func main() {
	var (
		queries     multiFlag
		csvMode     bool
		showVersion bool
	)
	flag.Var(&queries, "c", "SQL statement to execute (may be repeated)")
	flag.BoolVar(&csvMode, "csv", false, "output in CSV format")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	if showVersion {
		fmt.Printf("minisql %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	if flag.NArg() != 1 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(1)
	}

	filePath := flag.Arg(0)

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

	sh := newShell(db, filePath)
	if csvMode {
		sh.mode = modeCSV
	}

	if len(queries) > 0 {
		for _, q := range queries {
			sh.exec(q)
		}
		return
	}

	sh.run()
}

// multiFlag collects repeated -c flags into a slice.
type multiFlag []string

func (f *multiFlag) String() string {
	if f == nil {
		return ""
	}
	return fmt.Sprintf("%v", []string(*f))
}

func (f *multiFlag) Set(v string) error {
	*f = append(*f, v)
	return nil
}
