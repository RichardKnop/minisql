package main

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/mattn/go-isatty"
	"github.com/peterh/liner"
)

const (
	promptPrimary      = "minisql> "
	promptContinuation = "      -> "
)

type shell struct {
	db       *sql.DB
	out      io.Writer
	mode     outputMode
	timer    bool
	buf      strings.Builder
	scanner  *bufio.Scanner // used when liner is nil (non-tty / tests)
	liner    *liner.State   // used for interactive tty sessions
	filePath string
	isatty   bool
}

func newShell(db *sql.DB, filePath string) *shell {
	sh := &shell{
		db:       db,
		out:      os.Stdout,
		mode:     modeTable,
		filePath: filePath,
		isatty:   isatty.IsTerminal(os.Stdin.Fd()),
	}
	if sh.isatty {
		l := liner.NewLiner()
		l.SetCtrlCAborts(true)
		sh.liner = l
	} else {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		sh.scanner = sc
	}
	return sh
}

// readLine returns the next input line, showing prompt in tty mode.
// Returns ("", io.EOF) on end-of-input, ("", liner.ErrPromptAborted) on Ctrl+C.
func (s *shell) readLine(prompt string) (string, error) {
	if s.liner != nil {
		return s.liner.Prompt(prompt)
	}
	if !s.scanner.Scan() {
		return "", io.EOF
	}
	return s.scanner.Text(), nil
}

// historyFile returns the path to the persistent history file (~/.minisql_history).
func historyFile() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".minisql_history")
}

func (s *shell) run() {
	if s.isatty {
		fmt.Fprintf(s.out, "MiniSQL — %s\nEnter \".help\" for usage hints.\n", s.filePath)
	}
	if s.liner != nil {
		// Load history from previous sessions.
		if hf := historyFile(); hf != "" {
			if f, err := os.Open(hf); err == nil {
				_, _ = s.liner.ReadHistory(f)
				_ = f.Close()
			}
		}
		defer func() {
			// Persist history for future sessions.
			if hf := historyFile(); hf != "" {
				if f, err := os.Create(hf); err == nil {
					_, _ = s.liner.WriteHistory(f)
					_ = f.Close()
				}
			}
			_ = s.liner.Close()
		}()
	}

	for {
		prompt := promptPrimary
		if s.buf.Len() > 0 {
			prompt = promptContinuation
		}

		line, err := s.readLine(prompt)
		if errors.Is(err, liner.ErrPromptAborted) {
			// Ctrl+C — discard the in-progress buffer and start fresh.
			if s.buf.Len() > 0 {
				s.buf.Reset()
				fmt.Fprintln(s.out, "^C")
			}
			continue
		}
		if err != nil {
			// EOF (Ctrl+D) or scanner finished — flush any partial statement.
			if s.buf.Len() > 0 {
				s.exec(strings.TrimSpace(s.buf.String()))
			}
			break
		}

		// Dot commands are always single-line.
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, ".") {
			if s.buf.Len() == 0 {
				s.dotCommand(trimmed)
			} else {
				fmt.Fprintln(s.out, "Error: dot commands not allowed inside a multi-line statement")
			}
			continue
		}

		s.buf.WriteString(line)
		s.buf.WriteByte('\n')

		// Execute once we see a complete statement (line ending with ';' outside quotes).
		if statementComplete(s.buf.String()) {
			stmt := strings.TrimSpace(s.buf.String())
			s.buf.Reset()
			if stmt != "" && stmt != ";" {
				if s.liner != nil {
					s.liner.AppendHistory(stmt)
				}
				s.exec(stmt)
			}
		}
	}
}

// statementComplete returns true when buf contains at least one full SQL
// statement ending with ';' (ignoring quotes and comments at a basic level).
func statementComplete(buf string) bool {
	inSingle := false
	inDouble := false
	for i := 0; i < len(buf); i++ {
		c := buf[i]
		switch {
		case c == '\'' && !inDouble:
			// Check for escaped quote (doubled).
			if inSingle && i+1 < len(buf) && buf[i+1] == '\'' {
				i += 1
			} else {
				inSingle = !inSingle
			}
		case c == '"' && !inSingle:
			inDouble = !inDouble
		case c == ';' && !inSingle && !inDouble:
			return true
		}
	}
	return false
}

// isSelectLike returns true for statements that produce a result set and
// should be executed with db.Query rather than db.Exec.
func isSelectLike(query string) bool {
	fields := strings.Fields(query)
	if len(fields) == 0 {
		return false
	}
	switch strings.ToUpper(fields[0]) {
	case "SELECT", "WITH", "EXPLAIN", "VALUES", "TABLE":
		return true
	}
	// INSERT/UPDATE/DELETE with a RETURNING clause also produces rows.
	return hasReturning(strings.ToUpper(query))
}

// hasReturning does a lightweight scan for a RETURNING keyword outside quotes.
func hasReturning(upper string) bool {
	return strings.Contains(upper, "RETURNING")
}

func (s *shell) exec(query string) {
	start := time.Now()

	if isSelectLike(query) {
		s.execQuery(query, start)
	} else {
		s.execStatement(query, start)
	}
}

// execQuery runs statements that return rows (SELECT, EXPLAIN, WITH, RETURNING).
func (s *shell) execQuery(query string, start time.Time) {
	rows, err := s.db.Query(query)
	if err != nil {
		fmt.Fprintf(s.out, "Error: %v\n", err)
		if s.timer {
			fmt.Fprintf(s.out, "Time: %.3fs\n", time.Since(start).Seconds())
		}
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(s.out, "Error: %v\n", err)
		return
	}

	var resultRows [][]string
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Fprintf(s.out, "Error: %v\n", err)
			return
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = formatValue(v)
		}
		resultRows = append(resultRows, row)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(s.out, "Error: %v\n", err)
		return
	}

	if len(cols) > 0 {
		printResult(s.out, cols, resultRows, s.mode)
	}

	if s.timer {
		fmt.Fprintf(s.out, "Time: %.3fs\n", time.Since(start).Seconds())
	}
}

// execStatement runs DML/DDL via db.Exec and reports rows affected.
func (s *shell) execStatement(query string, start time.Time) {
	result, err := s.db.Exec(query)
	if err != nil {
		fmt.Fprintf(s.out, "Error: %v\n", err)
		if s.timer {
			fmt.Fprintf(s.out, "Time: %.3fs\n", time.Since(start).Seconds())
		}
		return
	}

	n, err := result.RowsAffected()
	if err == nil && n > 0 {
		fmt.Fprintf(s.out, "%d row(s) affected\n", n)
	}

	if s.timer {
		fmt.Fprintf(s.out, "Time: %.3fs\n", time.Since(start).Seconds())
	}
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case float64:
		// Trim trailing zeros for cleaner output.
		s := fmt.Sprintf("%g", val)
		return s
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (s *shell) dotCommand(cmd string) {
	fields := splitDotCommand(cmd)
	if len(fields) == 0 {
		return
	}
	switch fields[0] {
	case ".quit", ".exit":
		os.Exit(0)

	case ".help":
		s.printHelp()

	case ".mode":
		if len(fields) < 2 {
			fmt.Fprintf(s.out, "current output mode: %s\n", s.modeName())
			return
		}
		switch fields[1] {
		case "table":
			s.mode = modeTable
		case "csv":
			s.mode = modeCSV
		default:
			fmt.Fprintf(s.out, "Error: unknown mode %q (choose: table, csv)\n", fields[1])
		}

	case ".timer":
		if len(fields) < 2 {
			fmt.Fprintf(s.out, "timer: %v\n", onOff(s.timer))
			return
		}
		switch fields[1] {
		case "on":
			s.timer = true
		case "off":
			s.timer = false
		default:
			fmt.Fprintf(s.out, "Error: unknown timer setting %q (choose: on, off)\n", fields[1])
		}

	case ".tables":
		s.exec(`SELECT name FROM "minisql_schema" WHERE type = 1 AND name != 'minisql_schema' ORDER BY name`)

	case ".schema":
		var query string
		if len(fields) < 2 {
			query = `SELECT sql FROM "minisql_schema" WHERE type = 1 AND name != 'minisql_schema' ORDER BY name`
		} else {
			query = fmt.Sprintf(`SELECT sql FROM "minisql_schema" WHERE name = %s AND type = 1`, quoteString(fields[1]))
		}
		s.printDDL(query)

	default:
		fmt.Fprintf(s.out, "Error: unknown dot command %q — try .help\n", fields[0])
	}
}

// printDDL runs query (which must SELECT a single text column) and prints each
// value as raw text, which is the natural display for CREATE statements.
func (s *shell) printDDL(query string) {
	rows, err := s.db.Query(query)
	if err != nil {
		fmt.Fprintf(s.out, "Error: %v\n", err)
		return
	}
	defer rows.Close()
	first := true
	for rows.Next() {
		var ddl string
		if err := rows.Scan(&ddl); err != nil {
			fmt.Fprintf(s.out, "Error: %v\n", err)
			return
		}
		if !first {
			fmt.Fprintln(s.out)
		}
		first = false
		clean := strings.TrimRight(strings.TrimSpace(ddl), ";")
		fmt.Fprintln(s.out, clean+";")
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(s.out, "Error: %v\n", err)
	}
}

func (s *shell) printHelp() {
	fmt.Fprint(s.out, `Dot commands (single-line only):
  .help              Show this message
  .tables            List user tables
  .schema [table]    Show CREATE statement(s)
  .mode MODE         Set output mode: table (default), csv
  .timer on|off      Toggle query timing
  .quit / .exit      Exit the shell

SQL statements are terminated with a semicolon (;).
Multi-line statements are supported.
`)
}

func (s *shell) modeName() string {
	switch s.mode {
	case modeCSV:
		return "csv"
	default:
		return "table"
	}
}

// splitDotCommand splits a dot command respecting single-quoted strings.
func splitDotCommand(cmd string) []string {
	var fields []string
	var cur strings.Builder
	inQuote := false
	for _, r := range cmd {
		switch {
		case r == '\'' && !inQuote:
			inQuote = true
		case r == '\'' && inQuote:
			inQuote = false
		case unicode.IsSpace(r) && !inQuote:
			if cur.Len() > 0 {
				fields = append(fields, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		fields = append(fields, cur.String())
	}
	return fields
}

// quoteString wraps s in single quotes, escaping internal single quotes.
func quoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}
