# minisql

[![CI Status](https://github.com/RichardKnop/minisql/actions/workflows/go.yml/badge.svg)](https://github.com/RichardKnop/minisql/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/RichardKnop/minisql)](https://goreportcard.com/report/github.com/RichardKnop/minisql)
[![Donate Bitcoin](https://img.shields.io/badge/donate-bitcoin-orange.svg)](https://richardknop.github.io/donate/)

MiniSQL is an embedded single-file SQL database written in Go, inspired by SQLite but borrowing ideas from PostgreSQL.

**[Documentation](https://richardknop.github.io/minisql/)**

## Features

- Pure Go — zero CGO
- MVCC snapshot isolation for reads; serialised single-writer for writes
- B+ tree storage with WAL and crash recovery
- Parallel full-table scan
- JSON and UUID as native column types
- Built-in full-text search (inverted index + `MATCH` / `TS_RANK`)
- Built-in JSON inverted index (`JSON_CONTAINS`)
- Vector similarity search (`VECTOR(n)` column + `VEC_L2` / `VEC_COSINE` + HNSW ANN index)
- Transparent AES-256-CTR page encryption with HKDF key derivation

## Installation

```go
import _ "github.com/RichardKnop/minisql"
```

```go
db, err := sql.Open("minisql", "./my.db")
if err != nil {
    log.Fatal(err)
}
db.SetMaxOpenConns(1) // required — see docs
defer db.Close()
```

See the [Getting Started](https://richardknop.github.io/minisql/getting-started/) guide and [Connection](https://richardknop.github.io/minisql/connection/) reference for full details.

## Development

### Generate mocks

MiniSQL uses [mockery](https://github.com/vektra/mockery) to generate interface mocks:

```sh
go install github.com/vektra/mockery/v3@v3.6.1
mockery
```

### Run tests

```sh
LOG_LEVEL=info go test ./... -count=1
```

Setting `LOG_LEVEL=info` suppresses debug output and makes test failures easier to read.

### Fuzz testing

MiniSQL uses Go's built-in fuzzer (`go test -fuzz`) to find parser bugs and
storage corruption issues that structured unit tests are unlikely to reach.

**Run the parser fuzzer** (stops after 60 s; adjust `-fuzztime` as needed):

```sh
go test -fuzz=FuzzParser -fuzztime=60s ./internal/parser/
```

**Run seeds only** (fast, no mutation — safe for CI):

```sh
go test -run=FuzzParser ./internal/parser/
```

Corpus entries that previously found real bugs live in
`internal/parser/testdata/fuzz/FuzzParser/` and are automatically replayed as
ordinary unit tests on every `go test` run. When the fuzzer discovers a new
crash it writes the minimised input to that directory; commit it so the fix is
covered by CI forever.

### Run linter

```sh
make lint
```

### Benchmarks & profiling

See [benchmarks/README.md](benchmarks/README.md) and [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

## Acknowledgements

- [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/parts/part1.html)
- [go-sqldb](https://github.com/auxten/go-sqldb)
- [sqlparser](https://github.com/marianogappa/sqlparser)
- [SQLite file format docs](https://www.sqlite.org/fileformat2.html)
- [C++ implementation of B+ tree](https://github.com/sayef/bplus-tree)
- [Mastering PostgreSQL GIN Indexes](https://medium.com/@vedantthakkar1003/mastering-postgresql-gin-indexes-the-ultimate-guide-to-faster-jsonb-array-and-full-text-search-f1f8ec3e67af)
