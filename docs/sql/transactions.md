# Transactions

## Overview

MiniSQL uses two concurrency models:

- **Write transactions** — Optimistic Concurrency Control (OCC). One writer at a time; conflicts detected at commit.
- **Read-only transactions** — MVCC snapshot isolation. Multiple concurrent readers; always see a consistent point-in-time snapshot.

---

## Explicit transactions

```sql
BEGIN;

INSERT INTO accounts (id, balance) VALUES (1, 1000);
UPDATE accounts SET balance = balance - 100 WHERE id = 1;
UPDATE accounts SET balance = balance + 100 WHERE id = 2;

COMMIT;
```

Roll back if something goes wrong:

```sql
BEGIN;

UPDATE accounts SET balance = balance - 100 WHERE id = 1;

-- Something failed — undo everything
ROLLBACK;
```

---

## Transactions in Go

### Basic transaction

```go
tx, err := db.Begin()
if err != nil {
    return err
}
defer tx.Rollback() // safe to call even after Commit

_, err = tx.Exec(
    `UPDATE accounts SET balance = balance - ? WHERE id = ?`,
    100, fromID,
)
if err != nil {
    return err
}

_, err = tx.Exec(
    `UPDATE accounts SET balance = balance + ? WHERE id = ?`,
    100, toID,
)
if err != nil {
    return err
}

return tx.Commit()
```

### Retry on OCC conflict

Write transactions use optimistic concurrency control. When two concurrent writers modify the same pages, one receives `ErrTxConflict` at commit time and must retry:

```go
import minisqlErrors "github.com/RichardKnop/minisql/errors"

for {
    tx, err := db.Begin()
    if err != nil {
        return err
    }

    _, err = tx.Exec(
        `UPDATE accounts SET balance = balance - 100 WHERE id = 1`,
    )
    if err != nil {
        tx.Rollback()
        return err
    }

    if err := tx.Commit(); err != nil {
        tx.Rollback()
        if errors.Is(err, minisqlErrors.ErrTxConflict) {
            continue // retry
        }
        return err
    }
    break
}
```

### Read-only transaction

Every `SELECT` runs automatically in a snapshot-isolated read-only transaction. To run multiple SELECTs against the same snapshot, use `db.BeginTx` with `ReadOnly: true`:

```go
tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
if err != nil {
    return err
}
defer tx.Rollback()

var balance1, balance2 int64
tx.QueryRow(`SELECT balance FROM accounts WHERE id = 1`).Scan(&balance1)
tx.QueryRow(`SELECT balance FROM accounts WHERE id = 2`).Scan(&balance2)

// balance1 + balance2 is consistent — both rows come from the same snapshot
tx.Commit()
```

---

## Isolation guarantees

### Readers

Read-only transactions provide **snapshot isolation**:

- No dirty reads — a reader never sees uncommitted data.
- No non-repeatable reads — re-reading the same row within a snapshot returns the same value.
- No phantom reads — the set of rows matching a query is stable within a snapshot.
- Readers never block writers; writers never block readers.

### Writers

Write transactions provide **serialisability** via OCC:

- Only one write transaction commits at a time.
- Page versions are recorded in a read-set when first accessed.
- At commit, the engine checks that none of those pages were modified by a concurrent transaction.
- Conflict → `ErrTxConflict` → retry.
- Early conflict detection also happens in `ModifyPage` to fail fast.

---

## Autocommit

When you call `db.Exec` or `db.Query` outside a transaction, each statement runs in its own implicit transaction with autocommit. This is fine for single-statement operations:

```go
// Each of these runs in its own implicit transaction
db.Exec(`INSERT INTO users (email) VALUES (?)`, email)
db.Query(`SELECT * FROM users`)
```

For multi-statement operations that must be atomic, always use an explicit transaction.

---

## WAL durability

Transactions are written to the WAL file before the commit returns. Durability can be tuned with `PRAGMA synchronous`:

```sql
PRAGMA synchronous = full;   -- fsync WAL on every commit (safest)
PRAGMA synchronous = normal; -- fsync WAL periodically (default)
PRAGMA synchronous = off;    -- no fsync (fastest, least durable)
```

See [PRAGMA](explain.md#pragma) for full details.
