# Password Functions

MiniSQL provides built-in functions for hashing and verifying passwords using industry-standard algorithms. All functions propagate `NULL`: if any argument is `NULL` the result is `NULL`.

## ARGON2ID_HASH(password)

Hashes a plaintext password using [Argon2id](https://www.rfc-editor.org/rfc/rfc9106) and returns a self-describing string in [PHC format](https://github.com/P-H-C/phc-string-format/blob/master/phc-sf-spec.md).

```sql
-- Hash at INSERT time
INSERT INTO users (email, password)
VALUES (?, ARGON2ID_HASH(?));

-- Read back
SELECT password FROM users WHERE email = 'alice@example.com';
-- '$argon2id$v=19$m=65536,t=3,p=4$<base64-salt>$<base64-hash>'
```

### Parameters

| Parameter  | Type   | Description                 |
|------------|--------|-----------------------------|
| `password` | `TEXT` | Plaintext password to hash  |

### Return value

`TEXT` — PHC-format string, e.g.:

```
$argon2id$v=19$m=65536,t=3,p=4$dGVzdHNhbHQ$...
```

The string is fully self-describing: the parameters and random salt are embedded, so the original can be verified with `ARGON2ID_VERIFY` without storing anything extra.

### Default parameters

| Parameter   | Value  | Meaning                            |
|-------------|--------|------------------------------------|
| Memory      | 64 MiB | Working-set size for the hash      |
| Iterations  | 3      | Number of passes over memory       |
| Parallelism | 4      | Degree of parallelism              |
| Key length  | 32 B   | Output hash length                 |
| Salt length | 16 B   | Randomly generated per call        |

These meet the [OWASP minimum recommendations](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html) for Argon2id.

### Each call produces a unique hash

Because the salt is randomly generated on every call, the same password yields a different hash string each time. Use `ARGON2ID_VERIFY` to check passwords — never compare hash strings directly.

---

## ARGON2ID_VERIFY(password, hash)

Returns `1` if `password` matches `hash`, or `0` if it does not.

```sql
-- Verify during login
SELECT ARGON2ID_VERIFY(?, password) FROM users WHERE email = ?;
-- 1 (correct password) or 0 (wrong password)

-- Authenticate and return the email in one query
SELECT email FROM users WHERE ARGON2ID_VERIFY(?, password) = 1 AND email = ?;
```

### Parameters

| Parameter  | Type   | Description                              |
|------------|--------|------------------------------------------|
| `password` | `TEXT` | Plaintext password to check              |
| `hash`     | `TEXT` | PHC-format hash produced by `ARGON2ID_HASH` |

### Return value

`INT8` — `1` if the password matches, `0` if it does not.

An error is returned if `hash` is not a valid Argon2id PHC string.

---

## BCRYPT_HASH(password [, cost])

Hashes a plaintext password using [bcrypt](https://www.usenix.org/conference/1999-usenix-annual-technical-conference/future-adaptable-password-scheme) and returns the bcrypt-encoded string.

```sql
-- Hash with default cost (12)
INSERT INTO admins (email, password)
VALUES (?, BCRYPT_HASH(?));

-- Hash with explicit cost
INSERT INTO admins (email, password)
VALUES (?, BCRYPT_HASH(?, 12));
```

### Parameters

| Parameter  | Type   | Default | Description                                |
|------------|--------|---------|--------------------------------------------|
| `password` | `TEXT` | —       | Plaintext password to hash                 |
| `cost`     | `INT8` | `12`    | Work factor (4–31). Higher = slower = more secure |

### Return value

`TEXT` — bcrypt-encoded string, e.g.:

```
$2a$12$R9h/cIPz0gi.URNNX3kh2OPST9/PgBkqquzi.Ss7KIUgO2t0jWMUW
```

The cost is embedded in the string. The default cost of **12** meets [OWASP recommendations](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html) for bcrypt.

### Each call produces a unique hash

Like Argon2id, bcrypt embeds a random salt so the same password produces a different string each time. Use `BCRYPT_VERIFY` for verification.

---

## BCRYPT_VERIFY(password, hash)

Returns `1` if `password` matches the bcrypt `hash`, or `0` if it does not.

```sql
SELECT BCRYPT_VERIFY(?, password) FROM admins WHERE email = ?;
-- 1 (correct) or 0 (wrong)
```

### Parameters

| Parameter  | Type   | Description                            |
|------------|--------|----------------------------------------|
| `password` | `TEXT` | Plaintext password to check            |
| `hash`     | `TEXT` | bcrypt hash produced by `BCRYPT_HASH`  |

### Return value

`INT8` — `1` if the password matches, `0` if it does not.

An error is returned if `hash` is not a valid bcrypt string.

---

## Common patterns

### Store a hashed password on INSERT

```sql
INSERT INTO users (email, password)
VALUES (?, ARGON2ID_HASH(?));
-- args: "alice@example.com", "p@ssw0rd"
```

### Verify a password in a WHERE clause

```sql
-- Returns the user row only when the password is correct
SELECT id, email
FROM users
WHERE email = ?
  AND ARGON2ID_VERIFY(?, password) = 1;
-- args: "alice@example.com", "p@ssw0rd"
```

### Update a password

```sql
UPDATE users
SET password = ARGON2ID_HASH(?)
WHERE email = ?;
-- args: "newpassword", "alice@example.com"
```

### Hash on INSERT, verify in SELECT

```sql
-- Application registers a user
INSERT INTO accounts (username, password)
VALUES ('bob', BCRYPT_HASH('s3cr3t', 12));

-- Application logs in
SELECT username
FROM accounts
WHERE username = 'bob'
  AND BCRYPT_VERIFY('s3cr3t', password) = 1;
```

---

## NULL propagation

If any argument is `NULL`, the result is `NULL`:

```sql
SELECT ARGON2ID_HASH(NULL);   -- NULL
SELECT ARGON2ID_VERIFY(NULL, hash) FROM t;  -- NULL
SELECT BCRYPT_HASH(NULL);     -- NULL
SELECT BCRYPT_VERIFY(NULL, hash) FROM t;    -- NULL
```

---

## Choosing an algorithm

| Algorithm  | Recommended for          | Notes                                             |
|------------|--------------------------|---------------------------------------------------|
| Argon2id   | New applications         | Winner of Password Hashing Competition; memory-hard |
| bcrypt     | Compatibility / legacy   | Widely supported; 72-byte input limit             |

Argon2id is the modern choice. Use bcrypt only when you need compatibility with an existing bcrypt hash store or a library that only supports bcrypt.

**Do not** use these functions to hash data other than passwords (e.g., API keys, tokens). For those use cases, a keyed MAC (`HMAC-SHA256`) or a fast hash (`SHA-256`) is more appropriate.
