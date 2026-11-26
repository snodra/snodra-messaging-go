package sql

import (
	"context"
	stdSQL "database/sql"
	"embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	snodra "github.com/snodra/snodra-messaging-go"
)

// SchemaVersion is the schema version this add-on reads and writes. Versions are additive: code
// that knows version n accepts a database at version n or later.
const SchemaVersion int64 = 1

// The embedded reference files. They are the published schema version 1 files, byte for byte;
// schema_test.go pins their SHA-256 values.
const (
	schemaPostgresPath = "schema/postgresql/V1__snodra.sql"
	schemaMySQLPath    = "schema/mysql/V1__snodra.sql"
)

//go:embed schema/postgresql/V1__snodra.sql schema/mysql/V1__snodra.sql
var schemaFiles embed.FS

// The schema lock that serialises applying the schema between instances of every Snodra library:
// PostgreSQL's session advisory lock 0x534E44525343484D ("SNDRSCHM") and MySQL's named lock.
const (
	pgSchemaLockKey  int64 = 0x534E44525343484D
	mysqlSchemaLock        = "snodra_schema"
	schemaLockWait         = 60 * time.Second
	pgSchemaLockPoll       = 200 * time.Millisecond
)

// schemaFor returns the path and the bytes of driver's schema version 1.
func schemaFor(driver Driver) (path string, script []byte, err error) {
	switch driver {
	case Postgres:
		path = schemaPostgresPath
	case MySQL:
		path = schemaMySQLPath
	default:
		return "", nil, fmt.Errorf("the SQL message store: unknown driver %q", driver)
	}
	script, err = schemaFiles.ReadFile(path)
	if err != nil {
		return "", nil, fmt.Errorf("the SQL message store: cannot read its embedded schema: %w", err)
	}
	return path, script, nil
}

// InitSchema checks the server and applies schema version 1 unless the database has it or later,
// under the shared database lock so instances starting together apply it once. It implements
// snodra.StoreLifecycle.
func (s *Store) InitSchema(ctx context.Context, _ *snodra.Config) error {
	if s.closed.Load() {
		return fmt.Errorf("the SQL message store (%s) is closed", s.driver)
	}
	if err := s.verifyServer(ctx); err != nil {
		return err
	}
	version, err := s.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if version >= SchemaVersion {
		return nil
	}
	return s.applySchema(ctx)
}

// CheckSchema checks the server and fails unless the database has schema version 1 or later: the
// startup path of a service that applies the schema itself. It implements snodra.StoreLifecycle.
func (s *Store) CheckSchema(ctx context.Context) error {
	if s.closed.Load() {
		return fmt.Errorf("the SQL message store (%s) is closed", s.driver)
	}
	if err := s.verifyServer(ctx); err != nil {
		return err
	}
	version, err := s.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if version < SchemaVersion {
		path, _, _ := schemaFor(s.driver)
		return fmt.Errorf("the SQL message store's schema is at version %d, it needs %d or later: apply %s (and the versions after it), or set SNODRA_STORE_INIT_SCHEMA=true", version, SchemaVersion, path)
	}
	return nil
}

// verifyServer fails unless the pool has at least two connections and reaches a server this
// store runs on: PostgreSQL 13 or later; MySQL 8.0.17 or later (not MariaDB) with a strict
// sql_mode, so the database refuses rather than truncates a value.
func (s *Store) verifyServer(ctx context.Context) error {
	if max := s.db.Stats().MaxOpenConnections; max > 0 && max < 2 {
		return fmt.Errorf("the SQL message store needs a connection pool of at least 2 (this one has %d): a write that commits on its own inside the service's transaction takes a second connection", max)
	}
	if s.driver == Postgres {
		var product, num, version string
		err := s.db.QueryRowContext(ctx,
			"SELECT version() AS product, current_setting('server_version_num') AS num, current_setting('server_version') AS version").
			Scan(&product, &num, &version)
		if err != nil {
			return fmt.Errorf("the SQL message store cannot check the database server: %w", storeError(err))
		}
		return checkPostgres(product, num, version)
	}
	var version string
	var mode stdSQL.NullString
	err := s.db.QueryRowContext(ctx,
		"SELECT VERSION() AS version, @@SESSION.sql_mode AS mode").Scan(&version, &mode)
	if err != nil {
		return fmt.Errorf("the SQL message store cannot check the database server: %w", storeError(err))
	}
	return checkMySQL(version, mode.String)
}

// checkPostgres accepts PostgreSQL itself (not a server speaking its protocol, such as
// CockroachDB), 13 or later.
func checkPostgres(product, num, version string) error {
	if !strings.HasPrefix(product, "PostgreSQL") {
		return fmt.Errorf("the SQL message store supports PostgreSQL 13 or later, MySQL 8.0.17 or later; this server is %s", product)
	}
	if number, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64); err != nil || number < 130000 {
		return fmt.Errorf("the SQL message store needs PostgreSQL 13.0 or later; this server is PostgreSQL %s", version)
	}
	return nil
}

// checkMySQL accepts MySQL 8.0.17 or later (not MariaDB) with a strict sql_mode.
func checkMySQL(version, mode string) error {
	if strings.Contains(strings.ToLower(version), "mariadb") {
		return fmt.Errorf("the SQL message store does not support MariaDB yet (MySQL %s): it supports PostgreSQL 13 or later, MySQL 8.0.17 or later", version)
	}
	numbers := versionNumbers(version)
	if numbers[0] < 8 || (numbers[0] == 8 && numbers[1] == 0 && numbers[2] < 17) {
		return fmt.Errorf("the SQL message store needs MySQL 8.0.17 or later; this server is MySQL %s", version)
	}
	if !strings.Contains(mode, "STRICT_TRANS_TABLES") && !strings.Contains(mode, "STRICT_ALL_TABLES") {
		return fmt.Errorf("the SQL message store needs a strict sql_mode on MySQL (STRICT_TRANS_TABLES or STRICT_ALL_TABLES), so a value too long is refused rather than truncated; this session's sql_mode is %q", mode)
	}
	return nil
}

// versionNumbers is the first three numbers of a version text such as "8.0.36" or
// "17.7 (Debian 17.7-3)"; missing ones are 0.
func versionNumbers(version string) [3]int {
	var numbers [3]int
	field := 0
	current := -1
	for _, c := range version {
		if c >= '0' && c <= '9' {
			if current < 0 {
				current = 0
			}
			current = current*10 + int(c-'0')
			continue
		}
		if current >= 0 {
			if field < len(numbers) {
				numbers[field] = current
			}
			field++
			current = -1
		}
	}
	if current >= 0 && field < len(numbers) {
		numbers[field] = current
	}
	return numbers
}

// schemaVersion is the highest version in snodra_schema, 0 when there is no such table yet. A
// failure to read it (a missing right, say) is reported as it is, never as version 0.
func (s *Store) schemaVersion(ctx context.Context) (int64, error) {
	query := "SELECT CAST(MAX(version) AS BIGINT) AS v FROM snodra_schema"
	if s.driver == MySQL {
		query = "SELECT CAST(MAX(version) AS SIGNED) AS v FROM snodra_schema"
	}
	var version stdSQL.NullInt64
	err := s.db.QueryRowContext(ctx, query).Scan(&version)
	if err != nil {
		if isMissingTable(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("the SQL message store cannot read the schema version: %w", storeError(err))
	}
	if !version.Valid {
		return 0, nil
	}
	return version.Int64, nil
}

// applySchema applies schema version 1 under the shared lock. On PostgreSQL the whole script runs
// in one transaction; MySQL commits each DDL statement and the script is idempotent.
func (s *Store) applySchema(ctx context.Context) error {
	path, script, err := schemaFor(s.driver)
	if err != nil {
		return err
	}
	// A dedicated pool of one connection holds the session lock: closing it closes that
	// connection, so a lock can never stay behind in the service's pool.
	lockDB, err := stdSQL.Open(s.driverName(), s.dsn)
	if err != nil {
		return fmt.Errorf("the SQL message store cannot open a connection to apply its schema: %w", err)
	}
	lockDB.SetMaxOpenConns(1)
	defer func() { _ = lockDB.Close() }()
	conn, err := lockDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("the SQL message store cannot apply its schema: %w", storeError(err))
	}
	defer func() { _ = conn.Close() }()

	if err := s.acquireSchemaLock(ctx, conn); err != nil {
		return err
	}
	defer s.releaseSchemaLock(conn)

	// Another instance may have applied it while this one waited.
	version, err := s.schemaVersion(ctx)
	if err != nil {
		return err
	}
	if version >= SchemaVersion {
		return nil
	}

	refused := func(cause error) error {
		return fmt.Errorf("the SQL message store could not apply its schema (%s): apply that file yourself and set SNODRA_STORE_INIT_SCHEMA=false, or grant the service's database user the right to create tables. Cause: %w", path, cause)
	}
	statements := splitSQL(string(script))
	if len(statements) == 0 {
		return refused(errors.New("the embedded schema holds no statement"))
	}
	if s.driver == Postgres {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return refused(storeError(err))
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				_ = tx.Rollback()
				return refused(storeError(err))
			}
		}
		if err := tx.Commit(); err != nil {
			return refused(storeError(err))
		}
		return nil
	}
	for _, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return refused(storeError(err))
		}
	}
	return nil
}

// acquireSchemaLock takes the shared schema lock, waiting at most schemaLockWait.
func (s *Store) acquireSchemaLock(ctx context.Context, conn *stdSQL.Conn) error {
	deadline := time.Now().Add(schemaLockWait)
	timedOut := func() error {
		return fmt.Errorf("the SQL message store timed out after %s waiting for another instance to apply the schema", schemaLockWait)
	}
	if s.driver == MySQL {
		var got stdSQL.NullInt64
		err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", mysqlSchemaLock, int64(schemaLockWait/time.Second)).Scan(&got)
		if err != nil {
			return fmt.Errorf("the SQL message store cannot take the schema lock: %w", storeError(err))
		}
		if !got.Valid || got.Int64 != 1 {
			return timedOut()
		}
		return nil
	}
	for {
		var got bool
		if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", pgSchemaLockKey).Scan(&got); err != nil {
			return fmt.Errorf("the SQL message store cannot take the schema lock: %w", storeError(err))
		}
		if got {
			return nil
		}
		if !time.Now().Before(deadline) {
			return timedOut()
		}
		wait := time.NewTimer(pgSchemaLockPoll)
		select {
		case <-ctx.Done():
			wait.Stop()
			return ctx.Err()
		case <-wait.C:
		}
	}
}

// releaseSchemaLock releases the lock on its connection; a failure is ignored because closing the
// dedicated pool closes the session, which releases it anyway.
func (s *Store) releaseSchemaLock(conn *stdSQL.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.driver == MySQL {
		_, _ = conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", mysqlSchemaLock)
		return
	}
	_, _ = conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", pgSchemaLockKey)
}

// splitSQL splits a schema script into its statements, dropping comments. It understands single
// and double quotes (with their doubled-quote escape), MySQL backticks and backslash escapes in
// quoted strings; semicolons inside them do not end a statement.
func splitSQL(script string) []string {
	var statements []string
	var statement strings.Builder
	for i := 0; i < len(script); {
		switch c := script[i]; {
		case c == '-' && i+1 < len(script) && script[i+1] == '-':
			for i < len(script) && script[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(script) && script[i+1] == '*':
			i += 2
			for i+1 < len(script) && (script[i] != '*' || script[i+1] != '/') {
				i++
			}
			if i+1 < len(script) {
				i += 2
			} else {
				i = len(script)
			}
			statement.WriteByte(' ')
		case c == '\'' || c == '"' || c == '`':
			quote := c
			statement.WriteByte(c)
			i++
			for i < len(script) {
				statement.WriteByte(script[i])
				if script[i] == quote {
					if i+1 < len(script) && script[i+1] == quote {
						statement.WriteByte(script[i+1])
						i += 2
						continue
					}
					i++
					break
				}
				if script[i] == '\\' && quote != '`' {
					i++
					if i < len(script) {
						statement.WriteByte(script[i])
						i++
					}
					continue
				}
				i++
			}
		case c == ';':
			if text := strings.TrimSpace(statement.String()); text != "" {
				statements = append(statements, text)
			}
			statement.Reset()
			i++
		default:
			statement.WriteByte(c)
			i++
		}
	}
	if text := strings.TrimSpace(statement.String()); text != "" {
		statements = append(statements, text)
	}
	return statements
}
