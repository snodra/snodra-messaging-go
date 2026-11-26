package sql

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"strings"
	"testing"
)

// The published SHA-256 values of the shared schema version 1.
const (
	publishedPostgresSHA256 = "882bff968f8f2e13e021178f5772d124e6352e3efb3ec104f7e8e5fed254a012"
	publishedMySQLSHA256    = "ec77259da977f07ba83abe8ba9104dcf7a9e292760c87ae712639d786671557d"
)

// The schema files are the shared layout's reference files, byte for byte: services written with
// any Snodra library share one database. A change to either file is a change to the specification.
func TestTheSchemaFilesAreThePublishedOnes(t *testing.T) {
	for _, c := range []struct {
		path string
		want string
	}{
		{schemaPostgresPath, publishedPostgresSHA256},
		{schemaMySQLPath, publishedMySQLSHA256},
	} {
		onDisk, err := os.ReadFile(c.path)
		if err != nil {
			t.Fatalf("%s: %v", c.path, err)
		}
		embedded, err := schemaFiles.ReadFile(c.path)
		if err != nil {
			t.Fatalf("%s embedded: %v", c.path, err)
		}
		if !bytes.Equal(onDisk, embedded) {
			t.Fatalf("%s: the embedded bytes differ from the file", c.path)
		}
		sum := sha256.Sum256(onDisk)
		if got := hex.EncodeToString(sum[:]); got != c.want {
			t.Fatalf("%s: SHA-256 %s, want the published %s", c.path, got, c.want)
		}
		if bytes.HasPrefix(onDisk, []byte{0xEF, 0xBB, 0xBF}) {
			t.Fatalf("%s starts with a byte order mark", c.path)
		}
		if bytes.Contains(onDisk, []byte("\r\n")) {
			t.Fatalf("%s holds CRLF line endings", c.path)
		}
		if !bytes.HasSuffix(onDisk, []byte("\n")) {
			t.Fatalf("%s does not end with a newline", c.path)
		}
	}
}

// The schema lock is the same in every library: PostgreSQL's session advisory lock
// 0x534E44525343484D and MySQL's named lock.
func TestTheSchemaLockIsTheSharedOne(t *testing.T) {
	if pgSchemaLockKey != 6_002_810_473_706_375_245 {
		t.Fatalf("the PostgreSQL schema lock key is %d", pgSchemaLockKey)
	}
	if mysqlSchemaLock != "snodra_schema" {
		t.Fatalf("the MySQL schema lock is %q", mysqlSchemaLock)
	}
	if SchemaVersion != 1 {
		t.Fatalf("the schema version is %d", SchemaVersion)
	}
}

// Every statement the files hold is found, comments and quoted semicolons do not split one, and
// the version is recorded last.
func TestTheSchemaFilesSplitIntoTheirStatements(t *testing.T) {
	for _, path := range []string{schemaPostgresPath, schemaMySQLPath} {
		script, err := schemaFiles.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		statements := splitSQL(string(script))
		if len(statements) < 13 {
			t.Fatalf("%s: only %d statements", path, len(statements))
		}
		for i, statement := range statements {
			if strings.TrimSpace(statement) == "" {
				t.Fatalf("%s: statement %d is empty", path, i)
			}
			if strings.Contains(statement, "--") || strings.Contains(statement, "/*") {
				t.Fatalf("%s: statement %d keeps a comment: %.80s", path, i, statement)
			}
			if strings.Count(statement, ";") != 0 {
				t.Fatalf("%s: statement %d keeps a semicolon: %.80s", path, i, statement)
			}
		}
		last := statements[len(statements)-1]
		if !strings.HasPrefix(last, "INSERT INTO snodra_schema (version) VALUES (1)") {
			t.Fatalf("%s: the last statement is %.120s", path, last)
		}
	}
}

func TestSplitSQLKeepsQuotedSemicolonsAndDropsComments(t *testing.T) {
	script := "-- a comment; with a semicolon\n" +
		"CREATE TABLE \"a;b\" (x TEXT DEFAULT 'y;z'); /* another ; comment */\n" +
		"INSERT INTO t VALUES ('it''s; fine');\n" +
		"INSERT INTO t VALUES ('a\\'b; still quoted');"
	statements := splitSQL(script)
	if len(statements) != 3 {
		t.Fatalf("got %d statements: %q", len(statements), statements)
	}
	if !strings.Contains(statements[0], `"a;b"`) || !strings.Contains(statements[0], `'y;z'`) {
		t.Fatalf("the first statement is %q", statements[0])
	}
	if !strings.Contains(statements[1], `'it''s; fine'`) {
		t.Fatalf("the second statement is %q", statements[1])
	}
	if !strings.Contains(statements[2], `'a\'b; still quoted'`) {
		t.Fatalf("the third statement is %q", statements[2])
	}
	// A line comment inside a quoted string is text, not a comment.
	if got := splitSQL("INSERT INTO t VALUES ('-- x; y'); SELECT 1;"); len(got) != 2 {
		t.Fatalf("a quoted comment split: %q", got)
	}
}

func TestCheckPostgresAcceptsOnlyPostgreSQL13OrLater(t *testing.T) {
	if err := checkPostgres("PostgreSQL 17.7 (Debian 17.7-3.pgdg130+1)", "170007", "17.7"); err != nil {
		t.Fatalf("PostgreSQL 17.7: %v", err)
	}
	if err := checkPostgres("PostgreSQL 13.0 on x86_64", "130000", "13.0"); err != nil {
		t.Fatalf("PostgreSQL 13.0: %v", err)
	}
	for _, c := range []struct {
		product, num, version, want string
	}{
		{"PostgreSQL 12.18 on x86_64", "120018", "12.18", "needs PostgreSQL 13.0"},
		{"CockroachDB CCL v24.1.0 (x86_64-pc-linux-gnu)", "130000", "13.0", "this server is CockroachDB"},
	} {
		err := checkPostgres(c.product, c.num, c.version)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: %v, want %q", c.product, err, c.want)
		}
	}
}

func TestCheckMySQLAcceptsOnlyMySQL8017Strict(t *testing.T) {
	strict := "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
	for _, version := range []string{"8.4.7", "8.0.17", "9.1.0-commercial"} {
		if err := checkMySQL(version, strict); err != nil {
			t.Fatalf("MySQL %s: %v", version, err)
		}
	}
	if err := checkMySQL("8.0.17", "STRICT_ALL_TABLES"); err != nil {
		t.Fatalf("STRICT_ALL_TABLES: %v", err)
	}
	for _, c := range []struct {
		version, mode, want string
	}{
		{"8.0.16", strict, "needs MySQL 8.0.17"},
		{"5.7.44-log", strict, "needs MySQL 8.0.17"},
		{"10.11.6-MariaDB", strict, "does not support MariaDB"},
		{"8.4.7", "NO_ENGINE_SUBSTITUTION", "strict sql_mode"},
	} {
		err := checkMySQL(c.version, c.mode)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("MySQL %s (%q): %v, want %q", c.version, c.mode, err, c.want)
		}
	}
}
