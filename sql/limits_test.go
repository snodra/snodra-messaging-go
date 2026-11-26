package sql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	snodra "github.com/snodra/snodra-messaging-go"
)

func TestHashesAreTheLowercaseHexSHA256OfTheUTF8Bytes(t *testing.T) {
	cases := map[string]string{
		"":                              "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"processor:default:e-1":         "24ca69a5b6e5e53de85d1bd8f13651ed593283ecfe1638c5eb04d81bafade4ff",
		"conformance.Order|orderId=o-1": "83dc8dac44387920c04b298f2fa8a5685646f6b823ef12b3aefc8265260eb350",
		"é😀\x00x":                       "e8d673369c826a11be0cdbdaa662a86fc7edc0320aa1ccdd09b852aad8b400ca",
	}
	for value, want := range cases {
		if got := sha256Hex(value); got != want {
			t.Fatalf("sha256(%q) = %s, want %s", value, got, want)
		}
	}
}

func TestPositionsAreNumbersOnlyWhenCanonicalDecimals(t *testing.T) {
	for position, want := range map[string]int64{"41": 41, "-3": -3, "0": 0} {
		if got, ok := positionNumber(position); !ok || got != want {
			t.Fatalf("position %q: %d (%v), want %d", position, got, ok, want)
		}
	}
	for _, odd := range []string{"007", "+1", "", "x7", "99999999999999999999", " 1", "-0"} {
		if _, ok := positionNumber(odd); ok {
			t.Fatalf("position %q is a number", odd)
		}
	}
}

func TestBoundsCountCharactersAndNameTheFieldNotTheValue(t *testing.T) {
	if err := checkText("a processor's name", strings.Repeat("é", maxProcessor), maxProcessor); err != nil {
		t.Fatalf("at the bound: %v", err)
	}
	err := checkText("a processor's name", strings.Repeat("😀", maxProcessor+1), maxProcessor)
	if !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("over the bound: %v, want ErrInvalid", err)
	}
	if !strings.Contains(err.Error(), "65") || !strings.Contains(err.Error(), "64") {
		t.Fatalf("the refusal does not name the lengths: %v", err)
	}
	if err := checkText("an id", "a\x00b", -1); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("U+0000: %v, want ErrInvalid", err)
	}
	if err := checkText("an id", "a\xffb", -1); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("invalid UTF-8: %v, want ErrInvalid", err)
	}
	if err := checkOptionalText("a marker's processor", "", maxProcessor); err != nil {
		t.Fatalf("an absent value: %v", err)
	}
}

func TestFreeTextIsCutTo16KiBOnACharacterBoundary(t *testing.T) {
	if got := cutFreeText("short"); got != "short" {
		t.Fatalf("short: %q", got)
	}
	ascii := strings.Repeat("a", freeTextBytes+10)
	if got := cutFreeText(ascii); len(got) != freeTextBytes {
		t.Fatalf("ascii: %d bytes", len(got))
	}
	accents := "a" + strings.Repeat("é", freeTextBytes/2)
	cut := cutFreeText(accents)
	if len(cut) != freeTextBytes-1 || !strings.HasSuffix(cut, "é") {
		t.Fatalf("accents: %d bytes, ends %q", len(cut), cut[max(0, len(cut)-2):])
	}
	emoji := strings.Repeat("😀", freeTextBytes/4+1)
	if got := cutFreeText(emoji); len(got) != freeTextBytes {
		t.Fatalf("emoji: %d bytes", len(got))
	}
}

func TestHeadersAreAJSONObjectOfStringsReadBackAsWritten(t *testing.T) {
	headers := map[string]string{"traceparent": "00-x", "userId": ""}
	text, err := headersJSON(headers)
	if err != nil {
		t.Fatal(err)
	}
	if text != `{"traceparent":"00-x","userId":""}` {
		t.Fatalf("headers JSON %s", text)
	}
	read, err := headersFromJSON(text)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read, headers) {
		t.Fatalf("read %v, want %v", read, headers)
	}
	if _, err := headersJSON(map[string]string{"k": "a\x00"}); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("U+0000 in a header: %v", err)
	}
	read, err = headersFromJSON(`{"n":1,"s":"x","z":null}`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read, map[string]string{"n": "1", "s": "x", "z": "null"}) {
		t.Fatalf("non-string values: %v", read)
	}
	if read, err := headersFromJSON(""); err != nil || len(read) != 0 {
		t.Fatalf("empty: %v (%v)", read, err)
	}
}

func TestPayloadsRefuseNULInAStringOrName(t *testing.T) {
	for _, payload := range []string{`{"s":"a\u0000b"}`, `{"a\u0000":1}`, `{"a":[{"b":"\u0000"}]}`} {
		if !payloadHasNUL([]byte(payload)) {
			t.Fatalf("%s was accepted", payload)
		}
	}
	for _, payload := range []string{`{"s":"a\\u0000b"}`, `{"s":"é"}`, `{"n":1}`} {
		if payloadHasNUL([]byte(payload)) {
			t.Fatalf("%s was refused", payload)
		}
	}
}

func TestCheckOutboxEntryAppliesTheSharedContract(t *testing.T) {
	key := "k"
	good := snodra.OutboxEntry{
		ID:        "entry-1",
		Type:      "orders.Placed",
		Key:       &key,
		Headers:   map[string]string{snodra.HeaderCorrelationID: "c"},
		Payload:   []byte(`{"n":9223372036854775807}`),
		CreatedAt: nowAt(),
		Seq:       1,
	}
	if _, err := checkOutboxEntry(good); err != nil {
		t.Fatalf("a good entry: %v", err)
	}
	bad := good
	bad.Payload = []byte(`[1]`)
	if _, err := checkOutboxEntry(bad); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("an array payload: %v", err)
	}
	bad = good
	bad.Payload = []byte(`{"s":"a\u0000b"}`)
	if _, err := checkOutboxEntry(bad); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("a NUL in the payload: %v", err)
	}
	bad = good
	bad.ID = strings.Repeat("x", maxID+1)
	if _, err := checkOutboxEntry(bad); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("an over-long id: %v", err)
	}
	bad = good
	nulKey := "a\x00b"
	bad.Key = &nulKey
	if _, err := checkOutboxEntry(bad); !errors.Is(err, snodra.ErrInvalid) {
		t.Fatalf("a NUL key: %v", err)
	}
}

func TestDatabaseErrorsBecomeConflictsWhereARetryHelps(t *testing.T) {
	for _, err := range []error{
		&pgconn.PgError{Code: "23505", Message: "duplicate"},
		&pgconn.PgError{Code: "40001", Message: "serialization"},
		&pgconn.PgError{Code: "40P01", Message: "deadlock"},
		&pgconn.PgError{Code: "55P03", Message: "lock"},
		&mysql.MySQLError{Number: 1062, Message: "duplicate"},
		&mysql.MySQLError{Number: 1213, Message: "deadlock"},
		&mysql.MySQLError{Number: 1205, Message: "lock"},
		&mysql.MySQLError{Number: 3572, Message: "lock"},
	} {
		if got := storeError(err); !errors.Is(got, snodra.ErrConflict) {
			t.Fatalf("%v: %v, want ErrConflict", err, got)
		}
	}
	other := &pgconn.PgError{Code: "42P01", Message: "no table"}
	if got := storeError(other); !errors.Is(got, other) {
		t.Fatalf("another error: %v", got)
	}
	if !isMissingTable(other) {
		t.Fatal("42P01 is not a missing table")
	}
	if !isUniqueViolation(&mysql.MySQLError{Number: 1062}) {
		t.Fatal("1062 is not a duplicate")
	}
}

func TestOurQueriesGetPostgresNumberedPlaceholders(t *testing.T) {
	store := &Store{driver: Postgres}
	if got, want := store.bind("SELECT ? , ?"), "SELECT $1 , $2"; got != want {
		t.Fatalf("bind: %q, want %q", got, want)
	}
	store = &Store{driver: MySQL}
	if got := store.bind("SELECT ? , ?"); got != "SELECT ? , ?" {
		t.Fatalf("MySQL bind: %q", got)
	}
	if got := placeholders(3); got != "?, ?, ?" {
		t.Fatalf("placeholders: %q", got)
	}
}

// nowAt is a fixed instant the unit tests use.
func nowAt() time.Time { return time.UnixMilli(1_700_000_000_000) }
