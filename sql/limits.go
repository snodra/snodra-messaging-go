package sql

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	snodra "github.com/snodra/snodra-messaging-go"
)

// The bounds of the shared schema version 1, in characters as VARCHAR(n) counts them. A value
// over its bound, or holding U+0000, is refused before anything is written; the database never
// truncates it.
const (
	maxProcessor = 64
	maxSource    = 255
	maxSegment   = 128
	maxPosition  = 128
	maxName      = 255
	maxID        = 255
	// freeTextBytes is how much of an error message the store keeps: 16 KiB of UTF-8, never cut
	// inside a character.
	freeTextBytes = 16 * 1024
	// purgeBatch is how many rows one purge statement deletes.
	purgeBatch = 1000
)

// millis is t at the millisecond precision the shared layout keeps: the store truncates every
// time it binds instead of leaving the database to round it.
func millis(t time.Time) time.Time {
	return t.Truncate(time.Millisecond)
}

// invalidf is a bad argument: errors.Is finds snodra.ErrInvalid.
func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: the SQL message store %s", snodra.ErrInvalid, fmt.Sprintf(format, args...))
}

// checkText refuses value longer than max characters (max < 0 means no length bound) or holding
// U+0000, naming the field and never the value: it may be personal data.
func checkText(field, value string, max int) error {
	if !utf8.ValidString(value) {
		return invalidf("refuses text that is not UTF-8 in %s", field)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return invalidf("refuses the character U+0000 in %s", field)
	}
	if max >= 0 {
		if length := utf8.RuneCountInString(value); length > max {
			return invalidf("keeps %s of at most %d characters; this one has %d", field, max, length)
		}
	}
	return nil
}

// checkOptionalText is checkText for a value that may be absent (empty means none here).
func checkOptionalText(field, value string, max int) error {
	if value == "" {
		return nil
	}
	return checkText(field, value, max)
}

// cutFreeText cuts text to at most freeTextBytes bytes of UTF-8, never inside a character.
func cutFreeText(text string) string {
	if len(text) <= freeTextBytes {
		return text
	}
	end := freeTextBytes
	for end > 0 && !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end]
}

// sha256Hex is the lowercase hex SHA-256 of value's UTF-8 bytes, the value of every *_hash
// column.
func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// positionNumber reads a position as the number it stands for when it is a canonical decimal
// int64 (the position_num column), else reports false: positions of the two kinds never compare.
func positionNumber(position string) (int64, bool) {
	number, err := strconv.ParseInt(position, 10, 64)
	if err != nil || strconv.FormatInt(number, 10) != position {
		return 0, false
	}
	return number, true
}

// headersJSON is headers as the JSON object of strings a headers column holds, its keys sorted.
// U+0000 is refused in a name and in a value.
func headersJSON(headers map[string]string) (string, error) {
	if headers == nil {
		headers = map[string]string{}
	}
	for name, value := range headers {
		if err := checkText("a header name", name, -1); err != nil {
			return "", err
		}
		if err := checkText("a header value", value, -1); err != nil {
			return "", err
		}
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return "", invalidf("cannot encode a headers object: %v", err)
	}
	return string(encoded), nil
}

// headersFromJSON reads a headers column: an object of strings; a value that is not a string is
// read as its JSON text, as the shared layout says.
func headersFromJSON(text string) (map[string]string, error) {
	if strings.TrimSpace(text) == "" || strings.TrimSpace(text) == "null" {
		return map[string]string{}, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(text), &fields); err != nil {
		return nil, fmt.Errorf("the SQL message store cannot read a headers column: %w", err)
	}
	headers := make(map[string]string, len(fields))
	for name, raw := range fields {
		var value string
		if strings.TrimSpace(string(raw)) == "null" {
			value = "null"
		} else if err := json.Unmarshal(raw, &value); err != nil {
			value = string(raw)
		}
		headers[name] = value
	}
	return headers, nil
}

// payloadHasNUL reports whether a JSON object holds U+0000 in a string or a name: PostgreSQL's
// jsonb cannot keep one, and both databases refuse the same payloads.
func payloadHasNUL(payload []byte) bool {
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return false // the core's payload check already refused it
	}
	return valueHasNUL(value)
}

func valueHasNUL(value any) bool {
	switch v := value.(type) {
	case string:
		return strings.IndexByte(v, 0) >= 0
	case []any:
		for _, item := range v {
			if valueHasNUL(item) {
				return true
			}
		}
	case map[string]any:
		for name, item := range v {
			if strings.IndexByte(name, 0) >= 0 || valueHasNUL(item) {
				return true
			}
		}
	}
	return false
}

// checkOutboxEntry applies the outbox contract to entry and returns its headers as the column
// holds them.
func checkOutboxEntry(entry snodra.OutboxEntry) (string, error) {
	if err := snodra.CheckOutboxPayload(entry.Type, entry.Payload); err != nil {
		return "", err
	}
	if err := checkText("an outbox entry's id", entry.ID, maxID); err != nil {
		return "", err
	}
	if err := checkText("an outbox entry's type", entry.Type, maxName); err != nil {
		return "", err
	}
	if entry.Key != nil {
		if err := checkText("an outbox entry's key", *entry.Key, -1); err != nil {
			return "", err
		}
	}
	headers, err := headersJSON(entry.Headers)
	if err != nil {
		return "", err
	}
	if payloadHasNUL(entry.Payload) {
		return "", invalidf("refuses the character U+0000 in the payload of event %q", entry.Type)
	}
	return headers, nil
}

// isUniqueViolation reports whether err is a duplicate key: PostgreSQL 23505, MySQL 1062 and
// 1586.
func isUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code == "23505"
	}
	var my *mysql.MySQLError
	if errors.As(err, &my) {
		return my.Number == 1062 || my.Number == 1586
	}
	return false
}

// isMissingTable reports whether err is "no such table" (PostgreSQL 42P01, MySQL 1146).
func isMissingTable(err error) bool {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.Code == "42P01"
	}
	var my *mysql.MySQLError
	if errors.As(err, &my) {
		return my.Number == 1146
	}
	return false
}

// storeError is what a database error means to the store: a duplicate key, a serialization
// failure, a deadlock or a lock not available is snodra.ErrConflict (a retry of the whole
// attempt resolves it); every other error is returned as it is.
func storeError(err error) error {
	if err == nil {
		return nil
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "23505", "40001", "40P01", "55P03":
			return fmt.Errorf("%w: %s", snodra.ErrConflict, pg.Message)
		}
		return err
	}
	var my *mysql.MySQLError
	if errors.As(err, &my) {
		switch my.Number {
		case 1062, 1586, 1213, 1205, 3572:
			return fmt.Errorf("%w: %s", snodra.ErrConflict, my.Message)
		}
	}
	return err
}
