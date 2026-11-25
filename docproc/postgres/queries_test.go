package postgres_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"gorm.io/gorm"
)

// TestQueriesSQL checks that every query in db/queries.sql is valid against
// db/schema.sql: column names exist, parameter types can be inferred, and
// the syntax parses. It does so with PREPARE inside a transaction that is
// rolled back, so no data changes.
func TestQueriesSQL(t *testing.T) {
	db := openTestDB(t)
	_, file, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "db", "queries.sql"))
	if err != nil {
		t.Fatal(err)
	}

	queries := splitNamedQueries(string(raw))
	if len(queries) < 10 {
		t.Fatalf("found only %d queries, the parser is probably wrong", len(queries))
	}
	for name, sql := range queries {
		t.Run(name, func(t *testing.T) {
			err := db.Transaction(func(tx *gorm.DB) error {
				return tx.Exec("PREPARE " + name + " AS " + sql).Error
			})
			if err != nil {
				t.Fatalf("PREPARE failed: %v\n%s", err, sql)
			}
		})
	}
}

var nameRe = regexp.MustCompile(`(?m)^-- name: (\S+)\n`)

// splitNamedQueries takes the first statement (up to ';') after each
// "-- name: x" marker, skipping comment lines in between.
func splitNamedQueries(src string) map[string]string {
	out := map[string]string{}
	locs := nameRe.FindAllStringSubmatchIndex(src, -1)
	for i, loc := range locs {
		end := len(src)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		var buf []string
		for _, line := range strings.Split(src[loc[1]:end], "\n") {
			if len(buf) == 0 && (strings.HasPrefix(strings.TrimSpace(line), "--") || strings.TrimSpace(line) == "") {
				continue
			}
			buf = append(buf, line)
			if strings.HasSuffix(strings.TrimSpace(line), ";") {
				break
			}
		}
		out[src[loc[2]:loc[3]]] = strings.TrimSuffix(strings.TrimSpace(strings.Join(buf, "\n")), ";")
	}
	return out
}
