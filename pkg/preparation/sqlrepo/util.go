package sqlrepo

import (
	"database/sql"
	"fmt"
	"time"
)

type tsScanner struct {
	dst *time.Time
}

var _ sql.Scanner = tsScanner{}

func (ts tsScanner) Scan(value any) error {
	if value == nil {
		*ts.dst = time.Time{}
		return nil
	}
	switch v := value.(type) {
	case int64:
		*ts.dst = time.Unix(v, 0).UTC()
	default:
		return fmt.Errorf("unsupported type for timestamp scanning: %T (%v)", v, v)
	}
	return nil
}

// timestampScanner returns a sql.Scanner that scans a timestamp (as an integer
// of Unix time in seconds) into the given time.Time pointer.
func timestampScanner(t *time.Time) tsScanner {
	return tsScanner{dst: t}
}
