package sqlrepo_test

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
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

//go:embed schema.sql
var schema string

// createTestDB creates a temporary SQLite database for testing. It returns the
// database connection, a cleanup function, and any error encountered.
func createTestDB(ctx context.Context) (*sql.DB, func(), error) {
	dir, err := os.MkdirTemp("", "sqlrepo_test")
	if err != nil {
		return nil, nil, err
	}

	fn := filepath.Join(dir, "db")

	db, err := sql.Open("sqlite", fn)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	if _, err = db.ExecContext(ctx, schema); err != nil {
		cleanup()
		return nil, nil, err
	}

	return db, cleanup, nil
}

func TestCreateScan(t *testing.T) {
	db, cleanup, err := createTestDB(t.Context())
	require.NoError(t, err)
	defer cleanup()

	// Disable foreign key checks to simplify test.
	db.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF;")

	repo := sqlrepo.New(db)

	uploadID := uuid.New()

	scan, err := repo.CreateScan(t.Context(), uploadID)
	require.NoError(t, err)

	rows, err := db.QueryContext(t.Context(), `
	  SELECT 
			id,
			upload_id,
			root_id,
			created_at,
			updated_at,
			state,
			error_message
		FROM scans
	`)
	require.NoError(t, err)

	rows.Next()
	readScan, err := model.ReadScanFromDatabase(func(
		id,
		uploadID *types.SourceID,
		rootID **types.FSEntryID,
		createdAt,
		updatedAt *time.Time,
		state *model.ScanState,
		errorMessage **string,
	) error {

		err := rows.Scan(
			id,
			uploadID,
			rootID,
			timestampScanner(createdAt),
			timestampScanner(updatedAt),
			state,
			errorMessage,
		)
		if err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, scan, readScan)
}
