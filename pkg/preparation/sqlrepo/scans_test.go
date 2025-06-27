package sqlrepo_test

import (
	"context"
	"database/sql"
	_ "embed"
	"testing"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schema string

// createTestDB creates a temporary SQLite database for testing. It returns the
// database connection, a cleanup function, and any error encountered.
func createTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err, "failed to open in-memory SQLite database")

	t.Cleanup(func() {
		db.Close()
	})

	_, err = db.ExecContext(t.Context(), schema)
	require.NoError(t, err, "failed to execute schema")

	// Disable foreign key checks to simplify test.
	_, err = db.ExecContext(t.Context(), "PRAGMA foreign_keys = OFF;")
	require.NoError(t, err, "failed to disable foreign keys")

	return db
}

func TestCreateScan(t *testing.T) {
	t.Run("with an upload ID", func(t *testing.T) {
		db := createTestDB(t)
		repo := sqlrepo.New(db)
		uploadID := uuid.New()

		scan, err := repo.CreateScan(t.Context(), uploadID)
		require.NoError(t, err)

		readScan, err := repo.GetScanByID(t.Context(), scan.ID())

		require.NoError(t, err)
		require.Equal(t, scan, readScan)
	})

	t.Run("with a nil upload ID", func(t *testing.T) {
		repo := sqlrepo.New(createTestDB(t))
		_, err := repo.CreateScan(t.Context(), uuid.Nil)
		require.ErrorContains(t, err, "update id cannot be empty")
	})

	t.Run("when the DB fails", func(t *testing.T) {
		repo := sqlrepo.New(createTestDB(t))
		uploadID := uuid.New()

		// Simulate a DB failure by cancelling the context before the operation.
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := repo.CreateScan(ctx, uploadID)
		require.ErrorContains(t, err, "context canceled")
	})
}
