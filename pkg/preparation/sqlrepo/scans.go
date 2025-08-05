package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/storacha/guppy/pkg/preparation/scans"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sqlrepo/util"
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

var _ scans.Repo = (*repo)(nil)

// CreateScan creates a new scan in the repository with the given upload ID.
func (r *repo) CreateScan(ctx context.Context, uploadID id.UploadID) (*scanmodel.Scan, error) {
	scan, err := scanmodel.NewScan(uploadID)
	if err != nil {
		return nil, err
	}

	insertQuery := `
		INSERT INTO scans (
			id,
			upload_id,
			root_id,
			created_at,
			updated_at,
			state,
			error_message
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	err = scanmodel.WriteScanToDatabase(
		scan,
		func(
			id id.ScanID,
			uploadID id.UploadID,
			rootID *id.FSEntryID,
			createdAt,
			updatedAt time.Time,
			state scanmodel.ScanState,
			errorMessage *string) error {
			result, err := r.db.ExecContext(
				ctx,
				insertQuery,
				id,
				uploadID,
				Null(rootID),
				createdAt.Unix(),
				updatedAt.Unix(),
				state,
				NullString(errorMessage),
			)
			if err != nil {
				return fmt.Errorf("failed to insert scan: %w", err)
			}
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			}
			if rowsAffected == 0 {
				return fmt.Errorf("no scan inserted")
			}
			return err
		},
	)
	if err != nil {
		return nil, err
	}

	return scan, nil
}

func (r *repo) GetScanByID(ctx context.Context, scanID id.ScanID) (*scanmodel.Scan, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT
			id,
			upload_id,
			root_id,
			created_at,
			updated_at,
			state,
			error_message
		FROM scans WHERE id = ?`, scanID,
	)
	scan, err := scanmodel.ReadScanFromDatabase(func(
		id *id.ScanID,
		uploadID *id.UploadID,
		rootID **id.FSEntryID,
		createdAt *time.Time,
		updatedAt *time.Time,
		state *scanmodel.ScanState,
		errorMessage **string) error {
		return row.Scan(
			id,
			uploadID,
			rootID,
			util.TimestampScanner(createdAt),
			util.TimestampScanner(updatedAt),
			state,
			errorMessage,
		)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return scan, nil
}

// FindOrCreateFile finds or creates a file entry in the repository with the given parameters.
// If the file already exists, it returns the existing file and false.
// If the file does not exist, it creates a new file entry and returns it along with true.
func (r *repo) FindOrCreateFile(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID) (*scanmodel.File, bool, error) {
	if mode.IsDir() {
		return nil, false, errors.New("cannot create a file with directory mode")
	}
	entry, err := r.findFSEntry(ctx, path, lastModified, mode, size, checksum, sourceID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find file entry: %w", err)
	}
	if entry != nil {
		// File already exists, return it
		if file, ok := entry.(*scanmodel.File); ok {
			return file, false, nil
		}
		return nil, false, errors.New("found entry is not a file")
	}

	newfile, err := scanmodel.NewFile(path, lastModified, mode, size, checksum, sourceID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to make new file entry: %w", err)
	}

	err = r.createFSEntry(ctx, newfile)

	if err != nil {
		return nil, false, fmt.Errorf("failed to persist new file entry: %w", err)
	}

	return newfile, true, nil
}

// FindOrCreateDirectory finds or creates a directory entry in the repository with the given parameters.
// If the directory already exists, it returns the existing directory and false.
// If the directory does not exist, it creates a new directory entry and returns it along with true.
func (r *repo) FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID id.SourceID) (*scanmodel.Directory, bool, error) {
	log.Debugf("Finding or creating directory: %s", path)
	if !mode.IsDir() {
		return nil, false, errors.New("cannot create a directory with file mode")
	}
	entry, err := r.findFSEntry(ctx, path, lastModified, mode, 0, checksum, sourceID) // size is not used for directories
	if err != nil {
		return nil, false, fmt.Errorf("failed to find directory entry: %w", err)
	}
	if entry != nil {
		if dir, ok := entry.(*scanmodel.Directory); ok {
			// Directory already exists, return it
			return dir, false, nil
		}
		return nil, false, errors.New("found entry is not a directory")
	}

	newdir, err := scanmodel.NewDirectory(path, lastModified, mode, checksum, sourceID)
	if err != nil {
		return nil, false, fmt.Errorf("failed to make new directory entry: %w", err)
	}

	err = r.createFSEntry(ctx, newdir)
	if err != nil {
		return nil, false, fmt.Errorf("failed to persist new directory entry: %w", err)
	}

	log.Debugf("Created new directory %s: %s", path, newdir.ID())
	return newdir, true, nil
}

// CreateDirectoryChildren links a directory to its children in the repository.
func (r *repo) CreateDirectoryChildren(ctx context.Context, parent *scanmodel.Directory, children []scanmodel.FSEntry) error {
	if len(children) == 0 {
		return nil // No children to create
	}

	return r.WithTx(ctx, func(tx *sql.Tx) error {
		insertQuery := `
			INSERT INTO directory_children (directory_id, child_id)
			VALUES ($1, $2)
		`

		for _, child := range children {
			_, err := tx.ExecContext(ctx, insertQuery, parent.ID(), child.ID())
			if err != nil {
				return fmt.Errorf("failed to insert directory child relationship for parent %s, child %s: %w", parent.ID(), child.ID(), err)
			}
		}

		return nil
	})
}

// DirectoryChildren retrieves the children of a directory from the repository.
func (r *repo) DirectoryChildren(ctx context.Context, dir *scanmodel.Directory) ([]scanmodel.FSEntry, error) {
	query := `
		SELECT fse.id, fse.path, fse.last_modified, fse.mode, fse.size, fse.checksum, fse.source_id
		FROM directory_children dc
		JOIN fs_entries fse ON dc.child_id = fse.id
		WHERE dc.directory_id = $1
	`
	rows, err := r.db.QueryContext(ctx, query, dir.ID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []scanmodel.FSEntry
	for rows.Next() {
		entry, err := scanmodel.ReadFSEntryFromDatabase(func(
			id *id.FSEntryID,
			path *string,
			lastModified *time.Time,
			mode *fs.FileMode,
			size *uint64,
			checksum *[]byte,
			sourceID *id.SourceID,
		) error {
			return rows.Scan(
				id,
				path,
				util.TimestampScanner(lastModified),
				mode,
				size,
				checksum,
				sourceID,
			)
		})
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// UpdateScan updates the given scan in the repository.
func (r *repo) UpdateScan(ctx context.Context, scan *scanmodel.Scan) error {
	query := `
		UPDATE scans
		SET upload_id = $2, root_id = $3, created_at = $4, updated_at = $5, state = $6, error_message = $7
		WHERE id = $1
	`

	return scanmodel.WriteScanToDatabase(scan, func(id id.ScanID, uploadID id.UploadID, rootID *id.FSEntryID, createdAt, updatedAt time.Time, state scanmodel.ScanState, errorMessage *string) error {
		_, err := r.db.ExecContext(ctx, query, id, uploadID, Null(rootID), createdAt.Unix(), updatedAt.Unix(), state, NullString(errorMessage))
		return err
	})
}

// GetFileByID retrieves a file by its unique ID from the repository.
func (r *repo) GetFileByID(ctx context.Context, fileID id.FSEntryID) (*scanmodel.File, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, path, last_modified, mode, size, checksum, source_id FROM fs_entries WHERE id = ?`, fileID,
	)
	file, err := scanmodel.ReadFSEntryFromDatabase(func(id *id.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *id.SourceID) error {
		return row.Scan(id, path, util.TimestampScanner(lastModified), mode, size, checksum, sourceID)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if f, ok := file.(*scanmodel.File); ok {
		return f, nil
	}
	return nil, errors.New("found entry is not a file")
}

func (r *repo) findFSEntry(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID id.SourceID) (scanmodel.FSEntry, error) {
	query := `
		SELECT id, path, last_modified, mode, size, checksum, source_id
		FROM fs_entries
		WHERE path = $1
		  AND last_modified = $2
		  AND mode = $3
		  AND size = $4
		  AND checksum = $5
		  AND source_id = $6
	`
	row := r.db.QueryRowContext(
		ctx,
		query,
		path,
		lastModified.Unix(),
		mode,
		size,
		checksum,
		sourceID,
	)
	entry, err := scanmodel.ReadFSEntryFromDatabase(func(
		id *id.FSEntryID,
		path *string,
		lastModified *time.Time,
		mode *fs.FileMode,
		size *uint64,
		checksum *[]byte,
		sourceID *id.SourceID,
	) error {
		return row.Scan(
			id,
			path,
			util.TimestampScanner(lastModified),
			mode,
			size,
			checksum,
			sourceID,
		)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return entry, err
}

func (r *repo) createFSEntry(ctx context.Context, entry scanmodel.FSEntry) error {
	insertQuery := `
		INSERT INTO fs_entries (id, path, last_modified, mode, size, checksum, source_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	return scanmodel.WriteFSEntryToDatabase(
		entry,
		func(
			id id.FSEntryID,
			path string,
			lastModified time.Time,
			mode fs.FileMode,
			size uint64,
			checksum []byte,
			sourceID id.SourceID,
		) error {
			_, err := r.db.ExecContext(
				ctx,
				insertQuery,
				id,
				path,
				lastModified.Unix(),
				mode,
				size,
				checksum,
				sourceID,
			)
			return err
		})
}
