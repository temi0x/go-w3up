package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"time"

	"github.com/storacha/guppy/pkg/preparation/scans"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sources"
	sourcemodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

func NullString(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{Valid: false}
	}
	return sql.NullString{String: *s, Valid: true}
}

func Null[T any](v *T) sql.Null[T] {
	if v == nil {
		return sql.Null[T]{Valid: false}
	}
	return sql.Null[T]{Valid: true, V: *v}
}

// Repo is the interface that combines uploads, sources, and scans repositories.
type Repo interface {
	uploads.Repo
	sources.Repo
	scans.Repo
}

// New creates a new Repo instance with the given database connection.
func New(db *sql.DB) Repo {
	return &repo{db: db}
}

type repo struct {
	db *sql.DB
}

var _ sources.Repo = (*repo)(nil)

// NewRepo creates a new sources repository with the given database connection.
func NewRepo(db *sql.DB) sources.Repo {
	return &repo{db: db}
}

// CreateSource creates a new source in the repository with the given name, path, and options.
func (r *repo) CreateSource(ctx context.Context, name string, path string, options ...sourcemodel.SourceOption) (*sourcemodel.Source, error) {
	src, err := sourcemodel.NewSource(name, path, options...)
	if err != nil {
		return nil, err
	}
	_, err = r.db.ExecContext(ctx,
		`INSERT INTO sources (id, name, created_at, updated_at, kind, path, connection_params) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		src.ID(), src.Name(), src.CreatedAt(), src.UpdatedAt(), src.Kind(), src.Path(), src.ConnectionParams(),
	)
	if err != nil {
		return nil, err
	}
	return src, nil
}

// GetSourceByID retrieves a source by its unique ID from the repository.
func (r *repo) GetSourceByID(ctx context.Context, sourceID types.SourceID) (*sourcemodel.Source, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, name, created_at, updated_at, kind, path, connection_params FROM sources WHERE id = ?`, sourceID,
	)
	src, err := sourcemodel.ReadSourceFromDatabase(func(id *types.SourceID, name *string, createdAt, updatedAt *time.Time, kind *sourcemodel.SourceKind, path *string, connectionParams *sourcemodel.ConnectionParams) error {
		var ca, ua sql.NullTime
		err := row.Scan(id, name, &ca, &ua, kind, path, connectionParams)
		if err != nil {
			return err
		}
		if ca.Valid {
			*createdAt = ca.Time
		}
		if ua.Valid {
			*updatedAt = ua.Time
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return src, err
}

// GetSourceByName retrieves a source by its name from the repository.
func (r *repo) GetSourceByName(ctx context.Context, name string) (*sourcemodel.Source, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, name, created_at, updated_at, kind, path, connection_params FROM sources WHERE name = ?`, name,
	)
	src, err := sourcemodel.ReadSourceFromDatabase(func(id *types.SourceID, name *string, createdAt, updatedAt *time.Time, kind *sourcemodel.SourceKind, path *string, connectionParams *sourcemodel.ConnectionParams) error {
		var ca, ua sql.NullTime
		err := row.Scan(id, name, &ca, &ua, kind, path, connectionParams)
		if err != nil {
			return err
		}
		if ca.Valid {
			*createdAt = ca.Time
		}
		if ua.Valid {
			*updatedAt = ua.Time
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return src, err
}

// UpdateSource updates the given source in the repository.
func (r *repo) UpdateSource(ctx context.Context, src *sourcemodel.Source) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE sources SET name = ?, updated_at = ?, kind = ?, path = ?, connection_params = ? WHERE id = ?`,
		src.Name(), src.UpdatedAt(), src.Kind(), src.Path(), src.ConnectionParams(), src.ID(),
	)
	return err
}

var _ uploads.Repo = (*repo)(nil)

// CreateUpload creates a new upload in the repository with the given name and options.
func (r *repo) CreateUpload(name string, options ...uploadmodel.UploadOption) (*uploadmodel.Upload, error) {
	upload, err := uploadmodel.NewUpload(name, options...)
	if err != nil {
		return nil, err
	}
	_, err = r.db.Exec(
		`INSERT INTO uploads (id, name, created_at, shardSize) VALUES (?, ?, ?, ?)`,
		upload.ID(), upload.Name(), upload.CreatedAt(), upload.ShardSize(),
	)
	if err != nil {
		return nil, err
	}
	return upload, nil
}

// AddSourceToUpload adds a source to an upload in the repository.
func (r *repo) AddSourceToUpload(uploadID types.UploadID, sourceID types.SourceID) error {
	_, err := r.db.Exec(
		`INSERT INTO upload_sources (upload_id, source_id, created_at) VALUES (?, ?, ?)`,
		uploadID, sourceID, time.Now(),
	)
	return err
}

// DeleteUpload deletes an upload from the repository.
func (r *repo) DeleteUpload(uploadID types.UploadID) error {
	_, err := r.db.Exec(
		`DELETE FROM uploads WHERE id = ?`,
		uploadID,
	)
	if err != nil {
		return err
	}
	// Also delete associated upload sources
	_, err = r.db.Exec(
		`DELETE FROM upload_sources WHERE upload_id = ?`,
		uploadID,
	)
	return err
}

// RemoveSourceFromUpload removes a source from an upload in the repository.
func (r *repo) RemoveSourceFromUpload(uploadID types.UploadID, sourceID types.SourceID) error {
	_, err := r.db.Exec(
		`DELETE FROM upload_sources WHERE upload_id = ? AND source_id = ?`,
		uploadID, sourceID,
	)
	return err
}

// GetUploadByID retrieves an upload by its unique ID from the repository.
func (r *repo) GetUploadByID(uploadID types.UploadID) (*uploadmodel.Upload, error) {
	row := r.db.QueryRow(
		`SELECT id, name, created_at, shardSize FROM uploads WHERE id = ?`, uploadID,
	)
	upload, err := uploadmodel.ReadUploadFromDatabase(func(id *types.UploadID, name *string, createdAt *time.Time, shardSize *uint64) error {
		var cs sql.NullInt64
		err := row.Scan(id, name, createdAt, &cs)
		if err != nil {
			return err
		}
		if cs.Valid {
			*shardSize = uint64(cs.Int64)
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return upload, err
}

// GetUploadByName retrieves an upload by its name from the repository.
func (r *repo) GetUploadByName(name string) (*uploadmodel.Upload, error) {
	row := r.db.QueryRow(
		`SELECT id, name, created_at, shardSize FROM uploads WHERE name = ?`, name,
	)
	upload, err := uploadmodel.ReadUploadFromDatabase(func(id *types.UploadID, name *string, createdAt *time.Time, shardSize *uint64) error {
		var cs sql.NullInt64
		err := row.Scan(id, name, createdAt, &cs)
		if err != nil {
			return err
		}
		if cs.Valid {
			*shardSize = uint64(cs.Int64)
		}
		return nil
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return upload, err
}

// ListUploadSources lists all sources associated with a given upload ID.
func (r *repo) ListUploadSources(uploadID types.UploadID) ([]*sourcemodel.Source, error) {
	rows, err := r.db.Query(
		`SELECT s.id, s.name, s.created_at, s.updated_at, s.kind, s.path, s.connection_params
		FROM upload_sources us
		JOIN sources s ON us.source_id = s.id
		WHERE us.upload_id = ?`, uploadID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []*sourcemodel.Source
	for rows.Next() {
		src, err := sourcemodel.ReadSourceFromDatabase(func(id *types.SourceID, name *string, createdAt, updatedAt *time.Time, kind *sourcemodel.SourceKind, path *string, connectionParams *sourcemodel.ConnectionParams) error {
			var ca, ua sql.NullTime
			err := rows.Scan(id, name, &ca, &ua, kind, path, connectionParams)
			if err != nil {
				return err
			}
			if ca.Valid {
				*createdAt = ca.Time
			}
			if ua.Valid {
				*updatedAt = ua.Time
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		if src == nil {
			continue // Skip nil sources
		}
		sources = append(sources, src)
	}
	return sources, nil
}

// ListUploads lists all uploads in the repository.
func (r *repo) ListUploads() ([]*uploadmodel.Upload, error) {
	rows, err := r.db.Query(
		`SELECT id, name, created_at, shardSize FROM uploads`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uploads []*uploadmodel.Upload
	for rows.Next() {
		upload, err := uploadmodel.ReadUploadFromDatabase(func(id *types.UploadID, name *string, createdAt *time.Time, shardSize *uint64) error {
			var cs sql.NullInt64
			err := rows.Scan(id, name, createdAt, &cs)
			if err != nil {
				return err
			}
			if cs.Valid {
				*shardSize = uint64(cs.Int64)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, upload)
	}
	return uploads, nil
}

// CreateScan creates a new scan in the repository with the given source ID and upload ID.
func (r *repo) CreateScan(ctx context.Context, sourceID types.SourceID, uploadID types.UploadID) (*scanmodel.Scan, error) {

	scan, err := scanmodel.NewScan(sourceID, uploadID)
	if err != nil {
		return nil, err
	}

	insertQuery := `
		INSERT INTO scans (id, source_id, upload_id, root_id, created_at, updated_at, state, error_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	err = scanmodel.WriteScanToDatabase(scan, func(id scanmodel.ScanID, uploadID types.UploadID, sourceID types.SourceID, rootID *scanmodel.FSEntryID, createdAt, updatedAt time.Time, state scanmodel.ScanState, errorMessage *string) error {
		_, err := r.db.ExecContext(ctx, insertQuery, id, sourceID, uploadID, Null(rootID), createdAt, updatedAt, state, NullString(errorMessage))
		return err
	})

	return scan, nil
}

// UpdateScan updates the given scan in the repository.
func (r *repo) UpdateScan(ctx context.Context, scan *scanmodel.Scan) error {
	query := `
		UPDATE scans
		SET source_id = $2, upload_id = $3, root_id = $4, created_at = $5, updated_at = $6, state = $7, error_message = $8
		WHERE id = $1
	`

	return scanmodel.WriteScanToDatabase(scan, func(id scanmodel.ScanID, uploadID types.UploadID, sourceID types.SourceID, rootID *scanmodel.FSEntryID, createdAt, updatedAt time.Time, state scanmodel.ScanState, errorMessage *string) error {
		_, err := r.db.ExecContext(ctx, query, id, sourceID, uploadID, Null(rootID), createdAt, updatedAt, state, NullString(errorMessage))
		return err
	})
}

// FindOrCreateFile finds or creates a file entry in the repository with the given parameters.
// If the file already exists, it returns the existing file and false.
// If the file does not exist, it creates a new file entry and returns it along with true.
func (r *repo) FindOrCreateFile(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) (*scanmodel.File, bool, error) {

	entry, err := r.findFSEntry(ctx, path, lastModified, mode, size, checksum, sourceID)
	if err != nil {
		return nil, false, err
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
		return nil, false, err
	}

	err = r.createFSEntry(ctx, newfile)

	if err != nil {
		return nil, false, err
	}

	return newfile, true, nil
}

// FindOrCreateDirectory finds or creates a directory entry in the repository with the given parameters.
// If the directory already exists, it returns the existing directory and false.
// If the directory does not exist, it creates a new directory entry and returns it along with true.
func (r *repo) FindOrCreateDirectory(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, checksum []byte, sourceID types.SourceID) (*scanmodel.Directory, bool, error) {
	entry, err := r.findFSEntry(ctx, path, lastModified, mode, 0, checksum, sourceID) // size is not used for directories
	if err != nil {
		return nil, false, err
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
		return nil, false, err
	}

	err = r.createFSEntry(ctx, newdir)
	if err != nil {
		return nil, false, err
	}

	return newdir, true, nil
}

func (r *repo) findFSEntry(ctx context.Context, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) (scanmodel.FSEntry, error) {
	query := `
		SELECT id, path, last_modified, mode, size, checksum, source_id
		FROM files
		WHERE path = $1 AND last_modified = $2 AND mode = $3 AND size = $4 AND checksum = $5 AND source_id = $6
	`
	row := r.db.QueryRowContext(ctx, query, path, lastModified, mode, size, checksum, sourceID)
	entry, err := scanmodel.ReadFSEntryFromDatabase(func(id *scanmodel.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error {
		return row.Scan(id, path, lastModified, mode, size, checksum, sourceID)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return entry, err
}

func (r *repo) createFSEntry(ctx context.Context, entry scanmodel.FSEntry) error {
	insertQuery := `
		INSERT INTO files (id, path, last_modified, mode, size, checksum, source_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	return scanmodel.WriteFSEntryToDatabase(entry, func(id scanmodel.FSEntryID, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) error {
		_, err := r.db.ExecContext(ctx, insertQuery, id, path, lastModified, mode, size, checksum, sourceID)
		return err
	})
}

// CreateDirectoryChildren links a directory to its children in the repository.
func (r *repo) CreateDirectoryChildren(ctx context.Context, parent *scanmodel.Directory, children []scanmodel.FSEntry) error {
	insertQuery := `
		INSERT INTO directory_children (directory_id, child_id)
		VALUES ($1, $2)
	`

	for _, child := range children {
		_, err := r.db.ExecContext(ctx, insertQuery, parent.ID(), child.ID())
		if err != nil {
			return err
		}
	}

	return nil
}

// DirectoryChildren retrieves the children of a directory from the repository.
func (r *repo) DirectoryChildren(ctx context.Context, dir *scanmodel.Directory) ([]scanmodel.FSEntry, error) {
	query := `
		SELECT f.id, f.path, f.last_modified, f.mode, f.size, f.checksum, f.source_id
		FROM directory_children dc
		JOIN files f ON dc.child_id = f.id
		WHERE dc.directory_id = $1
	`
	rows, err := r.db.QueryContext(ctx, query, dir.ID())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []scanmodel.FSEntry
	for rows.Next() {
		entry, err := scanmodel.ReadFSEntryFromDatabase(func(id *scanmodel.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error {
			return rows.Scan(id, path, lastModified, mode, size, checksum, sourceID)
		})
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// GetFileByID retrieves a file by its unique ID from the repository.
func (r *repo) GetFileByID(ctx context.Context, fileID scanmodel.FSEntryID) (*scanmodel.File, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, path, last_modified, mode, size, checksum, source_id FROM files WHERE id = ?`, fileID,
	)
	file, err := scanmodel.ReadFSEntryFromDatabase(func(id *scanmodel.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error {
		return row.Scan(id, path, lastModified, mode, size, checksum, sourceID)
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
