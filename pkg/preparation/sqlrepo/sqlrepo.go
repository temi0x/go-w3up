package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"io/fs"
	"time"

	"github.com/storacha/guppy/pkg/preparation/configurations"
	configurationsmodel "github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/scans"
	scanmodel "github.com/storacha/guppy/pkg/preparation/scans/model"
	"github.com/storacha/guppy/pkg/preparation/sources"
	sourcemodel "github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
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
	configurations.Repo
	uploads.Repo
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
func (r *repo) CreateConfiguration(name string, options ...configurationsmodel.ConfigurationOption) (*configurationsmodel.Configuration, error) {
	configuration, err := configurationsmodel.NewConfiguration(name, options...)
	if err != nil {
		return nil, err
	}
	_, err = r.db.Exec(
		`INSERT INTO configurations (id, name, created_at, shard_size, block_size, links_per_node, use_hamt_directory_size) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		configuration.ID(), configuration.Name(), configuration.CreatedAt(), configuration.ShardSize(), configuration.BlockSize(), configuration.LinksPerNode(), configuration.UseHAMTDirectorySize(),
	)
	if err != nil {
		return nil, err
	}
	return configuration, nil
}

// AddSourceToConfiguration adds a source to a configuration in the repository.
func (r *repo) AddSourceToConfiguration(configurationID types.ConfigurationID, sourceID types.SourceID) error {
	_, err := r.db.Exec(
		`INSERT INTO configuration_sources (configuration_id, source_id) VALUES (?, ?)`,
		configurationID, sourceID,
	)
	return err
}

// DeleteConfiguration deletes a configuration from the repository.
func (r *repo) DeleteConfiguration(configurationID types.ConfigurationID) error {
	_, err := r.db.Exec(
		`DELETE FROM configurations WHERE id = ?`,
		configurationID,
	)
	if err != nil {
		return err
	}
	// Also delete associated configuration sources
	_, err = r.db.Exec(
		`DELETE FROM configuration_sources WHERE configuration_id = ?`,
		configurationID,
	)
	return err
}

// RemoveSourceFromConfiguration removes a source from a configuration in the repository.
func (r *repo) RemoveSourceFromConfiguration(configurationID types.ConfigurationID, sourceID types.SourceID) error {
	_, err := r.db.Exec(
		`DELETE FROM configuration_sources WHERE configuration_id = ? AND source_id = ?`,
		configurationID, sourceID,
	)
	return err
}

// GetConfigurationByID retrieves a configuration by its unique ID from the repository.
func (r *repo) GetConfigurationByID(configurationID types.ConfigurationID) (*configurationsmodel.Configuration, error) {
	row := r.db.QueryRow(
		`SELECT id, name, created_at, shard_size, block_size, links_per_node, use_hamt_directory_size FROM configurations WHERE id = ?`, configurationID,
	)
	configuration, err := configurationsmodel.ReadConfigurationFromDatabase(func(id *types.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64, blockSize *uint64, linksPerNode *uint64, useHAMTDirectorySize *uint64) error {
		return row.Scan(id, name, createdAt, shardSize, blockSize, linksPerNode, useHAMTDirectorySize)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return configuration, err
}

// GetConfigurationByName retrieves a configuration by its name from the repository.
func (r *repo) GetConfigurationByName(name string) (*configurationsmodel.Configuration, error) {
	row := r.db.QueryRow(
		`SELECT id, name, created_at, shard_size, block_size, links_per_node, use_hamt_directory_size FROM configurations WHERE name = ?`, name,
	)
	configuration, err := configurationsmodel.ReadConfigurationFromDatabase(func(id *types.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64, blockSize *uint64, linksPerNode *uint64, useHAMTDirectorySize *uint64) error {
		return row.Scan(id, name, createdAt, shardSize, blockSize, linksPerNode, useHAMTDirectorySize)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return configuration, err
}

// ListConfigurationSources lists all sources associated with a given configuration ID.
func (r *repo) ListConfigurationSources(configurationID types.ConfigurationID) ([]types.SourceID, error) {
	rows, err := r.db.Query(
		`SELECT cs.source_id
		FROM configuration_sources cs
		WHERE cs.configuration_id = ?`, configurationID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []types.SourceID
	for rows.Next() {
		var sourceID types.SourceID
		if err := rows.Scan(&sourceID); err != nil {
			return nil, err
		}
		sources = append(sources, sourceID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sources, nil
}

// ListConfigurations lists all configurations in the repository.
func (r *repo) ListConfigurations() ([]*configurationsmodel.Configuration, error) {
	rows, err := r.db.Query(
		`SELECT id, name, created_at, shard_size, block_size, links_per_node, use_hamt_directory_size FROM configurations`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configurations []*configurationsmodel.Configuration
	for rows.Next() {
		configuration, err := configurationsmodel.ReadConfigurationFromDatabase(func(id *types.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64, blockSize *uint64, linksPerNode *uint64, useHAMTDirectorySize *uint64) error {
			return rows.Scan(id, name, createdAt, shardSize, blockSize, linksPerNode, useHAMTDirectorySize)
		})
		if err != nil {
			return nil, err
		}
		if configuration == nil {
			continue
		}
		configurations = append(configurations, configuration)
	}
	return configurations, nil
}

// CreateScan creates a new scan in the repository with the given source ID and upload ID.
func (r *repo) CreateScan(ctx context.Context, uploadID types.UploadID) (*scanmodel.Scan, error) {

	scan, err := scanmodel.NewScan(uploadID)
	if err != nil {
		return nil, err
	}

	insertQuery := `
		INSERT INTO scans (id, source_id, upload_id, root_id, created_at, updated_at, state, error_message)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`

	err = scanmodel.WriteScanToDatabase(scan, func(id types.ScanID, uploadID types.UploadID, rootID *types.FSEntryID, createdAt, updatedAt time.Time, state scanmodel.ScanState, errorMessage *string) error {
		_, err := r.db.ExecContext(ctx, insertQuery, id, uploadID, Null(rootID), createdAt, updatedAt, state, NullString(errorMessage))
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

	return scanmodel.WriteScanToDatabase(scan, func(id types.ScanID, uploadID types.UploadID, rootID *types.FSEntryID, createdAt, updatedAt time.Time, state scanmodel.ScanState, errorMessage *string) error {
		_, err := r.db.ExecContext(ctx, query, id, uploadID, Null(rootID), createdAt, updatedAt, state, NullString(errorMessage))
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
		FROM fs_entries
		WHERE path = $1 AND last_modified = $2 AND mode = $3 AND size = $4 AND checksum = $5 AND source_id = $6
	`
	row := r.db.QueryRowContext(ctx, query, path, lastModified, mode, size, checksum, sourceID)
	entry, err := scanmodel.ReadFSEntryFromDatabase(func(id *types.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error {
		return row.Scan(id, path, lastModified, mode, size, checksum, sourceID)
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

	return scanmodel.WriteFSEntryToDatabase(entry, func(id types.FSEntryID, path string, lastModified time.Time, mode fs.FileMode, size uint64, checksum []byte, sourceID types.SourceID) error {
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
		entry, err := scanmodel.ReadFSEntryFromDatabase(func(id *types.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error {
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
func (r *repo) GetFileByID(ctx context.Context, fileID types.FSEntryID) (*scanmodel.File, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, path, last_modified, mode, size, checksum, source_id FROM fs_entries WHERE id = ?`, fileID,
	)
	file, err := scanmodel.ReadFSEntryFromDatabase(func(id *types.FSEntryID, path *string, lastModified *time.Time, mode *fs.FileMode, size *uint64, checksum *[]byte, sourceID *types.SourceID) error {
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

// GetSourceIDForUploadID retrieves the source ID associated with a given upload ID.
func (r *repo) GetSourceIDForUploadID(ctx context.Context, uploadID types.UploadID) (types.SourceID, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT source_id FROM uploads WHERE id = ?`, uploadID,
	)
	var sourceID types.SourceID
	err := row.Scan(&sourceID)
	if err != nil {
		return types.SourceID{}, err
	}
	return sourceID, nil
}

// GetUploadByID retrieves an upload by its unique ID from the repository.
func (r *repo) GetUploadByID(ctx context.Context, uploadID types.UploadID) (*uploadsmodel.Upload, error) {
	row := r.db.QueryRowContext(ctx,
		`SELECT id, configuration_id, source_id, created_at FROM uploads WHERE id = ?`, uploadID,
	)
	upload, err := uploadsmodel.ReadUploadFromDatabase(func(id *types.UploadID, configurationID *types.ConfigurationID, sourceID *types.SourceID, createdAt *time.Time) error {
		return row.Scan(id, configurationID, sourceID, createdAt)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return upload, err
}

// CreateUploads creates uploads for a given configuration and source IDs.
func (r *repo) CreateUploads(ctx context.Context, configurationID types.ConfigurationID, sourceIDs []types.SourceID) ([]*uploadsmodel.Upload, error) {
	var uploads []*uploadsmodel.Upload
	for _, sourceID := range sourceIDs {
		upload, err := uploadsmodel.NewUpload(configurationID, sourceID)
		if err != nil {
			return nil, err
		}
		_, err = r.db.ExecContext(ctx,
			`INSERT INTO uploads (id, configuration_id, source_id, created_at) VALUES (?, ?, ?, ?)`,
			upload.ID(), upload.ConfigurationID(), upload.SourceID(), upload.CreatedAt(),
		)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, upload)
	}
	return uploads, nil
}
