package sqlrepo

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/uploads"
	uploadsmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

var _ uploads.Repo = (*repo)(nil)

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
