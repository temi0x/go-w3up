package uploads

import (
	"context"

	"github.com/storacha/guppy/pkg/preparation/types"
	"github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type Uploads struct {
	repo                       Repo
	ConfigurationSourcesLookup ConfigurationSourcesLookupFn
}

type ConfigurationSourcesLookupFn func(ctx context.Context, configurationID types.ConfigurationID) ([]types.SourceID, error)

// CreateUploads creates uploads for a given configuration and its associated sources.
func (u Uploads) CreateUploads(ctx context.Context, configurationID types.ConfigurationID) ([]*model.Upload, error) {
	sources, err := u.ConfigurationSourcesLookup(ctx, configurationID)
	if err != nil {
		return nil, err
	}

	uploads, err := u.repo.CreateUploads(ctx, configurationID, sources)
	if err != nil {
		return nil, err
	}
	return uploads, nil
}

// GetSourceIDForUploadID retrieves the source ID associated with a given upload ID.
func (u Uploads) GetSourceIDForUploadID(ctx context.Context, uploadID types.UploadID) (types.SourceID, error) {
	return u.repo.GetSourceIDForUploadID(ctx, uploadID)
}

// GetUploadByID retrieves an upload by its unique ID.
func (u Uploads) GetUploadByID(ctx context.Context, uploadID types.UploadID) (*model.Upload, error) {
	return u.repo.GetUploadByID(ctx, uploadID)
}
