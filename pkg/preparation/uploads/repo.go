package uploads

import (
	"github.com/storacha/guppy/pkg/preparation/sources/model"
	"github.com/storacha/guppy/pkg/preparation/types"
	uploadmodel "github.com/storacha/guppy/pkg/preparation/uploads/model"
)

type Repo interface {
	// GetUploadByID retrieves an upload by its unique ID.
	GetUploadByID(uploadID types.UploadID) (*uploadmodel.Upload, error)
	// GetUploadByName retrieves an upload by its name.
	GetUploadByName(name string) (*uploadmodel.Upload, error)
	// CreateUpload creates a new upload with the given name and options.
	CreateUpload(name string, options ...uploadmodel.UploadOption) (*uploadmodel.Upload, error)
	// AddSourceToUpload creates a new upload source mapping with the given upload ID and source ID.
	AddSourceToUpload(uploadID types.UploadID, sourceID types.SourceID) error
	// RemoveSourceFromUpload removes the upload source mapping by upload ID and source ID.
	RemoveSourceFromUpload(uploadID types.UploadID, sourceID types.SourceID) error
	// ListUploadSources lists all upload sources for the given upload ID.
	ListUploadSources(uploadID types.UploadID) ([]*model.Source, error)
	// DeleteUpload deletes the upload by its unique ID.
	DeleteUpload(uploadID types.UploadID) error
	// ListUploads lists all uploads in the repository.
	ListUploads() ([]*uploadmodel.Upload, error)
}
