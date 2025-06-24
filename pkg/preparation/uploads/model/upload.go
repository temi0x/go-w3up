package model

import (
	"time"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// Upload represents the process of full or partial upload of data from a source, eventually represented as an upload in storacha.
type Upload struct {
	id              types.UploadID
	configurationID types.ConfigurationID
	sourceID        types.SourceID
	createdAt       time.Time
}

// ID returns the unique identifier of the upload.
func (u *Upload) ID() types.UploadID {
	return u.id
}

// ConfigurationID returns the ID of the configuration associated with the upload.
func (u *Upload) ConfigurationID() types.ConfigurationID {
	return u.configurationID
}

// SourceID returns the ID of the source associated with the upload.
func (u *Upload) SourceID() types.SourceID {
	return u.sourceID
}

// CreatedAt returns the creation time of the upload.
func (u *Upload) CreatedAt() time.Time {
	return u.createdAt
}

func validateUpload(upload *Upload) error {

	if upload.id == uuid.Nil {
		return types.ErrEmpty{"upload ID"}
	}
	if upload.configurationID == uuid.Nil {
		return types.ErrEmpty{"configuration ID"}
	}
	if upload.sourceID == uuid.Nil {
		return types.ErrEmpty{"source ID"}
	}
	if upload.createdAt.IsZero() {
		return types.ErrEmpty{"created at"}
	}
	return nil
}

// NewUpload creates a new Upload instance with the given parameters.
func NewUpload(configurationID types.ConfigurationID, sourceID types.SourceID) (*Upload, error) {
	upload := &Upload{
		id:              uuid.New(),
		configurationID: configurationID,
		sourceID:        sourceID,
		createdAt:       time.Now(),
	}
	if err := validateUpload(upload); err != nil {
		return nil, err
	}
	return upload, nil
}

// UploadScanner is a function type that defines the signature for scanning uploads from a database row
type UploadScanner func(id *types.UploadID, configurationID *types.ConfigurationID, sourceID *types.SourceID, createdAt *time.Time) error

// ReadUploadFromDatabase reads an upload from the database using the provided scanner function.
func ReadUploadFromDatabase(scanner UploadScanner) (*Upload, error) {
	var upload Upload

	if err := scanner(&upload.id, &upload.configurationID, &upload.sourceID, &upload.createdAt); err != nil {
		return nil, err
	}

	if err := validateUpload(&upload); err != nil {
		return nil, err
	}

	return &upload, nil
}
