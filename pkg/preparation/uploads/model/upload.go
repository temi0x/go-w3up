package model

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/storacha/guppy/pkg/preparation/types"
)

// MaxShardSize is the maximum allowed size for a shard, set to 4GB
const MaxShardSize = 4 << 30

// MinShardSize is the minimum allowed size for a shard, set to 128 bytes
const MinShardSize = 128

// DefaultShardSize is the default size for a shard, set to 512MB
const DefaultShardSize = 512 << 20 // default shard size = 512MB

// ErrShardSizeTooLarge indicates that the shard size is larger than the maximum allowed size.
var ErrShardSizeTooLarge = errors.New("Shard size must be less than 4GB")

// ErrShardSizeTooSmall indicates that the shard size is smaller than the minimum allowed size.
var ErrShardSizeTooSmall = errors.New("Shard size must be at least 128 bytes")

// Upload represents an ongoing data upload, which can be associated with multiple sources.
type Upload struct {
	id        types.UploadID
	name      string
	createdAt time.Time

	shardSize uint64 // blob size in bytes
}

// ID returns the unique identifier of the upload.
func (u *Upload) ID() types.UploadID {
	return u.id
}

// Name returns the name of the upload.
func (u *Upload) Name() string {
	return u.name
}

// CreatedAt returns the creation time of the upload.
func (u *Upload) CreatedAt() time.Time {
	return u.createdAt
}

// ShardSize returns the size of each shard in bytes.
func (u *Upload) ShardSize() uint64 {
	return u.shardSize
}

// UploadOption is a functional option type for configuring an Upload.
type UploadOption func(*Upload) error

// WithShardSize sets the size of each shard in bytes for the upload.
// The shard size must be between 128 bytes and 4GB.
func WithShardSize(shardSize uint64) UploadOption {
	return func(u *Upload) error {
		u.shardSize = shardSize
		return nil
	}
}

func validateUpload(u *Upload) (*Upload, error) {
	if u.id == uuid.Nil {
		return nil, errors.New("upload ID cannot be empty")
	}
	if u.name == "" {
		return nil, errors.New("upload name cannot be empty")
	}
	if u.shardSize >= MaxShardSize {
		return nil, ErrShardSizeTooLarge
	}
	if u.shardSize < MinShardSize {
		return nil, ErrShardSizeTooSmall
	}
	return u, nil
}

// NewUpload creates a new Upload instance with the given name and options.
func NewUpload(name string, opts ...UploadOption) (*Upload, error) {
	u := &Upload{
		id:        uuid.New(),
		name:      name,
		shardSize: DefaultShardSize, // default shard size
		createdAt: time.Now(),
	}
	for _, opt := range opts {
		if err := opt(u); err != nil {
			return nil, err
		}
	}
	return validateUpload(u)
}

// UploadRowScanner is a function type for scanning a upload row from the database.
type UploadRowScanner func(id *types.UploadID, name *string, createdAt *time.Time, shardSize *uint64) error

// ReadUploadFromDatabase reads a Upload from the database using the provided scanner function.
func ReadUploadFromDatabase(scanner UploadRowScanner) (*Upload, error) {
	upload := &Upload{}
	err := scanner(&upload.id, &upload.name, &upload.createdAt, &upload.shardSize)
	if err != nil {
		return nil, fmt.Errorf("reading upload from database: %w", err)
	}
	return validateUpload(upload)
}
