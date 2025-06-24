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

// MaxBlockSize is the maximum size of a block, set to 2MB
const MaxBlockSize = 2 << 20 // 2MB
// MinBlockSize is the minimum size of a block, set to 64KB
const MinBlockSize = 64 << 10 // 64KB
// DefaultBlockSize is the default size of a block, set to 1MB
const DefaultBlockSize = 1 << 20 // default block size = 1MB

// MaxLinksPerNode is the maximum number of links per node, set to 2048
const MaxLinksPerNode = 2048 // maximum number of links per node
// MinLinksPerNode is the minimum number of links per node, set to 2
const MinLinksPerNode = 2 // minimum number of links per node
// DefaultLinksPerNode is the default number of links per node, set to 1024
const DefaultLinksPerNode = 1024 // default links per node = 1024

// MaxUseHAMTDirectorySize is the maximum size for switching to a HAMT directory, set to 4096
const MaxUseHAMTDirectorySize = 4096

// MinUseHAMTDirectorySize is the minimum size for switching to a HAMT directory, set to 128
const MinUseHAMTDirectorySize = 128

// DefaultUseHAMTDirectorySize is the default size for switching to a HAMT directory, set to 1024
const DefaultUseHAMTDirectorySize = 1024 // default use HAMT directory size = 1024

// ErrBlockSizeTooLarge indicates that the block size is larger than the maximum allowed size.
var ErrBlockSizeTooLarge = errors.New("Block size must be less than 2MB")

// ErrBlockSizeTooSmall indicates that the block size is smaller than the minimum allowed size.
var ErrBlockSizeTooSmall = errors.New("Block size must be at least 64KB")

// ErrLinksPerNodeTooLarge indicates that the number of links per node is larger than the maximum allowed.
var ErrLinksPerNodeTooLarge = errors.New("Links per node must be less than 2048")

// ErrLinksPerNodeTooSmall indicates that the number of links per node is smaller than the minimum allowed.
var ErrLinksPerNodeTooSmall = errors.New("Links per node must be at least 2")

// ErrShardSizeTooLarge indicates that the shard size is larger than the maximum allowed size.
var ErrShardSizeTooLarge = errors.New("Shard size must be less than 4GB")

// ErrShardSizeTooSmall indicates that the shard size is smaller than the minimum allowed size.
var ErrShardSizeTooSmall = errors.New("Shard size must be at least 128 bytes")

// ErrUseHAMTDirectorySizeTooLarge indicates that the size for switching to a HAMT directory is larger than the maximum allowed.
var ErrUseHAMTDirectorySizeTooLarge = errors.New("Use HAMT directory size must be less than 4096")

// ErrUseHAMTDirectorySizeTooSmall indicates that the size for switching to a HAMT directory is smaller than the minimum allowed.
var ErrUseHAMTDirectorySizeTooSmall = errors.New("Use HAMT directory size must be at least 128")

// Configuration represents the configuration for an upload or uploads
type Configuration struct {
	id        types.ConfigurationID
	name      string
	createdAt time.Time

	blockSize            uint64
	linksPerNode         uint64
	shardSize            uint64 // blob size in bytes
	useHAMTDirectorySize uint64 // size in bytes for switching to a HAMT directory
}

// ID returns the unique identifier of the configuration.
func (u *Configuration) ID() types.ConfigurationID {
	return u.id
}

// Name returns the name of the configuration.
func (u *Configuration) Name() string {
	return u.name
}

// CreatedAt returns the creation time of the configuration.
func (u *Configuration) CreatedAt() time.Time {
	return u.createdAt
}

// ShardSize returns the size of each shard in bytes.
func (u *Configuration) ShardSize() uint64 {
	return u.shardSize
}

// BlockSize returns the size of each block in bytes.
func (u *Configuration) BlockSize() uint64 {
	return u.blockSize
}

// LinksPerNode returns the number of links per node.
func (u *Configuration) LinksPerNode() uint64 {
	return u.linksPerNode
}

// UseHAMTDirectorySize returns the size for switching to a HAMT directory.
func (u *Configuration) UseHAMTDirectorySize() uint64 {
	return u.useHAMTDirectorySize
}

// ConfigurationOption is a functional option type for configuring a Configuration.
type ConfigurationOption func(*Configuration) error

// WithShardSize sets the size of each shard in bytes for the configuration.
// The shard size must be between 128 bytes and 4GB.
func WithShardSize(shardSize uint64) ConfigurationOption {
	return func(u *Configuration) error {
		u.shardSize = shardSize
		return nil
	}
}

// WithBlockSize sets the size of each block in bytes for the configuration.
func WithBlockSize(blockSize uint64) ConfigurationOption {
	return func(u *Configuration) error {
		u.blockSize = blockSize
		return nil
	}
}

// WithLinksPerNode sets the number of links per node for the configuration.
func WithLinksPerNode(linksPerNode uint64) ConfigurationOption {
	return func(u *Configuration) error {
		u.linksPerNode = linksPerNode
		return nil
	}
}

// WithUseHAMTDirectorySize sets the size for switching to a HAMT directory.
func WithUseHAMTDirectorySize(useHAMTDirectorySize uint64) ConfigurationOption {
	return func(u *Configuration) error {
		u.useHAMTDirectorySize = useHAMTDirectorySize
		return nil
	}
}

// validateConfiguration checks if the configuration is valid.
func validateConfiguration(u *Configuration) (*Configuration, error) {
	if u.id == uuid.Nil {
		return nil, types.ErrEmpty{Field: "id"}
	}
	if u.name == "" {
		return nil, types.ErrEmpty{Field: "name"}
	}
	if u.shardSize >= MaxShardSize {
		return nil, ErrShardSizeTooLarge
	}
	if u.shardSize < MinShardSize {
		return nil, ErrShardSizeTooSmall
	}
	if u.blockSize >= MaxBlockSize {
		return nil, ErrBlockSizeTooLarge
	}
	if u.blockSize < MinBlockSize {
		return nil, ErrBlockSizeTooSmall
	}
	if u.linksPerNode > MaxLinksPerNode {
		return nil, ErrLinksPerNodeTooLarge
	}
	if u.linksPerNode < MinLinksPerNode {
		return nil, ErrLinksPerNodeTooSmall
	}
	if u.useHAMTDirectorySize > MaxUseHAMTDirectorySize {
		return nil, ErrUseHAMTDirectorySizeTooLarge
	}
	if u.useHAMTDirectorySize < MinUseHAMTDirectorySize {
		return nil, ErrUseHAMTDirectorySizeTooSmall
	}
	return u, nil
}

// NewConfiguration creates a new Configuration instance with the given name and options.
func NewConfiguration(name string, opts ...ConfigurationOption) (*Configuration, error) {
	u := &Configuration{
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
	return validateConfiguration(u)
}

// ConfigurationRowScanner is a function type for scanning a configuration row from the database.
type ConfigurationRowScanner func(id *types.ConfigurationID, name *string, createdAt *time.Time, shardSize *uint64, blockSize *uint64, linksPerNode *uint64, useHAMTDirectorySize *uint64) error

// ReadConfigurationFromDatabase reads a Configuration from the database using the provided scanner function.
func ReadConfigurationFromDatabase(scanner ConfigurationRowScanner) (*Configuration, error) {
	configuration := &Configuration{}
	err := scanner(&configuration.id, &configuration.name, &configuration.createdAt, &configuration.shardSize, &configuration.blockSize, &configuration.linksPerNode, &configuration.useHAMTDirectorySize)
	if err != nil {
		return nil, fmt.Errorf("reading configuration from database: %w", err)
	}
	return validateConfiguration(configuration)
}
