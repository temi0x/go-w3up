package configurations

import (
	"github.com/storacha/guppy/pkg/preparation/configurations/model"
	"github.com/storacha/guppy/pkg/preparation/types"
)

type Repo interface {
	// GetConfigurationByID retrieves a configuration by its unique ID.
	GetConfigurationByID(configID types.ConfigurationID) (*model.Configuration, error)
	// GetConfigurationByName retrieves a configuration by its name.
	GetConfigurationByName(name string) (*model.Configuration, error)
	// CreateConfiguration creates a new configuration with the given name and options.
	CreateConfiguration(name string, options ...model.ConfigurationOption) (*model.Configuration, error)
	// DeleteConfiguration deletes the configuration by its unique ID.
	DeleteConfiguration(configID types.ConfigurationID) error
	// ListConfigurations lists all configurations in the repository.
	ListConfigurations() ([]*model.Configuration, error)
	// AddSourceToConfiguration creates a new configuration source mapping with the given configuration ID and source ID.
	AddSourceToConfiguration(configID types.ConfigurationID, sourceID types.SourceID) error
	// RemoveSourceFromConfiguration removes the configuration source mapping by configuration ID and source ID.
	RemoveSourceFromConfiguration(configID types.ConfigurationID, sourceID types.SourceID) error
	// ListConfigurationSources lists all configuration sources for the given configuration ID.
	ListConfigurationSources(configID types.ConfigurationID) ([]types.SourceID, error)
}
