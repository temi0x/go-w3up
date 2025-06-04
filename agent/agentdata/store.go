package agentdata

import (
	"encoding/json"
	"os"
)

type AgentDataStore interface {
	Write(AgentData) error
	Read() (AgentData, error)
}

type FSAgentDataStore struct {
	path string
}

func NewFSAgentDataStore(path string) *FSAgentDataStore {
	return &FSAgentDataStore{
		path: path,
	}
}

func (s *FSAgentDataStore) Write(data AgentData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return os.WriteFile(s.path, b, 0600)
}

func (s *FSAgentDataStore) Read() (AgentData, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return AgentData{}, err
	}

	var data AgentData
	json.Unmarshal(b, &data)
	return data, nil
}
