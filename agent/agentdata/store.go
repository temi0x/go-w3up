package agentdata

import (
	"encoding/json"
	"os"
)

type FSStore struct {
	path string
}

func NewFSStore(path string) *FSStore {
	return &FSStore{
		path: path,
	}
}

func (s *FSStore) Write(data AgentData) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return os.WriteFile(s.path, b, 0600)
}

func (s *FSStore) Read() (AgentData, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return AgentData{}, err
	}

	var data AgentData
	json.Unmarshal(b, &data)
	return data, nil
}
