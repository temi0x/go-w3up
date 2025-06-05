package agentdata

import (
	"encoding/json"
	"os"
)

func (ad AgentData) WriteToFile(path string) error {
	b, err := json.Marshal(ad)
	if err != nil {
		return err
	}

	return os.WriteFile(path, b, 0600)
}

func ReadFromFile(path string) (AgentData, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return AgentData{}, err
	}

	var ad AgentData
	json.Unmarshal(b, &ad)
	return ad, nil
}
