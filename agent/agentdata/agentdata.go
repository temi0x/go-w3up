package agentdata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/multiformats/go-varint"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	rsasigner "github.com/storacha/go-ucanto/principal/rsa/signer"
)

type AgentData struct {
	Principal   principal.Signer
	Delegations []delegation.Delegation
}

type agentDataSerialized struct {
	Principal   []byte
	Delegations [][]byte
}

func (ad AgentData) MarshalJSON() ([]byte, error) {
	delegations := make([][]byte, 0, len(ad.Delegations))
	for _, d := range ad.Delegations {
		b, err := io.ReadAll(d.Archive())
		if err != nil {
			return nil, fmt.Errorf("reading delegation archive: %w", err)
		}
		delegations = append(delegations, b)
	}

	return json.Marshal(agentDataSerialized{
		Principal:   ad.Principal.Encode(),
		Delegations: delegations,
	})
}

func (ad *AgentData) UnmarshalJSON(b []byte) error {
	var s agentDataSerialized
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	// Principal

	code, err := varint.ReadUvarint(bytes.NewReader(s.Principal))
	if err != nil {
		return fmt.Errorf("reading private key codec: %s", err)
	}

	switch code {
	case ed25519signer.Code:
		ad.Principal, err = ed25519signer.Decode(s.Principal)
		if err != nil {
			return err
		}

	case rsasigner.Code:
		ad.Principal, err = rsasigner.Decode(s.Principal)
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("invalid private key codec: %d", code)
	}

	// Delegations

	ad.Delegations = make([]delegation.Delegation, len(s.Delegations))
	for i, db := range s.Delegations {
		d, err := delegation.Extract(db)
		if err != nil {
			return fmt.Errorf("decoding delegation %d: %w", i, err)
		}
		ad.Delegations[i] = d
	}

	return nil
}

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
