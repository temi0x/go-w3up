package agentdata_test

import (
	"path"
	"testing"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/agent/agentdata"
	"github.com/stretchr/testify/require"
)

func TestWriteReadAgentData(t *testing.T) {
	dataFilePath := path.Join(t.TempDir(), "agentdata.json")

	agentPrincipal, err := signer.Generate()
	require.NoError(t, err)
	del, err := newDelegation()

	require.NoError(t, err)

	agentData := agentdata.AgentData{
		Principal:   agentPrincipal,
		Delegations: []delegation.Delegation{del},
	}

	err = agentData.WriteToFile(dataFilePath)
	require.NoError(t, err)

	agentDataReturned, err := agentdata.ReadFromFile(dataFilePath)
	require.NoError(t, err)

	require.Equal(t, agentData.Principal, agentDataReturned.Principal)
	require.Equal(t, delegationsCids(agentData), delegationsCids(agentDataReturned))
}
