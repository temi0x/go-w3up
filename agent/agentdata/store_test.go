package agentdata_test

import (
	"os"
	"testing"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/agent/agentdata"
	"github.com/stretchr/testify/require"
)

const dataFilePath = "testdata/agentdata.json"

func TestWriteReadAgentData(t *testing.T) {
	err := os.RemoveAll(dataFilePath)
	require.NoError(t, err)

	agentPrincipal, err := signer.Generate()
	require.NoError(t, err)
	del, err := newDelegation()

	require.NoError(t, err)

	agentData := agentdata.AgentData{
		Principal:   agentPrincipal,
		Delegations: []delegation.Delegation{del},
	}

	err = agentdata.NewFSStore(dataFilePath).Write(agentData)
	require.NoError(t, err)

	agentDataReturned, err := agentdata.NewFSStore(dataFilePath).Read()
	require.NoError(t, err)

	require.Equal(t, agentData.Principal, agentDataReturned.Principal)
	require.Equal(t, delegationsCids(agentData), delegationsCids(agentDataReturned))
}
