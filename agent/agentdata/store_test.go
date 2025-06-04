package agentdata_test

import (
	"os"
	"testing"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/agent/agentdata"
	"github.com/stretchr/testify/require"
)

const dataFilePath = "testdata/agentdata.json"

func TestWriteReadAgentData(t *testing.T) {
	err := os.RemoveAll(dataFilePath)
	require.NoError(t, err)

	signer, err := signer.Generate()
	require.NoError(t, err)

	audienceDid, err := did.Parse("did:mailto:example.com:alice")
	require.NoError(t, err)

	del, err := greet.Delegate(
		signer,
		simplePrincipal{did: audienceDid},
		signer.DID().String(),
		greetCaveats{
			greeting: "Hi, there!",
		},
	)
	require.NoError(t, err)

	agentData := agentdata.AgentData{
		Principal:   signer,
		Delegations: []delegation.Delegation{del},
	}

	err = agentdata.NewFSAgentDataStore(dataFilePath).Write(agentData)
	require.NoError(t, err)

	agentDataReturned, err := agentdata.NewFSAgentDataStore(dataFilePath).Read()
	require.NoError(t, err)

	require.Equal(t, agentData.Principal, agentDataReturned.Principal)
	require.Equal(t, delegationsCids(agentData), delegationsCids(agentDataReturned))
}
