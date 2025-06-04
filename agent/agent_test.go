package agent_test

import (
	"encoding/json"
	"testing"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/agent"
	"github.com/stretchr/testify/require"
)

var greetSchema = []byte(`
type greetCaveats struct {
		greeting String
}
`)

type greetCaveats struct {
	greeting string
}

func (c greetCaveats) ToIPLD() (datamodel.Node, error) {
	return ipld.WrapWithRecovery(&c, nil)
}

type simplePrincipal struct {
	did did.DID
}

func (sp simplePrincipal) DID() did.DID {
	return sp.did
}

func TestRoundTripAgentData(t *testing.T) {
	signer, err := signer.Generate()
	require.NoError(t, err)

	greetTS, err := types.LoadSchemaBytes(greetSchema)
	require.NoError(t, err)

	greetCaveatsReader := schema.Struct[greetCaveats](greetTS.TypeByName("greetCaveats"), nil, types.Converters...)

	greet := validator.NewCapability(
		"speak/greet",
		schema.DIDString(),
		greetCaveatsReader,
		nil,
	)

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

	agentData := agent.AgentData{
		Principal:   signer,
		Delegations: []delegation.Delegation{del},
	}

	str, err := json.Marshal(agentData)
	require.NoError(t, err)

	var agentDataReturned agent.AgentData
	err = json.Unmarshal(str, &agentDataReturned)
	require.NoError(t, err)

	delegationsCids := func(d agent.AgentData) []ipld.Link {
		cids := make([]ipld.Link, len(d.Delegations))
		for i, d := range d.Delegations {
			cids[i] = d.Link()
		}
		return cids
	}

	require.Equal(t, agentData.Principal, agentDataReturned.Principal)
	require.Equal(t, delegationsCids(agentData), delegationsCids(agentDataReturned))
}
