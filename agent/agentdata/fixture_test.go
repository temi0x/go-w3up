package agentdata_test

import (
	"fmt"

	"github.com/ipld/go-ipld-prime/datamodel"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-libstoracha/capabilities/types"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/validator"
	"github.com/storacha/guppy/agent/agentdata"
)

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

var greetSchema = []byte(`
type greetCaveats struct {
		greeting String
}
`)

var greetTS = mustLoadTS()

func mustLoadTS() *ipldschema.TypeSystem {
	ts, err := types.LoadSchemaBytes(greetSchema)
	if err != nil {
		panic(fmt.Errorf("loading greet schema: %w", err))
	}
	return ts
}

func greetCaveatsType() ipldschema.Type {
	return greetTS.TypeByName("greetCaveats")
}

var greetCaveatsReader = schema.Struct[greetCaveats](greetCaveatsType(), nil, types.Converters...)

var greet = validator.NewCapability(
	"speak/greet",
	schema.DIDString(),
	greetCaveatsReader,
	nil,
)

var delegationsCids = func(d agentdata.AgentData) []ipld.Link {
	cids := make([]ipld.Link, len(d.Delegations))
	for i, d := range d.Delegations {
		cids[i] = d.Link()
	}
	return cids
}
