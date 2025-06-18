package client

import (
	"fmt"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/capability/uploadlist"
)

// UploadList returns a paginated list of uploads in a space.
//
// Required delegated capability proofs: `upload/list`
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `params` are caveats required to perform an `upload/list` invocation.
func (c *Client) UploadList(space did.DID, params uploadlist.Caveat, options ...Option) (receipt.Receipt[*uploadlist.Success, *uploadlist.Failure], error) {
	cfg, err := NewConfig(options...)
	if err != nil {
		return nil, err
	}

	proofs := make([]delegation.Proof, 0, len(c.Proofs()))
	for _, del := range append(c.Proofs(), cfg.prf...) {
		proofs = append(proofs, delegation.FromDelegation(del))
	}

	inv, err := invocation.Invoke(
		c.Issuer(),
		c.Connection().ID(),
		uploadlist.NewCapability(space, params),
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("generating invocation: %w", err)
	}

	resp, err := uclient.Execute([]invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return nil, fmt.Errorf("executing invocation: %w", err)
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader, err := uploadlist.NewReceiptReader()
	if err != nil {
		return nil, fmt.Errorf("generating receipt reader: %w", err)
	}

	return reader.Read(rcptlnk, resp.Blocks())
}
