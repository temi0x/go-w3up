package client

import (
	"context"
	"fmt"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/guppy/pkg/capability/uploadadd"
)

// UploadAdd registers an "upload" with the service. The issuer needs proof of
// `upload/add` delegated capability.
//
// Required delegated capability proofs: `upload/add`
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `proofs` are delegation proofs to use in addition to those in the client.
// They won't be saved in the client, only used for this invocation.
//
// The `params` are caveats required to perform an `upload/add` invocation.
func (c *Client) UploadAdd(ctx context.Context, space did.DID, params uploadadd.Caveat, proofs ...delegation.Delegation) (receipt.Receipt[*uploadadd.Success, *uploadadd.Failure], error) {
	pfs := make([]delegation.Proof, 0, len(c.Proofs()))
	for _, del := range append(c.Proofs(), proofs...) {
		pfs = append(pfs, delegation.FromDelegation(del))
	}

	inv, err := invocation.Invoke(
		c.Issuer(),
		c.Connection().ID(),
		uploadadd.NewCapability(space, params),
		delegation.WithProof(pfs...),
	)
	if err != nil {
		return nil, err
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.Connection())
	if err != nil {
		return nil, err
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader, err := uploadadd.NewReceiptReader()
	if err != nil {
		return nil, err
	}

	return reader.Read(rcptlnk, resp.Blocks())
}
