package client

import (
	"context"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
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
// The `caveats` are caveats required to perform an `upload/add` invocation.
func (c *Client) UploadAdd(ctx context.Context, space did.DID, root ipld.Link, shards []ipld.Link, proofs ...delegation.Delegation) (uploadcap.AddOk, error) {
	pfs := make([]delegation.Proof, 0, len(c.Proofs()))
	for _, del := range append(c.Proofs(), proofs...) {
		pfs = append(pfs, delegation.FromDelegation(del))
	}

	res, _, err := invokeAndExecute[uploadcap.AddCaveats, uploadcap.AddOk](
		ctx,
		c,
		uploadcap.Add,
		space.String(),
		uploadcap.AddCaveats{
			Root:   root,
			Shards: shards,
		},
		uploadcap.AddOkType(),
		delegation.WithProof(pfs...),
	)

	if err != nil {
		return uploadcap.AddOk{}, fmt.Errorf("invoking and executing `upload/add`: %w", err)
	}

	addOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return uploadcap.AddOk{}, fmt.Errorf("`upload/add` failed: %w", failErr)
	}

	return addOk, nil
}
