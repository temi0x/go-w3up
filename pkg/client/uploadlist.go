package client

import (
	"context"
	"fmt"

	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
)

// UploadList returns a paginated list of uploads in a space.
//
// Required delegated capability proofs: `upload/list`
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `params` are caveats required to perform an `upload/list` invocation.
//
// The `proofs` are delegation proofs to use in addition to those in the client.
// They won't be saved in the client, only used for this invocation.
func (c *Client) UploadList(ctx context.Context, space did.DID, params uploadcap.ListCaveats, proofs ...delegation.Delegation) (uploadcap.ListOk, error) {
	pfs := make([]delegation.Proof, 0, len(c.Proofs()))
	for _, del := range append(c.Proofs(), proofs...) {
		pfs = append(pfs, delegation.FromDelegation(del))
	}

	res, _, err := invokeAndExecute[uploadcap.ListCaveats, uploadcap.ListOk](
		ctx,
		c,
		uploadcap.List,
		space.String(),
		params,
		uploadcap.ListOkType(),
		delegation.WithProof(pfs...),
	)

	if err != nil {
		return uploadcap.ListOk{}, fmt.Errorf("invoking and executing `upload/add`: %w", err)
	}

	addOk, failErr := result.Unwrap(res)
	if failErr != nil {
		return uploadcap.ListOk{}, fmt.Errorf("`upload/add` failed: %w", failErr)
	}

	return addOk, nil

}
