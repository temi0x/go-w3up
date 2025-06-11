package client

import (
	"fmt"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/guppy/pkg/capability/uploadlist"
)

// UploadList returns a paginated list of uploads in a space.
//
// Required delegated capability proofs: `upload/list`
//
// The `issuer` is the signing authority that is issuing the UCAN invocation.
//
// The `space` is the resource the invocation applies to. It is typically the
// DID of a space.
//
// The `params` are caveats required to perform an `upload/list` invocation.
func UploadList(issuer principal.Signer, space did.DID, params uploadlist.Caveat, options ...Option) (receipt.Receipt[*uploadlist.Success, *uploadlist.Failure], error) {
	cfg, err := NewConfig(options...)
	if err != nil {
		return nil, err
	}

	inv, err := invocation.Invoke(
		issuer,
		cfg.conn.ID(),
		uploadlist.NewCapability(space, params),
		convertToInvocationOptions(cfg)...,
	)
	if err != nil {
		return nil, err
	}

	resp, err := uclient.Execute([]invocation.Invocation{inv}, cfg.conn)
	if err != nil {
		return nil, err
	}

	rcptlnk, ok := resp.Get(inv.Link())
	if !ok {
		return nil, fmt.Errorf("receipt not found: %s", inv.Link())
	}

	reader, err := uploadlist.NewReceiptReader()
	if err != nil {
		return nil, err
	}

	return reader.Read(rcptlnk, resp.Blocks())
}
