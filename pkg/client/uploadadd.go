package client

import (
	"fmt"

	uclient "github.com/storacha/go-ucanto/client"
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
// The `params` are caveats required to perform an `upload/add` invocation.
func (c *Client) UploadAdd(space did.DID, params uploadadd.Caveat, options ...Option) (receipt.Receipt[*uploadadd.Success, *uploadadd.Failure], error) {
	cfg, err := NewConfig(options...)
	if err != nil {
		return nil, err
	}

	inv, err := invocation.Invoke(
		c.Issuer(),
		c.Connection().ID(),
		uploadadd.NewCapability(space, params),
		convertToInvocationOptions(cfg)...,
	)
	if err != nil {
		return nil, err
	}

	resp, err := uclient.Execute([]invocation.Invocation{inv}, c.Connection())
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
