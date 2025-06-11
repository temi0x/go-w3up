package client

import (
	"fmt"

	"github.com/storacha/go-libstoracha/capabilities/access"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
)

// spaceAccess is the set of capabilities required by the agent to manage a
// space.
var spaceAccess = []access.CapabilityRequest{
	{Can: "space/*"},
	{Can: "blob/*"},
	{Can: "index/*"},
	{Can: "store/*"},
	{Can: "upload/*"},
	{Can: "access/*"},
	{Can: "filecoin/*"},
	{Can: "usage/*"},
}

// RequestAccess requests access to the service as an Account. This is the first
// step of the Agent authorization process.
//
// The [issuer] is the Agent which would like to act as the Account.
//
// The [account] is the Account the Agent would like to act as.
func RequestAccess(issuer principal.Signer, account did.DID, options ...Option) error {
	cfg, err := NewConfig(options...)
	if err != nil {
		return err
	}

	caveats := access.AuthorizeCaveats{
		Iss: &account,
		Att: spaceAccess,
	}

	inv, err := access.Authorize.Invoke(issuer, cfg.conn.ID(), issuer.DID().String(), caveats, convertToInvocationOptions(cfg)...)
	if err != nil {
		return fmt.Errorf("generating invocation: %w", err)
	}

	_, err = uclient.Execute([]invocation.Invocation{inv}, cfg.conn)
	if err != nil {
		return fmt.Errorf("sending invocation: %w", err)
	}

	return nil
}
