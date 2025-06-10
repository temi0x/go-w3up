package client

import (
	"fmt"

	"github.com/storacha/go-libstoracha/capabilities/access"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
)

// SpaceAccess is the set of capabilities required by the agent to manage a
// space.
var SpaceAccess = []string{
	"space/*",
	"blob/*",
	"index/*",
	"store/*",
	"upload/*",
	"access/*",
	"filecoin/*",
	"usage/*",
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

	capabilityRequests := make([]access.CapabilityRequest, 0, len(SpaceAccess))
	for _, cap := range SpaceAccess {
		capabilityRequests = append(capabilityRequests, access.CapabilityRequest{Can: cap})
	}

	caveats := access.AuthorizeCaveats{
		Iss: &account,
		Att: capabilityRequests,
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
