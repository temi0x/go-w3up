package client

import (
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/agentdata"
)

type Client struct {
	connection uclient.Connection
	data       agentdata.AgentData
	saveFn     func(agentdata.AgentData) error
}

// NewClient creates a new client. If [connection] is `nil`, the default
// connection will be used.
func NewClient(connection uclient.Connection, options ...Option2) (*Client, error) {
	c := Client{
		connection: connection,
	}

	for _, opt := range options {
		if err := opt(&c); err != nil {
			return nil, err
		}
	}

	if c.connection == nil {
		c.connection = DefaultConnection
	}

	if c.data.Principal == nil {
		newPrincipal, err := ed25519.Generate()
		if err != nil {
			return nil, err
		}
		c.data.Principal = newPrincipal
	}

	return &c, nil
}

// DID returns the DID of the client.
func (c *Client) DID() did.DID {
	if c.data.Principal == nil {
		return did.DID{}
	}
	return c.data.Principal.DID()
}

// Connection returns the connection used by the client.
func (c *Client) Connection() uclient.Connection {
	return c.connection
}

// Issuer returns the issuing signer of the client.
func (c *Client) Issuer() principal.Signer {
	return c.data.Principal
}

// Proofs returns the delegations of the client.
func (c *Client) Proofs() []delegation.Delegation {
	return c.data.Delegations
}

// AddProofs adds the given delegations to the client's data and saves it.
func (c *Client) AddProofs(delegations ...delegation.Delegation) error {
	c.data.Delegations = append(c.data.Delegations, delegations...)
	if c.saveFn != nil {
		return c.saveFn(c.data)
	}
	return nil
}

type Option2 func(c *Client) error

// WithConnection2 configures the connection for the client to use. If one is
// not provided, the default connection will be used.
func WithConnection2(conn uclient.Connection) Option2 {
	return func(c *Client) error {
		c.connection = conn
		return nil
	}
}

// WithPrincipal configures the principal for the client to use. If one is
// not provided, a new principal will be generated.
func WithPrincipal(principal principal.Signer) Option2 {
	return func(c *Client) error {
		c.data.Principal = principal
		return nil
	}
}

// WithData configures the agent data for the client to use. If one is not
// provided, a new agent data will be created with a new principal.
func WithData(data agentdata.AgentData) Option2 {
	return func(c *Client) error {
		c.data = data
		return nil
	}
}

// WithSaveFn configures the save function for the client to use. This
// function will be called to save the agent data whenever it changes. If
// one is not provided, saving will be silently ignored.
func WithSaveFn(saveFn func(agentdata.AgentData) error) Option2 {
	return func(c *Client) error {
		c.saveFn = saveFn
		return nil
	}
}
