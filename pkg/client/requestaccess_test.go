package client_test

import (
	"testing"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/did"
	ed25519 "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/client"
	"github.com/stretchr/testify/require"
)

func TestRequestAccess(t *testing.T) {
	t.Run("invokes `access/authorize`", func(t *testing.T) {
		signer, err := ed25519.Generate()
		require.NoError(t, err)

		account, err := did.Parse("did:mailto:example.com:alice")
		require.NoError(t, err)

		servicePrincipal, err := did.Parse("did:web:storage.example.com")
		require.NoError(t, err)

		connection, mock, err := newMockConnection(servicePrincipal)
		require.NoError(t, err)

		client.RequestAccess(signer, account, client.WithConnection(connection))
		invocations, _ := mock.ExecutedInvocations()

		require.Len(t, invocations, 1, "expected exactly one invocation to be executed")
		invocation := invocations[0]
		require.Equal(t, signer.DID().String(), invocation.Issuer().DID().String(), "expected invocation issuer to be the given signer")
		require.Equal(t, "did:web:storage.example.com", invocation.Audience().DID().String(), "expected invocation audience to be the service")

		require.Len(t, invocation.Capabilities(), 1, "expected exactly one capability in the invocation")
		capability := invocation.Capabilities()[0]
		require.Equal(t, "access/authorize", capability.Can(), "expected an `access/authorize` invocation")

		nb, err := access.AuthorizeCaveatsReader.Read(capability.Nb())
		require.NoError(t, err, "expected to read the caveats without error")
		require.Equal(t, "did:mailto:example.com:alice", nb.Iss.String(), "expected to authorize the correct issuer")

		requestedCapabilities := make([]string, 0, len(nb.Att))
		for _, att := range nb.Att {
			requestedCapabilities = append(requestedCapabilities, att.Can)
		}
		require.ElementsMatch(t, client.SpaceAccess, requestedCapabilities, "expected to authorize the correct capabilities")
	})
}
