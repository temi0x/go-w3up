package main

import (
	"context"
	"fmt"
	"os"

	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/guppy/pkg/capability/uploadlist"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/delegation"
)

func main() {
	// private key to sign invocation UCAN with
	keybytes, _ := os.ReadFile("path/to/private.key")
	signer, _ := signer.FromRaw(keybytes)

	// UCAN proof that signer can list uploads in this space (a delegation chain)
	prfbytes, _ := os.ReadFile("path/to/proof.ucan")
	proof, _ := delegation.ExtractProof(prfbytes)

	// space to list uploads from
	space, _ := did.Parse("did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y")

	// nil uses the default connection to the Storacha network
	c, _ := client.NewClient(nil, client.WithPrincipal(signer))

	rcpt, _ := c.UploadList(
		context.Background(),
		space,
		uploadlist.Caveat{},
		proof,
	)

	ok, _ := result.Unwrap(rcpt.Out())

	for _, r := range ok.Results {
		fmt.Printf("%s\n", r.Root)
	}
}
