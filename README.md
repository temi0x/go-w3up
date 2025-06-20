# guppy

A w3up client in golang. ⚠️ Heavily WIP.

## Install

```sh
go get github.com/storacha/guppy
```

## Usage

⚠️ Heavily WIP. At time of writing the client/CLI does not yet upload arbitrary files or directories, only prepared CAR files.

### Client library

There are two ways to use the client library: you can get a user to interactively log in, or bring a prepared, authorized identity.

To have the user log in, use `(*client.Client) RequestAccess()` to have the service ask the user to authenticate, and `(*client.Client) PollClaim()` to notice when they do. ([Example](examples/loginflow/loginflow.go))

To bring your own pre-authorized identity, instantiate the client with the option `client.WithPrincipal(signer)`. ([Example](examples/byoidentity/byoidentity.go)) You'll first need to [generate a DID](#generate-a-did) and then [delegate capabilities](#obtain-proofs) to that identity.

### CLI

The CLI will automatically generate an identity for you and store it in `~/.w3up/config`. Like the library, there are two ways to authenticate the CLI client: interactively, or by authorizing in advance.

To authorize interactively, use `go run ./cmd login` and follow the prompts.

To authorize in advance, use `go run ./cmd whoami` to see the client's DID and then [delegate capabilities](#obtain-proofs) to that identity. Then, pass the proofs you create on the command line whenever you use the CLI.

## How to

### Generate a DID

You can use `ucan-key` to generate a private key and DID for use with the library. Install Node.js and then use the `ucan-key` module:

```sh
npx ucan-key ed
```

Output should look something like:

```sh
# did:key:z6Mkh9TtUbFJcUHhMmS9dEbqpBbHPbL9oxg1zziWn1CYCNZ2
MgCb+bRGl02JqlWMPUxCyntxlYj0T/zLtR2tn8LFvw6+Yke0BKAP/OUu2tXpd+tniEoOzB3pxqxHZpRhrZl1UYUeraT0=
```
You can use the private key (the line starting `Mg...`) in the CLI by setting the environment variable `W3UP_PRIVATE_KEY`. Alternatively you can use it programmatically after parsing it:

```go
package main

import "github.com/web3-storage/go-ucanto/principal/ed25519/signer"

signer, _ := signer.Parse("MgCb+bRGl02JqlWMPUxCyntxlYj0T/zLtR2tn8LFvw6+Yke0BKAP/OUu2tXpd+tniEoOzB3pxqxHZpRhrZl1UYUeraT0=")
```

### Obtain proofs

Proofs are delegations to your DID enabling it to perform tasks. Currently the best way to obtain proofs that will allow you to interact with the web3.storage API is to use the w3up JS CLI:

1. [Generate a DID](#generate-a-did) and make a note of it (the string starting with `did:key:...`)
1. Install w3 CLI:
    ```sh
    npm install -g @web3-storage/w3cli
    ```
1. Create a space:
    ```sh
    w3 space create <NAME>
    ```
1. Delegate capabilities to your DID:
    ```sh
    w3 delegation create -c 'store/*' -c 'upload/*' <DID>`
    ```

## API

[pkg.go.dev Reference](https://pkg.go.dev/github.com/storacha/guppy)

## Contributing

Feel free to join in. All welcome. Please [open an issue](https://github.com/storacha/guppy/issues)!

## License

Dual-licensed under [MIT + Apache 2.0](LICENSE.md)
