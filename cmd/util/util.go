package util

import (
	_ "embed"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/transport/car"
	"github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/guppy/pkg/agentdata"
	"github.com/storacha/guppy/pkg/client"
	cdg "github.com/storacha/guppy/pkg/delegation"
)

const defaultServiceName = "staging.up.storacha.network"

func MustGetClient() *client.Client {
	homedir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("obtaining user home directory: %s", err)
	}

	datadir := path.Join(homedir, ".guppy")
	datapath := path.Join(datadir, "config.json")

	data, err := agentdata.ReadFromFile(datapath)

	if err != nil {
		s, err := signer.Generate()
		if err != nil {
			log.Fatalf("generating signer: %s", err)
		}
		data.Principal = s
		data.WriteToFile(datapath)
	}

	c, err := client.NewClient(
		MustGetConnection(),
		client.WithData(data),
		client.WithSaveFn(func(data agentdata.AgentData) error {
			data.WriteToFile(datapath)
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("creating client: %s", err)
	}
	return c
}

func MustGetConnection() uclient.Connection {
	// service URL & DID
	serviceURLStr := os.Getenv("STORACHA_SERVICE_URL") // use env var preferably
	if serviceURLStr == "" {
		serviceURLStr = fmt.Sprintf("https://%s", defaultServiceName)
	}

	serviceURL, err := url.Parse(serviceURLStr)
	if err != nil {
		log.Fatal(err)
	}

	serviceDIDStr := os.Getenv("STORACHA_SERVICE_DID")
	if serviceDIDStr == "" {
		serviceDIDStr = fmt.Sprintf("did:web:%s", defaultServiceName)
	}

	servicePrincipal, err := did.Parse(serviceDIDStr)
	if err != nil {
		log.Fatal(err)
	}

	// HTTP transport and CAR encoding
	channel := http.NewHTTPChannel(serviceURL)
	codec := car.NewCAROutboundCodec()

	conn, err := uclient.NewConnection(servicePrincipal, channel, uclient.WithOutboundCodec(codec))
	if err != nil {
		log.Fatal(err)
	}

	return conn
}

func MustGetReceiptsURL() *url.URL {
	receiptsURLStr := os.Getenv("STORACHA_RECEIPTS_URL")
	if receiptsURLStr == "" {
		receiptsURLStr = fmt.Sprintf("https://%s/receipt", defaultServiceName)
	}

	receiptsURL, err := url.Parse(receiptsURLStr)
	if err != nil {
		log.Fatal(err)
	}

	return receiptsURL
}

func MustParseDID(str string) did.DID {
	did, err := did.Parse(str)
	if err != nil {
		log.Fatalf("parsing DID: %s", err)
	}
	return did
}

func MustGetProof(path string) delegation.Delegation {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("reading proof file: %s", err)
	}

	proof, err := cdg.ExtractProof(b)
	if err != nil {
		log.Fatalf("extracting proof: %s", err)
	}
	return proof
}
