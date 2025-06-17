package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/briandowns/spinner"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/guppy/cmd/util"
	"github.com/storacha/guppy/pkg/capability/uploadlist"
	"github.com/storacha/guppy/pkg/client"
	"github.com/storacha/guppy/pkg/didmailto"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "w3",
		Usage: "interact with the web3.storage API",
		Commands: []*cli.Command{
			{
				Name:   "whoami",
				Usage:  "Print information about the current agent.",
				Action: whoami,
			},
			{
				Name:      "login",
				Usage:     "Authenticate this agent with your email address to gain access to all capabilities that have been delegated to it.",
				UsageText: "login <email>",
				Action:    login,
			},
			{
				Name:    "up",
				Aliases: []string{"upload"},
				Usage:   "Store a file(s) to the service and register an upload.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "space",
						Value: "",
						Usage: "DID of space to upload to.",
					},
					&cli.StringFlag{
						Name:  "proof",
						Value: "",
						Usage: "Path to file containing UCAN proof(s) for the operation.",
					},
					&cli.StringFlag{
						Name:    "car",
						Aliases: []string{"c"},
						Value:   "",
						Usage:   "Path to CAR file to upload.",
					},
					&cli.BoolFlag{
						Name:    "hidden",
						Aliases: []string{"H"},
						Value:   false,
						Usage:   "Include paths that start with \".\".",
					},
					&cli.BoolFlag{
						Name:    "json",
						Aliases: []string{"j"},
						Value:   false,
						Usage:   "Format as newline delimited JSON",
					},
					&cli.BoolFlag{
						Name:    "verbose",
						Aliases: []string{"v"},
						Value:   false,
						Usage:   "Output more details.",
					},
					&cli.BoolFlag{
						Name:  "wrap",
						Value: true,
						Usage: "Wrap single input file in a directory. Has no effect on directory or CAR uploads. Pass --no-wrap to disable.",
					},
					&cli.IntFlag{
						Name:  "shard-size",
						Value: 0,
						Usage: "Shard uploads into CAR files of approximately this size in bytes.",
					},
				},
				Action: upload,
			},
			{
				Name:    "ls",
				Aliases: []string{"list"},
				Usage:   "List uploads in the current space.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "space",
						Value: "",
						Usage: "DID of space to list uploads from.",
					},
					&cli.StringFlag{
						Name:  "proof",
						Value: "",
						Usage: "Path to file containing UCAN proof(s) for the operation.",
					},
					&cli.BoolFlag{
						Name:  "shards",
						Value: false,
						Usage: "Display shard CID(s) for each upload root.",
					},
				},
				Action: ls,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func whoami(cCtx *cli.Context) error {
	s := util.MustGetSigner()
	fmt.Println(s.DID())
	return nil
}

func login(cCtx *cli.Context) error {
	email := cCtx.Args().First()
	if email == "" {
		return fmt.Errorf("email address is required")
	}

	accountDid, err := didmailto.FromEmail(email)
	if err != nil {
		return fmt.Errorf("invalid email address: %w", err)
	}

	c, err := client.NewClient(util.MustGetConnection())
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	authOk, err := c.RequestAccess(accountDid.String())
	if err != nil {
		return fmt.Errorf("requesting access: %w", err)
	}

	resultChan := c.PollClaim(cCtx.Context, authOk)

	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond) // Spinner: ⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏
	s.Suffix = fmt.Sprintf(" 🔗 please click the link sent to %s to authorize this agent", email)
	s.Start()
	// FIXME: This is meant to clean up if we SIGINT (Ctrl+C) the process, but doesn't.
	defer s.Stop()
	claimedDels, err := result.Unwrap(<-resultChan)
	s.Stop()

	if err != nil {
		return fmt.Errorf("claiming access: %w", err)
	}

	fmt.Println("Successfully logged in!", claimedDels)

	// s := util.MustGetSigner()
	// fmt.Println(s.DID())
	return nil
}

func ls(cCtx *cli.Context) error {
	signer := util.MustGetSigner()
	conn := util.MustGetConnection()
	space := util.MustParseDID(cCtx.String("space"))
	proof := util.MustGetProof(cCtx.String("proof"))

	rcpt, err := client.UploadList(
		signer,
		space,
		uploadlist.Caveat{},
		client.WithConnection(conn),
		client.WithProofs([]delegation.Delegation{proof}),
	)
	if err != nil {
		return err
	}

	lsSuccess, lsFailure := result.Unwrap(rcpt.Out())
	if lsFailure != nil {
		return fmt.Errorf("%+v", lsFailure)
	}

	for _, r := range lsSuccess.Results {
		fmt.Printf("%s\n", r.Root)
		if cCtx.Bool("shards") {
			for _, s := range r.Shards {
				fmt.Printf("\t%s\n", s)
			}
		}
	}

	return nil
}
