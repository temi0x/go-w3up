package client

import (
	"context"
	"fmt"
	"time"

	"github.com/storacha/go-libstoracha/capabilities/access"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/result"
)

type tickChannelKey struct{}

// WithTickChannel adds a tick channel to the context.
func WithTickChannel(ctx context.Context, tickChan <-chan time.Time) context.Context {
	return context.WithValue(ctx, tickChannelKey{}, tickChan)
}

func (c *Client) PollClaim(ctx context.Context, authOk access.AuthorizeOk) <-chan result.Result[[]delegation.Delegation, error] {
	resultChan := make(chan result.Result[[]delegation.Delegation, error])

	go func() {
		defer close(resultChan)

		var tickChan <-chan time.Time
		givenTickChan := ctx.Value(tickChannelKey{})
		if givenTickChan == nil {
			tickChan = time.Tick(250 * time.Millisecond) // Default tick interval
		} else {
			var ok bool
			tickChan, ok = givenTickChan.(<-chan time.Time)
			if !ok {
				resultChan <- result.Error[[]delegation.Delegation](fmt.Errorf("context's tick channel is of wrong type, expected <-chan time.Time"))
				return
			}
		}

		for {
			select {
			case <-ctx.Done():
				resultChan <- result.Error[[]delegation.Delegation](fmt.Errorf("context cancelled before delegations could be claimed: %w", ctx.Err()))
				return
			case _, ok := <-tickChan:
				if !ok {
					resultChan <- result.Error[[]delegation.Delegation](fmt.Errorf("tickChan closed before delegations could be claimed: %w", ctx.Err()))
					return
				}

				dels, err := c.ClaimAccess()
				if err != nil {
					fmt.Println("Failed to claim access:", err)
					resultChan <- result.Error[[]delegation.Delegation](fmt.Errorf("failed to claim access: %w", err))
					return
				}

				// Collect all delegations whose "access/request" fact matches the request
				// link in authOk
				relevantDels := make([]delegation.Delegation, 0, len(dels))
				for _, del := range dels {
					for _, fact := range del.Facts() {
						requestLinkValue, ok := fact["access/request"]
						if !ok {
							continue // Skip if the fact does not contain "access/request"
						}
						requestLinkValueNode, ok := requestLinkValue.(ipld.Node)
						if !ok {
							continue // Skip if the fact is not a valid IPLD Node for some reason
						}
						requestLink, err := requestLinkValueNode.AsLink()
						if err != nil {
							continue // Skip if the "access/request" fact is not a link
						}
						if requestLink.String() == authOk.Request.String() {
							relevantDels = append(relevantDels, del)
							break // No need to check further facts for this delegation
						}
					}
				}

				if len(relevantDels) > 0 {
					resultChan <- result.Ok[[]delegation.Delegation, error](relevantDels)
					return
				}
			}
		}
	}()

	return resultChan
}
