package client_test

import (
	"context"
	"testing"
)

// testContext returns the `t.Context()` if available, or a background context
// on older versions of Go.
func testContext(t *testing.T) context.Context {
	if tCtx, ok := any(t).(interface{ Context() context.Context }); ok {
		return tCtx.Context()
	}
	return context.Background()
}
