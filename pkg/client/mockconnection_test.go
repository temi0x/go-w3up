package client_test

import (
	"fmt"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/transport"
)

// mockChannelCodec is a mock implementation of both [transport.Channel] and
// [transport.OutboundCodec]. It tracks messages sent to it during tests for
// later assertions.
type mockChannelCodec struct {
	state *mockConnectionState
}

type mockConnectionState struct {
	sentMessages []message.AgentMessage
}

func (tc mockChannelCodec) SentMessages() []message.AgentMessage {
	return tc.state.sentMessages
}

func (tc mockChannelCodec) ExecutedInvocations() ([]invocation.Invocation, error) {
	invocations := make([]invocation.Invocation, 0)

	for _, msg := range tc.SentMessages() {
		blocks := msg.Blocks()
		bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(blocks))
		if err != nil {
			return nil, fmt.Errorf("creating blockstore: %w", err)
		}

		for _, invLink := range msg.Invocations() {
			inv, err := invocation.NewInvocationView(invLink, bs)
			if err != nil {
				return nil, fmt.Errorf("creating invocation view: %w", err)
			}

			invocations = append(invocations, inv)
		}
	}

	return invocations, nil
}

func (tc mockChannelCodec) Request(request transport.HTTPRequest) (transport.HTTPResponse, error) {
	return nil, nil
}

func (tc mockChannelCodec) Encode(message message.AgentMessage) (transport.HTTPRequest, error) {
	tc.state.sentMessages = append(tc.state.sentMessages, message)
	return nil, nil

}

func (tc mockChannelCodec) Decode(response transport.HTTPResponse) (message.AgentMessage, error) {
	return nil, nil
}

func newMockConnection(servicePrincipal did.DID) (uclient.Connection, mockChannelCodec, error) {
	mock := mockChannelCodec{
		state: &mockConnectionState{},
	}

	conn, err := uclient.NewConnection(servicePrincipal, mock, uclient.WithOutboundCodec(mock))
	if err != nil {
		return nil, mockChannelCodec{}, err
	}

	return conn, mock, nil
}
