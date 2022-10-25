package gokitkafka

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestNewBroker(t *testing.T) {
	broker := NewBroker()
	require.NotNil(t, broker)
}

func TestBroker_AddChannel(t *testing.T) {
	expectedTopic := Topic("order.svc")
	expectedHandlesCount := 3
	expectedChannelsCount := 1
	expectedFilter := []string{"one", "two"}

	broker := NewBroker()
	ctrl := gomock.NewController(t)
	handler := NewMockHandler(ctrl)

	channel := broker.AddChannel(expectedTopic, handler, handler, handler).Match(expectedFilter...)
	require.NotNil(t, channel)
	require.Len(t, broker.Channels(), expectedChannelsCount)
	require.Equal(t, expectedTopic, channel.Topic())
	require.Equal(t, expectedFilter, channel.Filter())
	require.Len(t, channel.Handlers(), expectedHandlesCount)
}

func TestBroker_NewChannel(t *testing.T) {
	expectedTopic := Topic("order.svc")

	broker := NewBroker()
	channel := broker.NewChannel(expectedTopic)
	require.NotNil(t, channel)
	require.Equal(t, expectedTopic, channel.Topic())
}
