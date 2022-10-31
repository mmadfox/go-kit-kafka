package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/suite"

	"github.com/golang/mock/gomock"
)

type BrokerTestSuite struct {
	suite.Suite

	topics          []Topic
	ctrl            *gomock.Controller
	handler         *MockHandler
	rollbackHandler *MockHandler
	broker          *Broker
	msg             *Message
	ctx             context.Context
}

func TestBroker(t *testing.T) {
	suite.Run(t, new(BrokerTestSuite))
}

func (s *BrokerTestSuite) SetupTest() {
	s.topics = []Topic{"t1", "t2", "t3"}
	s.ctrl = gomock.NewController(s.T())
	s.handler = NewMockHandler(s.ctrl)
	s.rollbackHandler = NewMockHandler(s.ctrl)
	s.broker = NewBroker()
	s.msg = &Message{Topic: s.topics[0]}
}

func (s *BrokerTestSuite) TestAddChannels() {
	s.eachTopic(func(topic Topic) {
		s.broker.AddChannel(topic, s.handler, s.handler, s.handler)
	})
	channels := s.broker.Channels()
	require.Len(s.T(), channels, len(s.topics))
	for i := 0; i < len(channels); i++ {
		ch := channels[i]
		require.Equal(s.T(), s.topics[i], ch.Topic())
		require.Len(s.T(), ch.Handlers(), 3)
	}
}

func (s *BrokerTestSuite) TestSetNotFoundHandler() {
	s.handler.EXPECT().HandleMessage(gomock.Any(), s.msg).Times(1)

	s.broker.SetNotFoundHandler(s.handler)
	err := s.broker.HandleMessage(s.ctx, s.msg)
	require.NoError(s.T(), err)
}

func (s *BrokerTestSuite) eachTopic(fn func(Topic)) {
	for i := 0; i < len(s.topics); i++ {
		fn(s.topics[i])
	}
}
