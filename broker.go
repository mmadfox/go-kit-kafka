package kafka

import (
	"errors"
	"fmt"
)

var ErrChannelNotFound = errors.New("kafka: channel not found")

type Broker struct {
	streamChannels    map[Topic]*StreamChannel
	batchChannels     map[Topic]*BatchChannel
	pipeChannels      map[Topic]*PipeChannel
	batchPipeChannels map[Topic]*BatchPipeChannel
	types             map[Topic]ChannelType
}

// Topic represents a topic Kafka.
type Topic string

func (t Topic) String() string {
	return string(t)
}

// NewBroker creates a new broker instance.
func NewBroker() *Broker {
	return &Broker{
		streamChannels:    make(map[Topic]*StreamChannel),
		batchChannels:     make(map[Topic]*BatchChannel),
		pipeChannels:      make(map[Topic]*PipeChannel),
		batchPipeChannels: make(map[Topic]*BatchPipeChannel),
		types:             make(map[Topic]ChannelType),
	}
}

func (b *Broker) ChannelType(topic Topic) ChannelType {
	typ, ok := b.types[topic]
	if !ok {
		return ChannelType(0)
	}
	return typ
}

func (b *Broker) NewPipeChannel(from Topic, to Topic, opts ...PipeOption) *PipeChannel {
	channel := newPipeChannel(from, to, opts...)
	b.pipeChannels[from] = channel
	b.types[from] = StreamPipe
	return channel
}

// NewStreamChannel creates a new stream channel, registers it with the broker, and returns it.
func (b *Broker) NewStreamChannel(topic Topic, opts ...StreamOption) *StreamChannel {
	channel := newChannel(topic, opts...)
	b.streamChannels[topic] = channel
	b.types[topic] = Stream
	return channel
}

func (b *Broker) NewBatchChannel(topic Topic, opts ...BatchOption) *BatchChannel {
	channel := newBatchChannel(topic, opts...)
	b.batchChannels[topic] = channel
	b.types[topic] = Batch
	return channel
}

func (b *Broker) NewBatchPipeChannel(from Topic, to Topic, opts ...BatchPipeOption) *BatchPipeChannel {
	channel := newBatchPipeChannel(from, to, opts...)
	b.batchPipeChannels[from] = channel
	b.types[from] = BatchPipe
	return channel
}

func (b *Broker) BatchChannel(topic Topic) (*BatchChannel, error) {
	channel, ok := b.batchChannels[topic]
	if !ok {
		return nil, b.channelNotFoundErr(topic)
	}
	return channel, nil
}

func (b *Broker) PipeChannel(topic Topic) (*PipeChannel, error) {
	channel, ok := b.pipeChannels[topic]
	if !ok {
		return nil, b.channelNotFoundErr(topic)
	}
	return channel, nil
}

func (b *Broker) BatchPipeChannel(topic Topic) (*BatchPipeChannel, error) {
	channel, ok := b.batchPipeChannels[topic]
	if !ok {
		return nil, b.channelNotFoundErr(topic)
	}
	return channel, nil
}

func (b *Broker) StreamChannel(topic Topic) (*StreamChannel, error) {
	channel, ok := b.streamChannels[topic]
	if !ok {
		return nil, b.channelNotFoundErr(topic)
	}
	return channel, nil
}

func (b *Broker) channelNotFoundErr(topic Topic) error {
	return fmt.Errorf("%w for %s topic",
		ErrChannelNotFound, topic)
}
