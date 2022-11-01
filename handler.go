package kafka

import (
	"context"
	"time"
)

// Handler wraps an endpoint and provides a handler for Kafka messages.
type Handler interface {
	HandleMessage(ctx context.Context, msg *Message) error
}

// HandlerFunc wraps an endpoint and provides a handler for Kafka messages.
type HandlerFunc func(ctx context.Context, msg *Message) error

func (hf HandlerFunc) HandleMessage(ctx context.Context, msg *Message) error {
	return hf(ctx, msg)
}

// Message represents a Kafka message.
type Message struct {
	Topic     Topic
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// Header represents a Kafka header.
type Header struct {
	Key   []byte
	Value []byte
}
