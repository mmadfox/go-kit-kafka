package kafka

import "context"

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
	Timestamp int64
}

// Header represents a Kafka header.
type Header struct {
	Key   []byte
	Value []byte
}

// WithEventType sets the event type to the message header.
func WithEventType(msg *Message, eventType string) {
	if msg == nil {
		return
	}
	msg.Headers = append(msg.Headers, Header{
		Key:   eventTypeKey,
		Value: []byte(eventType),
	})
}
