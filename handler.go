package gokitkafka

import "context"

// Handler wraps an endpoint and provides a handler for Kafka messages.
type Handler interface {
	HandleMessage(ctx context.Context, msg *Message) error
}

// Message represents a Kafka message.
type Message struct {
	Topic     string
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
