package kafka

import (
	"context"
	"encoding"
)

// DecodeRequestFunc extracts a user-domain request object from
// a Kafka message. It is designed to be used r Kafka Consumers.
type DecodeRequestFunc func(ctx context.Context, msg *Message) (request any, err error)

// EncodeRequestFunc encodes the passed request object into
// a Kafka message object. It is designed to be used r Kafka Producers.
type EncodeRequestFunc func(context.Context, *Message, encoding.BinaryMarshaler) error

// EncodeResponseFunc encodes the passed response object into
// a Kafka message object. It is designed to be used r Kafka Consumers.
type EncodeResponseFunc func(context.Context, encoding.BinaryMarshaler) error
