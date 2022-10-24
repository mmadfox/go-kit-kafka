package gokitkafka

import "context"

// DecodeRequestFunc extracts a user-domain request object from
// a Kafka message. It is designed to be used in Kafka Consumers.
type DecodeRequestFunc func(ctx context.Context, msg *Message) (request any, err error)

// EncodeRequestFunc encodes the passed request object into
// a Kafka message object. It is designed to be used in Kafka Producers.
type EncodeRequestFunc func(context.Context, *Message, any) error

// EncodeResponseFunc encodes the passed response object into
// a Kafka message object. It is designed to be used in Kafka Consumers.
type EncodeResponseFunc func(context.Context, any) error
