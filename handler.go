package kafka

import "context"

// Handler wraps an endpoint and provides a handler for Kafka messages.
type Handler interface {
	HandleMessage(ctx context.Context, msg *Message) error
}

// HandlerFunc wraps an endpoint and provides a handler for Kafka messages.
type HandlerFunc func(ctx context.Context, msg *Message) error

func (fn HandlerFunc) HandleMessage(ctx context.Context, msg *Message) error {
	return fn(ctx, msg)
}

type BatchHandler interface {
	HandleMessages(ctx context.Context, buf []*Message, size int) error
}

type BatchHandlerFunc func(ctx context.Context, buf []*Message, size int) error

func (fn BatchHandlerFunc) HandleMessages(ctx context.Context, buf []*Message, size int) error {
	return fn(ctx, buf, size)
}

type PipeHandler interface {
	HandleMessage(ctx context.Context, in *Message, out *Message) error
}

type PipeHandlerFunc func(ctx context.Context, in *Message, out *Message) error

func (fn PipeHandlerFunc) HandleMessage(ctx context.Context, in *Message, out *Message) error {
	return fn(ctx, in, out)
}

type BatchPipeHandler interface {
	HandleMessages(ctx context.Context, in []*Message, size int, out *Message) error
}
