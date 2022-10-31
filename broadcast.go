package kafka

import (
	"context"
	"encoding"
)

// EventPublisher represents a publisher of any event in the Kafka topic.
type EventPublisher interface {
	// Publish publishes events to the specified kafka topic.
	Publish(ctx context.Context, topic Topic, event encoding.BinaryMarshaler) (err error)
}

// Topic represents a topic Kafka.
type Topic string

func (t Topic) String() string {
	return string(t)
}

// Broadcaster allows sending messages to multiple topics.
type Broadcaster struct {
	handler   Handler
	enc       map[Topic]EncodeRequestFunc
	before    []RequestFunc
	after     []ProducerResponseFunc
	finalizer []ProducerFinalizerFunc
}

// BroadcastOption sets an optional parameter for broadcaster.
type BroadcastOption func(*Broadcaster)

// NewBroadcaster creates a new broadcaster.
func NewBroadcaster(
	handler Handler,
	options ...BroadcastOption,
) *Broadcaster {
	b := &Broadcaster{
		handler: handler,
		enc:     make(map[Topic]EncodeRequestFunc),
	}
	for _, opt := range options {
		opt(b)
	}
	return b
}

// BroadcasterEncoder adds an encoder for the specified topic.
// Defaults to default encoder for all topics.
func BroadcasterEncoder(topic Topic, enc EncodeRequestFunc) BroadcastOption {
	return func(b *Broadcaster) {
		b.enc[topic] = enc
	}
}

// BroadcasterBefore sets the RequestFunc that are applied to the outgoing producer
// request before it's invoked.
func BroadcasterBefore(before ...RequestFunc) BroadcastOption {
	return func(b *Broadcaster) {
		b.before = append(b.before, before...)
	}
}

// BroadcasterAfter adds one or more ProducerResponseFunc, which are applied to the
// context after successful message producing.
func BroadcasterAfter(after ...ProducerResponseFunc) BroadcastOption {
	return func(b *Broadcaster) {
		b.after = append(b.after, after...)
	}
}

// BroadcasterFinalizer adds one or more ProducerFinalizerFunc to be executed at the
// end of producing Kafka message. Finalizers are executed in the order in which they
// were added. By default, no finalizer is registered.
func BroadcasterFinalizer(f ...ProducerFinalizerFunc) BroadcastOption {
	return func(b *Broadcaster) {
		b.finalizer = append(b.finalizer, f...)
	}
}

// Publish sends an any event to the specified topic.
func (b Broadcaster) Publish(ctx context.Context, topic Topic, event encoding.BinaryMarshaler) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if len(b.finalizer) > 0 {
		defer func() {
			for _, f := range b.finalizer {
				f(ctx, err)
			}
		}()
	}

	msg := &Message{Topic: topic}
	enc, ok := b.enc[topic]
	if ok {
		err = enc(ctx, msg, event)
		if err != nil {
			return err
		}
	} else {
		msg.Value, err = event.MarshalBinary()
		if err != nil {
			return err
		}
	}

	for _, f := range b.before {
		ctx = f(ctx, msg)
	}

	if err = b.handler.HandleMessage(ctx, msg); err != nil {
		return err
	}

	for _, f := range b.after {
		ctx = f(ctx)
	}

	return
}
