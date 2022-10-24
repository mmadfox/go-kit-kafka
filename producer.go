package gokitkafka

import (
	"context"

	"github.com/go-kit/kit/endpoint"
)

var fakeResponse = struct{}{}

// Producer wraps single Kafka topic for message producing
// and implements endpoint.Endpoint.
type Producer struct {
	handler   Handler
	topic     string
	enc       EncodeRequestFunc
	before    []RequestFunc
	after     []ProducerResponseFunc
	finalizer []ProducerFinalizerFunc
}

// ProducerOption sets an optional parameter for producer.
type ProducerOption func(*Producer)

// NewProducer constructs a new producer for a single Kafka topic,
// which implements endpoint.Endpoint.
func NewProducer(
	handler Handler,
	topic string,
	enc EncodeRequestFunc,
	options ...ProducerOption,
) *Producer {
	p := &Producer{
		handler: handler,
		topic:   topic,
		enc:     enc,
	}
	for _, opt := range options {
		opt(p)
	}
	return p
}

// ProducerBefore sets the RequestFunc that are applied to the outgoing producer
// request before it's invoked.
func ProducerBefore(before ...RequestFunc) ProducerOption {
	return func(p *Producer) {
		p.before = append(p.before, before...)
	}
}

// ProducerAfter adds one or more ProducerResponseFunc, which are applied to the
// context after successful message producing.
func ProducerAfter(after ...ProducerResponseFunc) ProducerOption {
	return func(p *Producer) {
		p.after = append(p.after, after...)
	}
}

// ProducerFinalizer adds one or more ProducerFinalizerFunc to be executed at the
// end of producing Kafka message. Finalizers are executed in the order in which they
// were added. By default, no finalizer is registered.
func ProducerFinalizer(f ...ProducerFinalizerFunc) ProducerOption {
	return func(p *Producer) {
		p.finalizer = append(p.finalizer, f...)
	}
}

// Endpoint returns a usable endpoint that invokes message producing.
func (p Producer) Endpoint() endpoint.Endpoint {
	return func(ctx context.Context, request any) (response any, err error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		if len(p.finalizer) > 0 {
			defer func() {
				for _, f := range p.finalizer {
					f(ctx, err)
				}
			}()
		}

		msg := &Message{
			Topic: p.topic,
		}
		if err := p.enc(ctx, msg, request); err != nil {
			return nil, err
		}

		for _, f := range p.before {
			ctx = f(ctx, msg)
		}

		if err := p.handler.HandleMessage(ctx, msg); err != nil {
			return nil, err
		}

		for _, f := range p.after {
			ctx = f(ctx)
		}

		return fakeResponse, nil
	}
}
