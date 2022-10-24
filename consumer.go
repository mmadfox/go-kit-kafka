package gokitkafka

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/transport"
)

// Consumer wraps an endpoint and provides a handler for Kafka messages.
type Consumer struct {
	e            endpoint.Endpoint
	dec          DecodeRequestFunc
	before       []RequestFunc
	after        []ConsumerResponseFunc
	finalizer    []ConsumerFinalizerFunc
	errorHandler transport.ErrorHandler
}

// ConsumerOption sets an optional parameter for consumer.
type ConsumerOption func(*Consumer)

// NewConsumer constructs a new consumer, which implements Handler and wraps
// the provided endpoint.
func NewConsumer(
	e endpoint.Endpoint,
	dec DecodeRequestFunc,
	opts ...ConsumerOption,
) *Consumer {
	c := &Consumer{
		e:   e,
		dec: dec,
		errorHandler: transport.ErrorHandlerFunc(
			func(ctx context.Context, err error) {}),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ConsumerBefore functions are executed on the consumer message object
// before the request is decoded.
func ConsumerBefore(before ...RequestFunc) ConsumerOption {
	return func(c *Consumer) {
		c.before = append(c.before, before...)
	}
}

// ConsumerAfter functions are executed on the consumer reply after the
// endpoint is invoked, but before anything is published to the reply.
func ConsumerAfter(after ...ConsumerResponseFunc) ConsumerOption {
	return func(c *Consumer) {
		c.after = append(c.after, after...)
	}
}

// ConsumerErrorHandler is used to handle non-terminal errors. By default, non-terminal errors
// are ignored. This is intended as a diagnostic measure.
func ConsumerErrorHandler(errorHandler transport.ErrorHandler) ConsumerOption {
	return func(c *Consumer) {
		c.errorHandler = errorHandler
	}
}

// ConsumerFinalizer is executed at the end of every message processing.
// By default, no finalizer is registered.
func ConsumerFinalizer(f ...ConsumerFinalizerFunc) ConsumerOption {
	return func(c *Consumer) {
		c.finalizer = append(c.finalizer, f...)
	}
}

// HandleMessage handles Kafka messages.
func (c Consumer) HandleMessage(ctx context.Context, msg *Message) (err error) {
	if len(c.finalizer) > 0 {
		defer func() {
			for _, f := range c.finalizer {
				f(ctx, msg, err)
			}
		}()
	}

	for _, f := range c.before {
		ctx = f(ctx, msg)
	}

	request, err := c.dec(ctx, msg)
	if err != nil {
		c.errorHandler.Handle(ctx, err)
		return err
	}

	response, err := c.e(ctx, request)
	if err != nil {
		c.errorHandler.Handle(ctx, err)
		return err
	}

	for _, f := range c.after {
		ctx = f(ctx, response)
	}

	return
}
