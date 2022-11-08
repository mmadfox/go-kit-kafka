package kafka

import (
	"context"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/transport"
)

// Endpoint wraps an endpoint and provides a handler for Kafka messages.
type Endpoint struct {
	e            endpoint.Endpoint
	dec          DecodeRequestFunc
	before       []RequestFunc
	after        []ConsumerResponseFunc
	finalizer    []ConsumerFinalizerFunc
	errorHandler transport.ErrorHandler
}

// EndpointOption sets an optional parameter for r.
type EndpointOption func(*Endpoint)

// NewEndpoint constructs a new Endpoint, which implements Handler and wraps
// the provided endpoint.
func NewEndpoint(
	e endpoint.Endpoint,
	dec DecodeRequestFunc,
	opts ...EndpointOption,
) *Endpoint {
	c := &Endpoint{
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

// EndpointBefore functions are executed on the r message object
// before the request is decoded.
func EndpointBefore(before ...RequestFunc) EndpointOption {
	return func(c *Endpoint) {
		c.before = append(c.before, before...)
	}
}

// EndpointAfter functions are executed on the r reply after the
// endpoint is invoked, but before anything is published to the reply.
func EndpointAfter(after ...ConsumerResponseFunc) EndpointOption {
	return func(c *Endpoint) {
		c.after = append(c.after, after...)
	}
}

// EndpointErrorHandler is used to handle non-terminal errors. By default, non-terminal errors
// are ignored. This is intended as a diagnostic measure.
func EndpointErrorHandler(errorHandler transport.ErrorHandler) EndpointOption {
	return func(c *Endpoint) {
		c.errorHandler = errorHandler
	}
}

// EndpointFinalizer is executed at the end of every message processing.
// By default, no finalizer is registered.
func EndpointFinalizer(f ...ConsumerFinalizerFunc) EndpointOption {
	return func(c *Endpoint) {
		c.finalizer = append(c.finalizer, f...)
	}
}

// HandleMessage handles Kafka messages.
func (c Endpoint) HandleMessage(ctx context.Context, msg *Message) (err error) {
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
