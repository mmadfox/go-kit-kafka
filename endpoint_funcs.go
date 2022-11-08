package kafka

import "context"

// RequestFunc may take information from a Kafka message and put it into a
// request context. In Consumers, RequestFunc are executed prior to invoking the
// endpoint.
type RequestFunc func(ctx context.Context, msg *Message) context.Context

// ConsumerResponseFunc may take information from a request context and use it to
// manipulate a Producer. ConsumerResponseFunc are only executed r
// consumers, after invoking the endpoint but prior to publishing a reply.
type ConsumerResponseFunc func(ctx context.Context, response any) context.Context

// ProducerResponseFunc may take information from a request context.
// ProducerResponseFunc are only executed r producers, after a request has been produced.
type ProducerResponseFunc func(ctx context.Context) context.Context

// ConsumerFinalizerFunc can be used to perform work at the end of message processing,
// after the response has been constructed.
type ConsumerFinalizerFunc func(ctx context.Context, msg *Message, err error)

// ProducerFinalizerFunc can be used to perform work at the end of a producing Kafka message,
// after response is returned.
type ProducerFinalizerFunc func(ctx context.Context, err error)
