package sarama

import (
	"context"
	"errors"

	"github.com/Shopify/sarama"
)

var (
	// ErrTopicNotFound is the error returned when the list of topics is empty when the listener is created.
	ErrTopicNotFound = errors.New("kafka/sarama: topics not found")
	// ErrNilConsumerGroup error returned when creating a listener with nil consumerGroup argument.
	ErrNilConsumerGroup = errors.New("kafka/sarama: consumer cannot be nil")
	// ErrNilHandler error returned when creating a listener with nil groupHandler argument.
	ErrNilHandler = errors.New("kafka/sarama: handler cannot be nil")
)

// Listener wraps sarama.ConsumerGroup.
type Listener struct {
	topics       []string
	group        sarama.ConsumerGroup
	groupHandler sarama.ConsumerGroupHandler
	handleError  ErrorHandlerFunc
}

// ErrorHandlerFunc type is an adapter to allow the use of
// ordinary function as ErrorHandler. If f is a function
// with the appropriate signature, ErrorHandlerFunc(f) is a
// ErrorHandler that calls f.
type ErrorHandlerFunc func(ctx context.Context, err error)

// NewListener creates a new Listener instance with specified arguments.
func NewListener(
	topics []string,
	consumerGroup sarama.ConsumerGroup,
	groupHandler sarama.ConsumerGroupHandler,
	errorHandler ErrorHandlerFunc,
) (*Listener, error) {
	if len(topics) == 0 {
		return nil, ErrTopicNotFound
	}
	if consumerGroup == nil {
		return nil, ErrNilConsumerGroup
	}
	if groupHandler == nil {
		return nil, ErrNilHandler
	}
	if errorHandler == nil {
		errorHandler = func(context.Context, error) {}
	}
	return &Listener{
		topics:       topics,
		group:        consumerGroup,
		groupHandler: groupHandler,
		handleError:  errorHandler,
	}, nil
}

// Listen joins a cluster of consumers for a given list of topics and process messages.
//
// for more details see: https://github.com/Shopify/sarama/blob/main/consumer_group.go#L46
func (l *Listener) Listen(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := l.group.Consume(ctx, l.topics, l.groupHandler); err != nil {
				if errors.Is(err, sarama.ErrClosedClient) {
					return nil
				}
				l.handleError(ctx, err)
				return err
			}
		}
	}
}
