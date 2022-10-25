package adaptersarama

import (
	"context"
	"errors"

	"github.com/go-kit/kit/transport"

	"github.com/Shopify/sarama"
)

var (
	ErrTopicsNotFound = errors.New("kafka: topics not found")
	ErrConsumerGroup  = errors.New("kafka: consumer cannot be nil")
	ErrHandler        = errors.New("kafka: handler cannot be nil")
)

type Listener struct {
	topics []string
	cg     sarama.ConsumerGroup
	cgh    sarama.ConsumerGroupHandler
	erh    transport.ErrorHandler
}

func NewListener(
	topics []string,
	cg sarama.ConsumerGroup,
	cgh sarama.ConsumerGroupHandler,
	erh transport.ErrorHandler,
) (*Listener, error) {
	if len(topics) == 0 {
		return nil, ErrTopicsNotFound
	}
	if cg == nil {
		return nil, ErrConsumerGroup
	}
	if cgh == nil {
		return nil, ErrHandler
	}
	if erh == nil {
		erh = transport.ErrorHandlerFunc(func(context.Context, error) {})
	}
	return &Listener{topics: topics, cg: cg, cgh: cgh, erh: erh}, nil
}

func (l *Listener) Listen(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if err := l.cg.Consume(ctx, l.topics, l.cgh); err != nil {
				l.erh.Handle(ctx, err)
				return err
			}
		}
	}
}
