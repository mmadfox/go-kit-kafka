package kafka

import (
	"context"
)

type Publisher interface {
	Publish(ctx context.Context, message *Message) error
}

type publisher struct {
	handler Handler
}

func NewPublisher(handler Handler) Publisher {
	return &publisher{handler: handler}
}

func (p *publisher) Publish(ctx context.Context, message *Message) error {
	return p.handler.HandleMessage(ctx, message)
}

var _ Publisher = &publisher{}
