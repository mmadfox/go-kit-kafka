package kafka

import (
	"context"

	"github.com/hashicorp/go-multierror"
)

type StreamOption func(*StreamChannel)

type StreamChannel struct {
	topic         Topic
	handlers      []Handler
	rollbacks     []Handler
	filters       []FilterFunc
	forceCommit   bool
	topicsForJoin []Topic
}

func newStreamChannel(name Topic, opts ...StreamOption) *StreamChannel {
	channel := &StreamChannel{
		topic:    name,
		handlers: make([]Handler, 0),
	}
	for _, fn := range opts {
		fn(channel)
	}
	return channel
}

func StreamJoinTopic(topic ...Topic) StreamOption {
	return func(ch *StreamChannel) {
		ch.topicsForJoin = append(ch.topicsForJoin, topic...)
	}
}

func StreamWithFilter(filter ...FilterFunc) StreamOption {
	return func(ch *StreamChannel) {
		ch.filters = append(ch.filters, filter...)
	}
}

func StreamWithForceCommit() StreamOption {
	return func(ch *StreamChannel) {
		ch.forceCommit = true
	}
}

func (ch *StreamChannel) IsForceCommit() bool {
	return ch.forceCommit
}

// Handler appends handler to the channel.
func (ch *StreamChannel) Handler(h Handler) *StreamChannel {
	ch.handlers = append(ch.handlers, h)
	return ch
}

// HandlerFunc appends handler func to the channel.
func (ch *StreamChannel) HandlerFunc(h HandlerFunc) *StreamChannel {
	ch.handlers = append(ch.handlers, h)
	return ch
}

// RollbackHandler appends rollback handler(s) to the channel.
// Compensating handler. Processed r reverse order of addition.
func (ch *StreamChannel) RollbackHandler(h ...Handler) *StreamChannel {
	ch.rollbacks = append(ch.rollbacks, h...)
	return ch
}

func (ch *StreamChannel) HandleMessage(ctx context.Context, msg *Message) (err error) {
	if len(ch.filters) > 0 {
		found := match(ch.filters, msg)
		// skip message. nopHandler
		if !found {
			return
		}
	}
	var errs *multierror.Error
	for i := 0; i < len(ch.handlers); i++ {
		err = ch.handlers[i].HandleMessage(ctx, msg)
		if err != nil {
			errs = multierror.Append(errs, err)
			if rbErr := ch.rollback(ctx, msg); rbErr != nil {
				errs = multierror.Append(errs, rbErr)
			}
			return errs
		}
	}
	return
}

func (ch *StreamChannel) rollback(ctx context.Context, origin *Message) error {
	var errs *multierror.Error
	for i := len(ch.rollbacks) - 1; i >= 0; i-- {
		handler := ch.rollbacks[i]
		if err := handler.HandleMessage(ctx, origin); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs.ErrorOrNil()
}
