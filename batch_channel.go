package kafka

import (
	"context"
	"sync/atomic"
)

type BatchOption func(*BatchChannel)

type BatchChannel struct {
	name        Topic
	handlers    []BatchHandler
	forceCommit uint32
	filters     []FilterFunc
}

func newBatchChannel(topic Topic, opts ...BatchOption) *BatchChannel {
	batchChannel := &BatchChannel{
		name:     topic,
		handlers: make([]BatchHandler, 0),
	}
	for _, fn := range opts {
		fn(batchChannel)
	}
	return batchChannel
}

func (ch *BatchChannel) IsForceCommit() bool {
	return atomic.LoadUint32(&ch.forceCommit) == 1
}

func (ch *BatchChannel) EnableForceCommit() *BatchChannel {
	atomic.StoreUint32(&ch.forceCommit, 1)
	return ch
}

func (ch *BatchChannel) AddFilter(filter ...FilterFunc) *BatchChannel {
	ch.filters = append(ch.filters, filter...)
	return ch
}

// Handler appends handler to the channel.
func (ch *BatchChannel) Handler(h BatchHandler) *BatchChannel {
	ch.handlers = append(ch.handlers, h)
	return ch
}

func (ch *BatchChannel) HandlerFunc(h BatchHandlerFunc) *BatchChannel {
	ch.handlers = append(ch.handlers, h)
	return ch
}

func (ch *BatchChannel) HandleMessages(ctx context.Context, buf []*Message, size int) (err error) {
	for i := 0; i < len(ch.handlers); i++ {
		err = ch.handlers[i].HandleMessages(ctx, buf, size)
		if err != nil {
			return err
		}
	}
	return
}
