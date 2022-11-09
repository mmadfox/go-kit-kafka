package kafka

import (
	"context"
)

type BatchOption func(*BatchChannel)

type BatchChannel struct {
	topic         Topic
	handlers      []BatchHandler
	forceCommit   bool
	filters       []FilterFunc
	topicsForJoin []Topic
}

func newBatchChannel(topic Topic, opts ...BatchOption) *BatchChannel {
	batchChannel := &BatchChannel{
		topic:    topic,
		handlers: make([]BatchHandler, 0),
	}
	for _, fn := range opts {
		fn(batchChannel)
	}
	return batchChannel
}

func BatchJoinTopic(topic ...Topic) BatchOption {
	return func(ch *BatchChannel) {
		ch.topicsForJoin = append(ch.topicsForJoin, topic...)
	}
}

func BatchWithFilter(filter ...FilterFunc) BatchOption {
	return func(ch *BatchChannel) {
		ch.filters = append(ch.filters, filter...)
	}
}

func BatchWithForceCommit() BatchOption {
	return func(ch *BatchChannel) {
		ch.forceCommit = true
	}
}

func (ch *BatchChannel) IsForceCommit() bool {
	return ch.forceCommit
}

func (ch *BatchChannel) Handler(h BatchHandler) *BatchChannel {
	ch.handlers = append(ch.handlers, h)
	return ch
}

func (ch *BatchChannel) HandlerFunc(h BatchHandlerFunc) *BatchChannel {
	ch.handlers = append(ch.handlers, h)
	return ch
}

func (ch *BatchChannel) HandleMessages(ctx context.Context, buf []*Message, size int) (err error) {
	if size == 0 {
		return
	}

	if len(ch.filters) > 0 {
		filtered := make([]*Message, 0, size)
		for i := 0; i < size; i++ {
			found := match(ch.filters, buf[i])
			if !found {
				continue
			}
			filtered = append(filtered, buf[i])
		}
		if len(filtered) == 0 {
			// skip messages. nopHandler
			return
		}
		buf = filtered
		size = len(filtered)
	}

	for i := 0; i < len(ch.handlers); i++ {
		err = ch.handlers[i].HandleMessages(ctx, buf, size)
		if err != nil {
			return err
		}
	}
	return
}
