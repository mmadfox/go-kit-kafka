package kafka

import (
	"context"
	"sync/atomic"
)

type BatchPipeOption func(*BatchPipeChannel)

type BatchPipeChannel struct {
	inTopic       Topic
	outTopic      Topic
	handlers      []batchPipeHandler
	filters       []FilterFunc
	forceCommit   uint32
	topicsForJoin []Topic
}

func newBatchPipeChannel(in Topic, out Topic, opts ...BatchPipeOption) *BatchPipeChannel {
	pipeChannel := &BatchPipeChannel{
		inTopic:  in,
		outTopic: out,
		handlers: make([]batchPipeHandler, 0),
	}
	for i := 0; i < len(opts); i++ {
		opts[i](pipeChannel)
	}
	return pipeChannel
}

func (ch *BatchPipeChannel) IsForceCommit() bool {
	return atomic.LoadUint32(&ch.forceCommit) == 1
}

func (ch *BatchPipeChannel) EnableForceCommit() *BatchPipeChannel {
	atomic.StoreUint32(&ch.forceCommit, 1)
	return ch
}

func (ch *BatchPipeChannel) AddFilter(filter ...FilterFunc) *BatchPipeChannel {
	ch.filters = append(ch.filters, filter...)
	return ch
}

func (ch *BatchPipeChannel) Handler(in BatchPipeHandler, out Handler) *BatchPipeChannel {
	handler := batchPipeHandler{r: in, w: out}
	ch.handlers = append(ch.handlers, handler)
	return ch
}

func (ch *BatchPipeChannel) Join(topic ...Topic) *BatchPipeChannel {
	ch.topicsForJoin = append(ch.topicsForJoin, topic...)
	return ch
}

func (ch *BatchPipeChannel) HandleMessages(ctx context.Context, in []*Message, size int) (err error) {
	if size == 0 {
		return
	}
	if len(ch.filters) > 0 {
		filtered := make([]*Message, 0, size)
		for i := 0; i < size; i++ {
			found := match(ch.filters, in[i])
			if !found {
				continue
			}
			filtered = append(filtered, in[i])
		}
		if len(filtered) == 0 {
			// skip messages. nopHandler
			return
		}
		in = filtered
		size = len(filtered)
	}
	out := &Message{Topic: ch.outTopic}
	for i := 0; i < len(ch.handlers); i++ {
		handler := ch.handlers[i]
		if err = handler.r.HandleMessages(ctx, in, size, out); err != nil {
			return
		}
		if len(out.Value) == 0 {
			continue
		}
		if err = handler.w.HandleMessage(ctx, out); err != nil {
			return
		}
	}
	return
}

type batchPipeHandler struct {
	r BatchPipeHandler
	w Handler
}
