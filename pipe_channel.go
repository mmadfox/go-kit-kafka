package kafka

import (
	"context"
	"sync/atomic"
)

type PipeOption func(*PipeChannel)

type PipeChannel struct {
	inTopic       Topic
	outTopic      Topic
	handlers      []pipeHandler
	filters       []FilterFunc
	forceCommit   uint32
	topicsForJoin []Topic
}

func newPipeChannel(in Topic, out Topic, opts ...PipeOption) *PipeChannel {
	pipeChannel := &PipeChannel{
		inTopic:  in,
		outTopic: out,
		handlers: make([]pipeHandler, 0),
	}
	for i := 0; i < len(opts); i++ {
		opts[i](pipeChannel)
	}
	return pipeChannel
}

func (ch *PipeChannel) IsForceCommit() bool {
	return atomic.LoadUint32(&ch.forceCommit) == 1
}

func (ch *PipeChannel) EnableForceCommit() *PipeChannel {
	atomic.StoreUint32(&ch.forceCommit, 1)
	return ch
}

func (ch *PipeChannel) AddFilter(filter ...FilterFunc) *PipeChannel {
	ch.filters = append(ch.filters, filter...)
	return ch
}

func (ch *PipeChannel) Handler(in PipeHandler, out Handler) *PipeChannel {
	handler := pipeHandler{r: in, w: out}
	ch.handlers = append(ch.handlers, handler)
	return ch
}

func (ch *PipeChannel) HandlerFunc(in PipeHandlerFunc, out Handler) *PipeChannel {
	handler := pipeHandler{r: in, w: out}
	ch.handlers = append(ch.handlers, handler)
	return ch
}

func (ch *PipeChannel) Join(topic ...Topic) *PipeChannel {
	ch.topicsForJoin = append(ch.topicsForJoin, topic...)
	return ch
}

func (ch *PipeChannel) HandleMessage(ctx context.Context, in *Message) (err error) {
	if len(ch.filters) > 0 {
		found := match(ch.filters, in)
		// skip message. nopHandler
		if !found {
			return
		}
	}

	out := &Message{Topic: ch.outTopic}
	for i := 0; i < len(ch.handlers); i++ {
		handler := ch.handlers[i]
		if err = handler.r.HandleMessage(ctx, in, out); err != nil {
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

type pipeHandler struct {
	r PipeHandler
	w Handler
}
