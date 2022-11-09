package kafka

import (
	"context"
)

type PipeOption func(*PipeChannel)

type PipeChannel struct {
	inTopic       Topic
	outTopic      Topic
	handlers      []pipeHandler
	filters       []FilterFunc
	forceCommit   bool
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

func PipeJoinTopic(topic ...Topic) PipeOption {
	return func(ch *PipeChannel) {
		ch.topicsForJoin = append(ch.topicsForJoin, topic...)
	}
}

func PipeWithFilter(filter ...FilterFunc) PipeOption {
	return func(ch *PipeChannel) {
		ch.filters = append(ch.filters, filter...)
	}
}

func PipeWithForceCommit() PipeOption {
	return func(ch *PipeChannel) {
		ch.forceCommit = true
	}
}

func (ch *PipeChannel) IsForceCommit() bool {
	return ch.forceCommit
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
