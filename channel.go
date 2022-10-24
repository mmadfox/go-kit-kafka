package gokitkafka

import (
	"bytes"
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
)

var eventTypeKey = []byte("_e_")

type Broker struct {
	channels        map[string]*Channel
	notFoundHandler Handler
}

func NewBroker() *Broker {
	return &Broker{
		channels: make(map[string]*Channel),
	}
}

func (b *Broker) NotFoundHandler(h Handler) {
	b.notFoundHandler = h
}

func (b *Broker) AddChannel(name string, handler Handler, other ...Handler) *Channel {
	channel := b.NewChannel(name).Handler(handler)
	for i := 0; i < len(other); i++ {
		channel.Handler(other[i])
	}
	return channel
}

func (b *Broker) NewChannel(name string) *Channel {
	channel := newChannel(name)
	b.channels[name] = channel
	return channel
}

func (b *Broker) HandleMessage(ctx context.Context, msg *Message) (err error) {
	channel, ok := b.channels[msg.Topic]
	if ok {
		return channel.handleMessage(ctx, msg)
	}
	if b.notFoundHandler != nil {
		err = b.notFoundHandler.HandleMessage(ctx, msg)
	} else {
		err = fmt.Errorf("gokitkafka: handler not found for topic - %s", msg.Topic)
	}
	return
}

type Channel struct {
	name     string
	handlers []Handler
	matchers []string
}

func newChannel(name string) *Channel {
	return &Channel{
		name:     name,
		handlers: make([]Handler, 0),
	}
}

func (ch *Channel) Match(eventType ...string) *Channel {
	ch.matchers = append(ch.matchers, eventType...)
	return ch
}

func (ch *Channel) Handler(h Handler) *Channel {
	ch.handlers = append(ch.handlers, h)
	return ch
}

func (ch *Channel) handleMessage(ctx context.Context, msg *Message) (err error) {
	et, ok := ch.eventType(msg)
	if ok && len(ch.matchers) > 0 {
		var found bool
		for i := 0; i < len(ch.matchers); i++ {
			if ch.matchers[i] == et {
				found = true
				break
			}
		}
		if !found {
			return
		}
	}
	var errs *multierror.Error
	for i := 0; i < len(ch.handlers); i++ {
		if err = ch.handlers[i].HandleMessage(ctx, msg); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs.ErrorOrNil()
}

func (ch *Channel) eventType(msg *Message) (et string, ok bool) {
	if len(msg.Headers) == 0 {
		return
	}
	for i := 0; i < len(msg.Headers); i++ {
		if bytes.Compare(msg.Headers[i].Key, eventTypeKey) == 0 {
			et = string(msg.Headers[i].Value)
			ok = true
			break
		}
	}
	return
}
