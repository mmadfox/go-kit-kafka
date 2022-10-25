package gokitkafka

import (
	"bytes"
	"context"
	"fmt"

	"github.com/hashicorp/go-multierror"
)

var eventTypeKey = []byte("_e_")

// Broker represents is a router for channels and handlers.
type Broker struct {
	channels        map[Topic]*Channel
	notFoundHandler Handler
}

// NewBroker creates a new broker instance.
func NewBroker() *Broker {
	return &Broker{
		channels: make(map[Topic]*Channel),
	}
}

// Channels returns a list of channels.
func (b *Broker) Channels() []*Channel {
	channels := make([]*Channel, 0, len(b.channels))
	for _, channel := range b.channels {
		channels = append(channels, channel)
	}
	return channels
}

// SetNotFoundHandler sets a handler for undefined topic.
func (b *Broker) SetNotFoundHandler(h Handler) {
	b.notFoundHandler = h
}

// AddChannel adds channel to the broker with specified name and handler(s).
func (b *Broker) AddChannel(name Topic, handler Handler, other ...Handler) *Channel {
	channel := b.NewChannel(name).Handler(handler)
	for i := 0; i < len(other); i++ {
		channel.Handler(other[i])
	}
	return channel
}

// NewChannel creates a new channel in the broker and returns it.
func (b *Broker) NewChannel(name Topic) *Channel {
	channel := newChannel(name)
	b.channels[name] = channel
	return channel
}

// HandleMessage handles messages from the broker.
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

// Channel represents a single named channel.
type Channel struct {
	name     Topic
	handlers []Handler
	matchers []string
}

func newChannel(name Topic) *Channel {
	return &Channel{
		name:     name,
		handlers: make([]Handler, 0),
	}
}

// Topic returns the channel name.
func (ch *Channel) Topic() Topic {
	return ch.name
}

// Filter returns a list of event filters.
func (ch *Channel) Filter() []string {
	filters := make([]string, len(ch.matchers))
	copy(filters, ch.matchers)
	return filters
}

// Handlers returns a list of handlers.
func (ch *Channel) Handlers() []Handler {
	handlers := make([]Handler, len(ch.handlers))
	copy(handlers, ch.handlers)
	return handlers
}

// Match matches the handlers by event type.
func (ch *Channel) Match(eventType ...string) *Channel {
	ch.matchers = append(ch.matchers, eventType...)
	return ch
}

// Handler appends handler to the channel.
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
