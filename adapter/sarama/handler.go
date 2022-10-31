package adaptersarama

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/transport"
	gokitkafka "github.com/mmadfox/go-kit-kafka"
)

type HandlerOption func(*Handler)

type HookFunc func(sarama.ConsumerGroupSession) error

type HandlerBeforeFunc func(context.Context, *sarama.ConsumerMessage) error
type HandlerAfterFunc func(context.Context, sarama.ConsumerGroupSession, *sarama.ConsumerMessage)

type Handler struct {
	handler      gokitkafka.Handler
	errorHandler transport.ErrorHandler
	onSetup      []HookFunc
	onCleanup    []HookFunc
	before       []HandlerBeforeFunc
	after        HandlerAfterFunc
}

func NewHandler(h gokitkafka.Handler, opts ...HandlerOption) (*Handler, error) {
	if h == nil {
		return nil, ErrHandler
	}
	handler := &Handler{
		handler:      h,
		errorHandler: transport.ErrorHandlerFunc(func(context.Context, error) {}),
	}
	for _, opt := range opts {
		opt(handler)
	}
	return handler, nil
}

func HandlerOnSetup(fn ...HookFunc) HandlerOption {
	return func(h *Handler) {
		h.onSetup = append(h.onSetup, fn...)
	}
}

func HandlerOnCleanup(fn ...HookFunc) HandlerOption {
	return func(h *Handler) {
		h.onCleanup = append(h.onCleanup, fn...)
	}
}

func HandlerBefore(before ...HandlerBeforeFunc) HandlerOption {
	return func(h *Handler) {
		h.before = append(h.before, before...)
	}
}

func HandlerAfter(after HandlerAfterFunc) HandlerOption {
	return func(h *Handler) {
		h.after = after
	}
}

func (h *Handler) Setup(session sarama.ConsumerGroupSession) error {
	for i := 0; i < len(h.onSetup); i++ {
		if err := h.onSetup[i](session); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) Cleanup(session sarama.ConsumerGroupSession) error {
	for i := 0; i < len(h.onCleanup); i++ {
		if err := h.onCleanup[i](session); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	ctx := session.Context()

	for {
		select {
		case msg := <-claim.Messages():
			for i := 0; i < len(h.before); i++ {
				if err := h.before[i](ctx, msg); err != nil {
					h.errorHandler.Handle(ctx, err)
					continue
				}
			}

			if err := h.handler.HandleMessage(ctx, convertSaramaToKafka(msg)); err != nil {
				h.errorHandler.Handle(ctx, err)
				continue
			}

			if h.after != nil {
				h.after(ctx, session, msg)
			} else {
				session.MarkMessage(msg, "")
			}

		case <-ctx.Done():
			return nil
		}
	}
}
