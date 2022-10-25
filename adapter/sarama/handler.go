package adaptersarama

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/transport"
	gokitkafka "github.com/mmadfox/go-kit-kafka"
)

type HandlerOption func(*Handler)

type Handler struct {
	handler      gokitkafka.Handler
	errorHandler transport.ErrorHandler
	onSetup      []func(sarama.ConsumerGroupSession) error
	onCleanup    []func(sarama.ConsumerGroupSession) error
	before       []func(ctx context.Context, msg *sarama.ConsumerMessage) error
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

func HandlerOnSetup(fn ...func(sarama.ConsumerGroupSession) error) HandlerOption {
	return func(h *Handler) {
		h.onSetup = append(h.onSetup, fn...)
	}
}

func HandlerOnCleanup(fn ...func(sarama.ConsumerGroupSession) error) HandlerOption {
	return func(h *Handler) {
		h.onCleanup = append(h.onCleanup, fn...)
	}
}

func HandlerBefore(before ...func(ctx context.Context, msg *sarama.ConsumerMessage) error) HandlerOption {
	return func(h *Handler) {
		h.before = append(h.before, before...)
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
	for msg := range claim.Messages() {
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
		session.MarkMessage(msg, "")
	}
	return nil
}
