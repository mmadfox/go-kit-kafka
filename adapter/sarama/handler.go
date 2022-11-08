package sarama

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

var (
	defaultErrorHandler     = func(context.Context, error) bool { return false }
	defaultBatchSize        = 15
	defaultBatchWaitTimeout = 300 * time.Millisecond
)

// Handler implements sarama.ConsumerGroupHandler interface
// and runs in a separate goroutine for each partition in the topic.
//
// Handler supports two modes of handling:
//   - Streaming: in streaming mode, the handler is called for each incoming message
//   - Batching: in batch mode, the handler is called for a group of messages
//
// Batching mode is configures with two options:
//   - WithBatchSize(batchSize int)
//   - WithBatchWaitTimeout(timeout time.Duration)
//
// The batchSize sets the upper bound of messages in the buffer.
// If the buffer has less the number of messages, then the handler will wait timeout.
//
// Defaults:
//   - BatchSize: 15 messages
//   - WaitTimeout: 300 * time.Millisecond
type Handler struct {
	broker           *kafka.Broker
	onSetup          []HookFunc
	onCleanup        []HookFunc
	before           []HandlerBeforeFunc
	errorHandler     ErrorHandler
	batchSize        int
	batchWaitTimeout time.Duration
}

// HandlerOption sets an optional parameter for Handler.
type HandlerOption func(*Handler)

// HookFunc handler lifecycle hook.
type HookFunc func(sarama.ConsumerGroupSession) error

// HandlerBeforeFunc the function is executed before message processing starts.
// If the function returns an error, the handler will go into balancing mode.
// Use to inject context.
type HandlerBeforeFunc func(context.Context, sarama.ConsumerGroupSession, sarama.ConsumerGroupClaim) (context.Context, error)

// ErrorHandler handles an errors.
//
// If the error handler returns TRUE, the messages handler stops executing immediately and proceeds to balance.
// If the error handler returns FALSE, messages processing continues.
//
// The handler is executed synchronously, be careful.
type ErrorHandler func(ctx context.Context, err error) bool

// NewHandler creates a new Handler instance with optional params,
// which implements sarama.ConsumerGroupHandler interface.
func NewHandler(b *kafka.Broker, opts ...HandlerOption) (*Handler, error) {
	if b == nil {
		return nil, fmt.Errorf("kafka/sarama: broker is nil")
	}
	handler := &Handler{
		broker:           b,
		batchSize:        defaultBatchSize,
		batchWaitTimeout: defaultBatchWaitTimeout,
		errorHandler:     defaultErrorHandler,
	}
	for _, opt := range opts {
		opt(handler)
	}
	return handler, nil
}

// WithBatchSize the function sets the upper bound of the messages in the batch.
func WithBatchSize(batchSize int) HandlerOption {
	return func(h *Handler) {
		if batchSize <= 0 {
			return
		}
		h.batchSize = batchSize
	}
}

// WithBatchWaitTimeout the function sets a timeout for forced reset of the messages batch.
func WithBatchWaitTimeout(timeout time.Duration) HandlerOption {
	return func(h *Handler) {
		if timeout == 0 {
			return
		}
		h.batchWaitTimeout = timeout
	}
}

// OnSetup the hook is called at the start of the handler, as well as after each balancing in the group.
// Attention! It is not recommended to perform long operations in the handler.
func OnSetup(fn ...HookFunc) HandlerOption {
	return func(h *Handler) {
		h.onSetup = append(h.onSetup, fn...)
	}
}

// OnCleanup the hook is called before the shutdown and before the rebalance.
func OnCleanup(fn ...HookFunc) HandlerOption {
	return func(h *Handler) {
		h.onCleanup = append(h.onCleanup, fn...)
	}
}

// Setup implements the sarama interface.
func (h *Handler) Setup(session sarama.ConsumerGroupSession) error {
	for i := 0; i < len(h.onSetup); i++ {
		if err := h.onSetup[i](session); err != nil {
			return err
		}
	}
	return nil
}

// Cleanup implements the sarama interface.
func (h *Handler) Cleanup(session sarama.ConsumerGroupSession) error {
	for i := 0; i < len(h.onCleanup); i++ {
		if err := h.onCleanup[i](session); err != nil {
			return err
		}
	}
	return nil
}

// ConsumeClaim implements the sarama interface.
func (h *Handler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) (err error) {
	topic := kafka.Topic(claim.Topic())
	switch h.broker.ChannelType(topic) {
	default:
		err = fmt.Errorf("kafka/sarama: handler not found for %s topic", topic)
	case kafka.Stream:
		channel, chErr := h.broker.StreamChannel(topic)
		if chErr != nil {
			return err
		}
		err = h.handleStreamChannel(topic, channel, session, claim, channel.IsForceCommit())
	case kafka.Batch:
		channel, chErr := h.broker.BatchChannel(topic)
		if chErr != nil {
			return err
		}
		err = h.handleBatchChannel(channel, session, claim, channel.IsForceCommit())
	case kafka.StreamPipe:
		channel, chErr := h.broker.PipeChannel(topic)
		if chErr != nil {
			return err
		}
		err = h.handleStreamChannel(topic, channel, session, claim, channel.IsForceCommit())
	case kafka.BatchPipe:
		channel, chErr := h.broker.BatchPipeChannel(topic)
		if chErr != nil {
			return err
		}
		err = h.handleBatchChannel(channel, session, claim, channel.IsForceCommit())
	}
	return
}

func (h *Handler) handleError(ctx context.Context, err error) (rebalance bool) {
	if h.errorHandler == nil {
		return true
	}
	return h.errorHandler(ctx, err)
}

func (h *Handler) convertMsg(dst *kafka.Message, src *sarama.ConsumerMessage) {
	if len(src.Headers) > 0 {
		dst.Headers = dst.Headers[:]
		for i := 0; i < len(src.Headers); i++ {
			dst.Headers = append(dst.Headers, kafka.Header{
				Key:   src.Headers[i].Key,
				Value: src.Headers[i].Value,
			})
		}
	}
	dst.Topic = kafka.Topic(src.Topic)
	dst.Partition = src.Partition
	dst.Offset = src.Offset
	dst.Timestamp = src.Timestamp
	dst.Key = src.Key
	dst.Value = src.Value
}
