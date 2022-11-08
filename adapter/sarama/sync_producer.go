package sarama

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

// SyncProducer wraps a standard sarama synchronous producer
// and provides a proxy method for sending messages.
type SyncProducer struct {
	producer sarama.SyncProducer
}

// NewSyncProducer creates a new SyncProducer instance.
func NewSyncProducer(producer sarama.SyncProducer) *SyncProducer {
	return &SyncProducer{producer: producer}
}

// HandleMessage sends a message to the kafka broker.
func (p *SyncProducer) HandleMessage(ctx context.Context, msg *kafka.Message) (err error) {
	originMsg := new(sarama.ProducerMessage)
	makeSaramaMsg(originMsg, msg)
	select {
	case <-ctx.Done():
		err = fmt.Errorf("kafka/syncproducer: failed to send message, error: %v", ctx.Err())
	default:
		_, _, err = p.producer.SendMessage(originMsg)
	}
	return
}

func (p *SyncProducer) HandleMessages(ctx context.Context, msg []*kafka.Message) (err error) {
	messages := make([]*sarama.ProducerMessage, len(msg))
	for i := 0; i < len(msg); i++ {
		originMsg := new(sarama.ProducerMessage)
		makeSaramaMsg(originMsg, msg[i])
		messages[i] = originMsg
	}
	select {
	case <-ctx.Done():
		err = fmt.Errorf("kafka/syncproducer: failed to send messages, error: %v", ctx.Err())
	default:
		err = p.producer.SendMessages(messages)
	}
	return
}
