package sarama

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

type SyncProducer struct {
	producer sarama.SyncProducer
}

func NewSyncProducer(producer sarama.SyncProducer) *SyncProducer {
	return &SyncProducer{
		producer: producer,
	}
}

func (p *SyncProducer) HandleMessage(ctx context.Context, msg *kafka.Message) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("failed to produce message: %w", ctx.Err())
	default:
		message := convertKafkaToSarama(msg)
		if _, _, err := p.producer.SendMessage(message); err != nil {
			return err
		}
		return nil
	}
}
