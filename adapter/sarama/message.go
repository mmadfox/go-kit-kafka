package sarama

import (
	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

func convertKafkaToSarama(msg *kafka.Message) *sarama.ProducerMessage {
	headers := make([]sarama.RecordHeader, len(msg.Headers))
	for i, h := range msg.Headers {
		headers[i] = sarama.RecordHeader{
			Key:   h.Key,
			Value: h.Value,
		}
	}
	return &sarama.ProducerMessage{
		Topic:     msg.Topic.String(),
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       sarama.ByteEncoder(msg.Key),
		Value:     sarama.ByteEncoder(msg.Value),
		Headers:   headers,
		Timestamp: msg.Timestamp,
	}
}
