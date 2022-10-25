package adaptersarama

import (
	"time"

	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

func convertSaramaToKafka(msg *sarama.ConsumerMessage) *kafka.Message {
	headers := make([]kafka.Header, len(msg.Headers))
	for i, h := range msg.Headers {
		headers[i] = kafka.Header{
			Key:   h.Key,
			Value: h.Value,
		}
	}
	return &kafka.Message{
		Topic:     kafka.Topic(msg.Topic),
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
		Timestamp: msg.Timestamp.Unix(),
	}
}

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
		Timestamp: time.Unix(msg.Timestamp, 0),
	}
}
