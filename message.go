package kafka

import "time"

// Message represents a Kafka message.
type Message struct {
	Topic     Topic
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// Header represents a Kafka header.
type Header struct {
	Key   []byte
	Value []byte
}

func NewMessage(topic string) *Message {
	return &Message{Topic: Topic(topic)}
}
