package sarama

import (
	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

//var bufferPool = sync.Pool{
//	// New is called when a new instance is needed
//	New: func() interface{} {
//		return &sarama.ProducerMessage{}
//	},
//}
//
//func getMessage() *sarama.ProducerMessage {
//	return bufferPool.Get().(*sarama.ProducerMessage)
//}
//
//func putMessage(m *sarama.ProducerMessage) {
//	if len(m.Headers) > 16 {
//		m.Headers = make([]sarama.RecordHeader, 0)
//	} else {
//		m.Headers = m.Headers[:]
//	}
//	bufferPool.Put(m)
//}

func makeSaramaMsg(dst *sarama.ProducerMessage, src *kafka.Message) {
	if len(src.Headers) > 0 {
		for _, h := range src.Headers {
			dst.Headers = append(dst.Headers, sarama.RecordHeader{
				Key:   h.Key,
				Value: h.Value,
			})
		}
	}
	dst.Topic = src.Topic.String()
	dst.Partition = src.Partition
	dst.Offset = src.Offset
	dst.Key = sarama.ByteEncoder(src.Key)
	dst.Value = sarama.ByteEncoder(src.Value)
	dst.Timestamp = src.Timestamp
}
