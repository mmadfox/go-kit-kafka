package sarama

import (
	"context"
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

type AsyncProducer struct {
	producer  sarama.AsyncProducer
	onSuccess func(msg *sarama.ProducerMessage)
	onError   func(err *sarama.ProducerError)
	closeCh   chan struct{}
	once      sync.Once
}

func NewAsyncProducer(producer sarama.AsyncProducer) *AsyncProducer {
	asyncProducer := &AsyncProducer{
		producer:  producer,
		closeCh:   make(chan struct{}),
		onError:   func(err *sarama.ProducerError) {},
		onSuccess: func(msg *sarama.ProducerMessage) {},
	}
	go asyncProducer.dispatcher()
	return asyncProducer
}

func (p *AsyncProducer) OnError(fn func(err *sarama.ProducerError)) {
	p.onError = fn
}

func (p *AsyncProducer) OnSuccess(fn func(msg *sarama.ProducerMessage)) {
	p.onSuccess = fn
}

func (p *AsyncProducer) HandleMessage(ctx context.Context, msg *kafka.Message) (err error) {
	select {
	case <-ctx.Done():
		err = fmt.Errorf("kafka/asyncproducer: failed to send message, error: %v", ctx.Err())
	default:
	}
	originMsg := new(sarama.ProducerMessage)
	makeSaramaMsg(originMsg, msg)
	p.producer.Input() <- originMsg
	return
}

func (p *AsyncProducer) HandleMessages(ctx context.Context, msg []*kafka.Message) (err error) {
	for i := 0; i < len(msg); i++ {
		originMsg := new(sarama.ProducerMessage)
		makeSaramaMsg(originMsg, msg[i])
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafka/asyncproducer: failed to send messages, error: %v", ctx.Err())
		default:
		}
		p.producer.Input() <- originMsg
	}
	return
}

func (p *AsyncProducer) Close() error {
	err := p.producer.Close()
	p.once.Do(func() {
		close(p.closeCh)
	})
	return err
}

func (p *AsyncProducer) dispatcher() {
	for {
		select {
		case <-p.closeCh:
			return
		case msg := <-p.producer.Errors():
			p.onError(msg)
		case msg := <-p.producer.Successes():
			p.onSuccess(msg)
		}
	}
}
