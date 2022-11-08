package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
	adaptersarama "github.com/mmadfox/go-kit-kafka/adapter/sarama"
)

var (
	brokers string
	topic   string
)

func init() {
	flag.StringVar(&brokers, "brokers", ":9092", "kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&topic, "topic", "topic", "")
	flag.Parse()
	if len(brokers) == 0 {
		panic("no kafka bootstrap brokers defined, please set the -brokers flag")
	}
}

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_1_0
	ready := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())

	publisher, cleanup := asyncProducer()
	defer cleanup()

	// async publish
	go func() {
		<-ready
		fmt.Println("start publishing...")
		time.Sleep(time.Second)
		for i := 0; i < 50000; i++ {
			msg := &kafka.Message{
				Topic: kafka.Topic(topic),
				Value: []byte("data"),
			}
			err := publisher.Publish(ctx, msg)
			if err != nil {
				panic(err)
			}
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	// batch consume
	batchConsumer(ctx,
		func(ctx context.Context, buf []*kafka.Message, size int) error {
			fmt.Printf("messages received => capacity=%d, len=%d \n", len(buf), size)
			return nil
		}, ready)
}

func batchConsumer(ctx context.Context, handler kafka.BatchHandlerFunc, ready chan struct{}) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_1_0
	config.ChannelBufferSize = 256
	config.Consumer.Fetch.Min = 64

	fmt.Println("batch consumer")

	addr := strings.Split(brokers, ",")
	client, err := sarama.NewClient(addr, config)
	if err != nil {
		panic(err)
	}

	consumer, err := sarama.NewConsumerGroupFromClient("group", client)
	if err != nil {
		panic(err)
	}

	var once sync.Once

	broker := kafka.NewBroker()
	broker.NewBatchChannel(kafka.Topic(topic)).HandlerFunc(handler)

	adapter, err := adaptersarama.NewHandler(
		broker,
		adaptersarama.WithBatchSize(100),
		adaptersarama.WithBatchWaitTimeout(time.Second),
		adaptersarama.OnCleanup(
			func(session sarama.ConsumerGroupSession) error {
				fmt.Println("cleanup", session.Claims())
				return nil
			}),
		adaptersarama.OnSetup(
			func(session sarama.ConsumerGroupSession) error {
				fmt.Println("setup", session.Claims())
				once.Do(func() {
					close(ready)
				})
				return nil
			}))
	if err != nil {
		panic(err)
	}

	listener, err := adaptersarama.NewListener([]string{topic}, consumer, adapter,
		func(ctx context.Context, err error) {
			fmt.Println("error:", err.Error())
		})
	if err != nil {
		panic(err)
	}

	_ = listener.Listen(ctx)
}

func asyncProducer() (kafka.Publisher, func()) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_1_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	addr := strings.Split(brokers, ",")
	client, err := sarama.NewClient(addr, config)
	if err != nil {
		panic(err)
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}

	adapter := adaptersarama.NewAsyncProducer(producer)
	adapter.OnError(
		func(err *sarama.ProducerError) {
			fmt.Println(err)
		})

	return kafka.NewPublisher(adapter), func() {
		_ = client.Close()
	}
}
