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
	topic1  string
	topic2  string
	topic3  string
	topic4  string
)

func init() {
	flag.StringVar(&brokers, "brokers", ":9092", "kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&topic1, "topic1", "topic1", "")
	flag.StringVar(&topic2, "topic2", "topic2", "")
	flag.StringVar(&topic3, "topic3", "topic3", "")
	flag.StringVar(&topic4, "topic4", "topic4", "")
	flag.Parse()
	if len(brokers) == 0 {
		panic("no kafka bootstrap brokers defined, please set the -brokers flag")
	}
}

const wait = 500 * time.Millisecond

func main() {
	out, cleanup := asyncProducer()
	defer cleanup()

	ready := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	// client
	go func() {
		<-ready
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			msg := &kafka.Message{
				Topic: kafka.Topic(topic1),
				Value: kafka.ToBytes("start:"),
			}
			_ = out.HandleMessage(ctx, msg)
			time.Sleep(wait)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	// pipeline
	broker := kafka.NewBroker()
	broker.NewPipeChannel(kafka.Topic(topic1), kafka.Topic(topic2)).HandlerFunc(
		func(ctx context.Context, in *kafka.Message, out *kafka.Message) error {
			out.Value = kafka.ToBytes(kafka.ToString(in.Value) + "+topic1")
			return nil
		}, out)

	broker.NewPipeChannel(kafka.Topic(topic2), kafka.Topic(topic3)).HandlerFunc(
		func(ctx context.Context, in *kafka.Message, out *kafka.Message) error {
			out.Value = kafka.ToBytes(kafka.ToString(in.Value) + "+topic2")
			return nil
		}, out)

	broker.NewPipeChannel(kafka.Topic(topic3), kafka.Topic(topic4)).HandlerFunc(
		func(ctx context.Context, in *kafka.Message, out *kafka.Message) error {
			out.Value = kafka.ToBytes(kafka.ToString(in.Value) + "+topic3")
			return nil
		}, out)

	broker.NewStreamChannel(kafka.Topic(topic4)).HandlerFunc(
		func(ctx context.Context, msg *kafka.Message) error {
			fmt.Printf("result: %s, target: topic4 \n", kafka.ToString(msg.Value))
			return nil
		})

	listen(ctx, broker, ready)
}

func asyncProducer() (kafka.Handler, func()) {
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

	return adapter, func() {
		_ = client.Close()
	}
}

func listen(ctx context.Context, broker *kafka.Broker, ready chan struct{}) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_1_0
	config.ChannelBufferSize = 256
	config.Consumer.Fetch.Min = 64

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

	topics := []string{topic1, topic2, topic3, topic4}
	listener, err := adaptersarama.NewListener(topics, consumer, adapter,
		func(ctx context.Context, err error) {
			fmt.Println("error:", err.Error())
		})
	if err != nil {
		panic(err)
	}

	_ = listener.Listen(ctx)
}
