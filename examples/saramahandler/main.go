package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/kit/transport"
	adaptersarama "github.com/mmadfox/go-kit-kafka/adapter/sarama"

	"github.com/Shopify/sarama"

	kafka "github.com/mmadfox/go-kit-kafka"
)

var (
	brokers  = ""
	version  = ""
	group    = ""
	assignor = ""
	oldest   = true
	verbose  = false
)

func init() {
	flag.StringVar(&brokers, "brokers", ":9092", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "example", "Kafka consumer group definition")
	flag.StringVar(&assignor, "assignor", "range", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(group) == 0 {
		panic("no Kafka consumer group defined, please set the -group flag")
	}
}

const (
	orderTopicName    = kafka.Topic("private.order.v1")
	customerTopicName = kafka.Topic("private.customer.v1")
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_1_0

	addrs := strings.Split(brokers, ",")
	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	consumerGroup, err := sarama.NewConsumerGroupFromClient("group", client)
	if err != nil {
		panic(err)
	}

	broker := kafka.NewBroker()
	broker.AddChannel(orderTopicName,
		kafka.HandlerFunc(
			func(ctx context.Context, msg *kafka.Message) error {
				fmt.Println(msg.Topic, msg.Partition, len(msg.Value))
				return nil
			}))
	broker.AddChannel(customerTopicName,
		kafka.HandlerFunc(
			func(ctx context.Context, msg *kafka.Message) error {
				fmt.Println(msg.Topic, msg.Partition, len(msg.Value))
				return nil
			}))

	handler, err := adaptersarama.NewHandler(
		broker,
		adaptersarama.OnCleanup(
			func(session sarama.ConsumerGroupSession) error {
				fmt.Println("cleanup", session.Claims())
				return nil
			}),
		adaptersarama.OnSetup(
			func(session sarama.ConsumerGroupSession) error {
				fmt.Println("setup", session.Claims())
				return nil
			}))
	if err != nil {
		panic(err)
	}

	topics := []string{
		orderTopicName.String(),
		customerTopicName.String(),
	}

	listener, err := adaptersarama.NewListener(
		topics,
		consumerGroup,
		handler, transport.ErrorHandlerFunc(
			func(ctx context.Context, err error) {
				fmt.Println("error:", err.Error())
			}))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	if err = listener.Listen(ctx); err != nil {
		panic(err)
	}
}
