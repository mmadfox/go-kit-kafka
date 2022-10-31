package main

import (
	"context"
	"flag"
	"fmt"
	"strings"

	adaptersarama "github.com/mmadfox/go-kit-kafka/adapter/sarama"

	"github.com/Shopify/sarama"
	kafka "github.com/mmadfox/go-kit-kafka"
)

var brokers = ""

func init() {
	flag.StringVar(&brokers, "brokers", ":9092", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}
}

const (
	orderTopicName    = kafka.Topic("private.order.v1")
	customerTopicName = kafka.Topic("private.customer.v1")
)

func main() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_1_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	addrs := strings.Split(brokers, ",")
	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(err)
	}

	publisher := kafka.NewBroadcaster(
		adaptersarama.NewSyncProducer(producer),
	)

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		err = publisher.Publish(ctx, orderTopicName, OrderCreatedEvent{
			OrderID: fmt.Sprintf("orderID-%d", i),
		})
		if err != nil {
			panic(err)
		}
		err = publisher.Publish(ctx, customerTopicName, CustomerMovedEvent{
			CustomerID: fmt.Sprintf("customerID-%d", i),
		})
		if err != nil {
			panic(err)
		}
	}
}

type OrderCreatedEvent struct {
	OrderID string
}

func (e OrderCreatedEvent) MarshalBinary() ([]byte, error) {
	return []byte(e.OrderID), nil
}

type CustomerMovedEvent struct {
	CustomerID string
}

func (e CustomerMovedEvent) MarshalBinary() ([]byte, error) {
	return []byte(e.CustomerID), nil
}
