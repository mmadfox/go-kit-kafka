package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	kafka "github.com/mmadfox/go-kit-kafka"
	adaptersarama "github.com/mmadfox/go-kit-kafka/adapter/sarama"

	"github.com/Shopify/sarama"
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

	syncProducer := adaptersarama.NewSyncProducer(producer)

	publisher := kafka.NewPublisher(syncProducer)
	ctx := context.Background()
	startTime := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				msg := kafka.NewMessage(topic)
				if pubErr := publisher.Publish(ctx, msg); pubErr != nil {
					panic(pubErr)
				}
			}
		}(i)
	}
	wg.Wait()

	_ = producer.Close()

	fmt.Printf("took time: %s \n", time.Since(startTime))
}
