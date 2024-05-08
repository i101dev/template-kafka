package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"

	worker, err := connectConsumer([]string{"localhost:29092"})

	if err != nil {
		fmt.Println("Error connecting consumer")
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)

	if err != nil {
		fmt.Println("Error consuming partition")
		panic(err)
	}

	fmt.Println("consumer started")

	sigChan := make(chan os.Signal, 1)

	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println("\n*** >>> [consumer.error] -", err)

			case msg := <-consumer.Messages():
				msgCount++
				fmt.Printf("Received message count: %d: | Topic (%s) | Message (%s)\n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigChan:
				fmt.Println("Interruption detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh

	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokerURL []string) (sarama.Consumer, error) {

	config := sarama.NewConfig()

	config.Consumer.Return.Errors = true

	conn, err := sarama.NewConsumer(brokerURL, config)

	if err != nil {
		return nil, err
	}

	return conn, nil
}
