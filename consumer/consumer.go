package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/i101dev/template-kafka/config"
)

func main() {

	worker, err := connectConsumer([]string{config.KafkaURI()})

	if err != nil {
		fmt.Println("Error connecting consumer")
		panic(err)
	}

	defer func() {
		if err := worker.Close(); err != nil {
			fmt.Println("Error closing worker:", err)
		}
	}()

	topic := "comments"
	consumer, err := startConsumer(worker, topic, 0)

	if err != nil {
		fmt.Println("Error starting consumer:", err)
		panic(err)
	}

	doneCh := make(chan struct{})
	sigChan := setupSignalHandler()
	msgCount := processMessages(consumer, sigChan, doneCh)

	<-doneCh

	fmt.Println("Processed", msgCount, "messages")
}

func startConsumer(consumer sarama.Consumer, topic string, partition int32) (sarama.PartitionConsumer, error) {

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)

	if err != nil {
		return nil, err
	}

	fmt.Println("Consumer started for topic:", topic)

	return partitionConsumer, nil
}

func setupSignalHandler() chan os.Signal {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	return sigChan
}

func processMessages(consumer sarama.PartitionConsumer, sigChan chan os.Signal, doneCh chan struct{}) int {

	msgCount := 0

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
				return // exit the goroutine
			}
		}
	}()

	return msgCount
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
