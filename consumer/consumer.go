package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/i101dev/template-kafka/config"
)

var (
	last     int64
	lastFile = "last.txt"
)

func main() {

	topic := "comments"
	last, _ = config.ReadNumFromFile(lastFile)
	consumer, err := connectConsumer([]string{config.KafkaURI()})

	if err != nil {
		fmt.Println("Error connecting consumer")
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println("Error closing consumer:", err)
		}
	}()

	topicConsumer, err := startConsumer(consumer, topic, 0, last)

	if err != nil {
		fmt.Println("Error starting consumer:", err)
		panic(err)
	}

	doneCh := make(chan struct{})
	sigChan := setupSignalHandler()
	processMessages(topicConsumer, sigChan, doneCh)

	<-doneCh

	fmt.Println("Processed", last, "messages")
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

func startConsumer(consumer sarama.Consumer, topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)

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

func processMessages(consumer sarama.PartitionConsumer, sigChan chan os.Signal, doneCh chan struct{}) {

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println("\n*** >>> [consumer.error] -", err)
			case msg := <-consumer.Messages():
				destructureMSG(msg)
			case <-sigChan:
				fmt.Println("\nInterruption detected")
				doneCh <- struct{}{}
				return
			}
		}
	}()
}

func destructureMSG(msg *sarama.ConsumerMessage) {

	if msg.Offset > 5 {
		config.WriteNumToFile(msg.Offset-5, lastFile)
	}

	// fmt.Printf("Msg count: %d: | Topic (%s) | Message (%s)\n", last, string(msg.Topic), msg.Value)

	var data map[string]interface{}

	if err := json.Unmarshal(msg.Value, &data); err != nil {
		fmt.Println("Error decoding message:", err)
	}

	if msgValue, ok := data["msg"].(string); ok {
		fmt.Printf("\n*** >>> Message received - %s", msgValue)
	} else {
		fmt.Println("Message does not contain 'msg' field or it's not a string")
	}
}
