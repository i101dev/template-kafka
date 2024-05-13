package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/i101dev/template-kafka/config"
)

var (
	msgcount     int64
	msgcountFile = "msgcount.txt"
)

func main() {

	topic := "comments"
	consumer, err := connectConsumer([]string{config.KafkaURI()})

	if err != nil {
		fmt.Println("Error connecting consumer")
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println("Error closing worker:", err)
		}
	}()

	topicConsumer, err := startConsumer(consumer, topic, 0, 0)

	if err != nil {
		fmt.Println("Error starting consumer:", err)
		panic(err)
	}

	doneCh := make(chan struct{})
	sigChan := setupSignalHandler()
	processMessages(topicConsumer, sigChan, doneCh)

	<-doneCh

	fmt.Println("Processed", msgcount, "messages")
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
				msgcount++
				writeNumToFile(msgcount, msgcountFile)
				fmt.Printf("Msg count: %d: | Topic (%s) | Message (%s)\n", msgcount, string(msg.Topic), msg.Value)
				if string(msg.Value) == "PURGE" {
					fmt.Println("\nPurge all Kafka topic messages")
				}
			case <-sigChan:
				fmt.Println("Interruption detected")
				doneCh <- struct{}{}
				return // exit the goroutine
			}
		}
	}()

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

func readNumFromFile(filename string) (int64, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var offset int64
	_, err = fmt.Fscanf(file, "%d", &offset)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func writeNumToFile(offset int64, filename string) error {

	// fmt.Println("\n*** >>>New offset - ", offset)

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)

	if err != nil {
		return err
	}

	defer file.Close()

	_, err = fmt.Fprintf(file, "%d", offset)

	if err != nil {
		return err
	}

	offset += 5

	return nil
}
