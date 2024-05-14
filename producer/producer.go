package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
	"github.com/i101dev/template-kafka/config"
)

type Comment struct {
	Msg string `form:"msg" json:"msg"`
}
type KafkaError struct {
	OriginalError error
	Message       string
}

func main() {

	app := fiber.New()

	app.Post("/comment", createComment)

	producerPort := config.Var("PRODUCER_PORT")

	if err := app.Listen(":" + producerPort); err != nil {
		panic("Error starting producer HTTP server: " + err.Error())
	}
}

func createComment(ctx *fiber.Ctx) error {

	// -------------------------------------------------------------------------------
	// Step 1: Process incoming API request
	//
	newComment := new(Comment)

	if err := ctx.BodyParser(&newComment); err != nil {
		return HandleFiberApiError(ctx, 400, err, "Error parsing comment data")
	}

	commentInBytes, err := json.Marshal(newComment)

	if err != nil {
		return HandleFiberApiError(ctx, 400, err, "Error marshalling comment to JSON")
	}

	// -------------------------------------------------------------------------------
	// Step 2: Push to Kafka
	//
	if err := pushCommentToKafkaQueue("comments", commentInBytes); err != nil {
		return HandleFiberApiError(ctx, 500, err, "Error pushing comment to queue")
	}

	// -------------------------------------------------------------------------------
	// Step 3: Return API response
	//
	data := fiber.Map{
		"success": true,
		"comment": newComment,
		"message": "Comment pushed successfully",
	}

	if err := ctx.JSON(&data); err != nil {
		return HandleFiberApiError(ctx, 500, err, "Error sending JSON response")
	}

	return nil
}

func pushCommentToKafkaQueue(topic string, message []byte) error {

	producer, err := connectKafkaProducer()

	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:    topic,
		Value:    sarama.StringEncoder(message),
		Metadata: []string{1: "metadata 1", 2: "metadata 2", 3: "metadata 3"},
	}

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		return HandleKafkaError(err, "Failed to send message to Kafka")
	}

	fmt.Printf("Message stored in topic (%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

func connectKafkaProducer() (sarama.SyncProducer, error) {

	brokerURLs := []string{config.KafkaURI()}

	config := sarama.NewConfig()

	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerURLs, config)

	if err != nil {
		return nil, HandleKafkaError(err, "Failed to connect to Kafka broker")
	}

	return producer, nil
}

// -------------------------------------------------------------------------------
// Error handlers

func HandleFiberApiError(ctx *fiber.Ctx, status int, err error, message string) error {
	log.Printf("*** >>> %s: %v", message, err)
	return ctx.Status(status).JSON(&fiber.Map{
		"success": false,
		"message": message,
		"error":   err.Error(),
	})
}

func HandleKafkaError(err error, context string) error {
	log.Printf("*** >>> Kafka Error (%s): %v", context, err)
	return &KafkaError{
		OriginalError: err,
		Message:       context,
	}
}
func (e *KafkaError) Error() string {
	return fmt.Sprintf("%s: %v", e.Message, e.OriginalError)
}
