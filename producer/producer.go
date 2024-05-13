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

func (e *KafkaError) Error() string {
	return fmt.Sprintf("%s: %v", e.Message, e.OriginalError)
}

func main() {

	app := fiber.New()
	// apiV1 := app.Group("/api/v1")

	app.Post("/comment", createComment)

	producerPort := config.Var("PRODUCER_PORT")

	if err := app.Listen(":" + producerPort); err != nil {
		panic("Error starting producer HTTP server: " + err.Error())
	}
}

func ConnectProducer(brokerURLs []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokerURLs, config)

	if err != nil {
		return nil, HandleKafkaError(err, "Failed to connect to Kafka broker")
	}

	return producer, nil
}

func PushCommentToQueue(topic string, message []byte) error {

	brokerURLs := []string{config.KafkaURI()}

	producer, err := ConnectProducer(brokerURLs)

	if err != nil {
		return HandleKafkaError(err, "Producer connection failed")
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)

	if err != nil {
		return HandleKafkaError(err, "Failed to send message to Kafka")
	}

	fmt.Printf("Message stored in topic (%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

func createComment(ctx *fiber.Ctx) error {

	newComment := new(Comment)

	if err := ctx.BodyParser(&newComment); err != nil {
		return HandleError(ctx, 400, err, "Error parsing comment data")
	}

	commentInBytes, err := json.Marshal(newComment)

	if err != nil {
		return HandleError(ctx, 400, err, "Error marshalling comment to JSON")
	}

	if err := PushCommentToQueue("comments", commentInBytes); err != nil {
		return HandleError(ctx, 500, err, "Error pushing comment to queue")
	}

	data := fiber.Map{
		"success": true,
		"comment": newComment,
		"message": "Comment pushed successfully",
	}

	if err := ctx.JSON(&data); err != nil {
		return HandleError(ctx, 500, err, "Error sending JSON response")
	}

	return nil
}

func HandleError(ctx *fiber.Ctx, status int, err error, message string) error {
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
