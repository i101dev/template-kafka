package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

func Var(key string) string {

	if err := godotenv.Load("../.env"); err != nil {
		log.Printf("Warning: Could not load .env file, continuing with environment variables or defaults: %v", err)
		log.Fatal(err)
	}

	value := os.Getenv(key)

	if value == "" {
		log.Printf("Warning: Environment variable %s is not set", key)
	}

	return value
}

func KafkaURI() string {
	kafkaUrl := Var("KAFKA_URL")
	kafkaPort := Var("KAFKA_PORT")
	return kafkaUrl + kafkaPort
}
