package config

import (
	"fmt"
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

func ReadNumFromFile(filename string) (int64, error) {
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

func WriteNumToFile(offset int64, filename string) error {

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
