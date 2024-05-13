# Kafka Producer & Worker Template

Run the project with the following steps:

1. Start the Kafka instance with `docker-compose.yaml`.
2. Run the producer service.
3. Run the consumer service.
4. Post a message to the producer to see it processed by the consumer.

## Prerequisites

-   Docker and Docker Compose installed
-   Go installed
-   Kafka and Zookeeper installed or ready to be run via Docker Compose

## Step 1: Start Kafka Instance

Open your first terminal and navigate to the folder containing `docker-compose.yaml`. Then, run Docker Compose to start Kafka and Zookeeper.

```bash
docker-compose up -d
```

## Step 2: Start Producer Instance

```bash
cd /producer
go run producer.go
```

## Step 3: Start Consumer Instance

```bash
cd /consumer
go run consumer.go
```

## Step 4: Send a test message

-   Using curl:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"msg": "Hello, Kafka!"}' http://localhost:5000/api/v1/comment
```

-   Using Postman/Thunder, or other

```
{
  "msg":"Pushing test message to Kafka"
}
```

## Step 5: Purging expired messages

```
docker exec -it 7c2e8960d96b /bin/bash
```

Replace `7c2e8960d96b` with the ID of your Kafka container.

```
kafka-topics --bootstrap-server localhost:9092 --delete --topic <topic_name>
```

### Or

```
topics=$(kafka-topics --bootstrap-server localhost:9092 --list)

for topic in $topics
do
    kafka-topics --bootstrap-server localhost:9092 --delete --topic $topic
done
```

```
exit
```

This process will delete all messages and associated data for the specified topics. Make sure to replace the container IDs and topic names as appropriate. Let me know if you need further assistance!
