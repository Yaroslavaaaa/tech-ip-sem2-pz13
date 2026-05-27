package main

import (
	"log"
	"os"

	"worker-service/internal/consumer"
)

func main() {
	// Получаем конфигурацию из переменных окружения
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://guest:guest@localhost:5672/"
	}

	queueName := os.Getenv("RABBIT_QUEUE_NAME")
	if queueName == "" {
		queueName = "task_events"
	}

	prefetch := 1 // количество сообщений, которые worker обрабатывает параллельно

	worker, err := consumer.NewWorker(consumer.Config{
		RabbitURL: rabbitURL,
		QueueName: queueName,
		Prefetch:  prefetch,
	})
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	log.Println("Starting RabbitMQ consumer worker...")
	if err := worker.Start(); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
}
