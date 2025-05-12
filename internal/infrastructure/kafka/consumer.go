package kafka

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"goEvents/internal/domain/service"
	"sync"
	"time"
)

// Consumer handles Kafka message consumption
type Consumer struct {
	orderService *service.OrderService
	wg           sync.WaitGroup
}

// NewConsumer creates a new Kafka consumer with the given order service
func NewConsumer(orderService *service.OrderService) *Consumer {
	return &Consumer{
		orderService: orderService,
	}
}

// Start begins consuming messages in a goroutine
func (c *Consumer) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consume(ctx)
	}()
}

// consume handles the actual message consumption
func (c *Consumer) consume(ctx context.Context) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "order.group",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create consumer")
		return
	}

	consumer.SubscribeTopics([]string{"orders"}, nil)

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()
		logrus.Info("Context canceled, stopping consumer polling")
		// This will cause consumer.Poll to return nil, breaking the loop
		consumer.Close()
	}()

	logrus.Info("Consumer started and waiting for messages")
	run := true
	for run {
		select {
		case <-ctx.Done():
			logrus.Info("Shutdown signal received, stopping consumer")
			run = false
		default:
			startTime := time.Now()
			ev := consumer.Poll(100) // Poll with 100ms timeout

			if ev == nil {
				continue
			}

			switch ev.(type) {
			case *kafka.Message:
				messageID := uuid.New().String()

				// Process the message using the domain service
				description := "Note-" + messageID
				_, err := c.orderService.CreateOrder(description, 1)
				if err != nil {
					logrus.WithError(err).Error("Error creating order")
				}

				// Calculate processing time in milliseconds
				processingTimeMs := time.Since(startTime).Milliseconds()

				logrus.WithFields(logrus.Fields{
					"message_id":         messageID,
					"processing_time_ms": processingTimeMs,
				}).Info("Message processed")
			}
		}
	}

	logrus.Info("Consumer loop exited")
}

// Wait waits for all consumer goroutines to finish
func (c *Consumer) Wait() {
	c.wg.Wait()
}
