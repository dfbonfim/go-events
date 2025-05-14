package messaging

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"goEvents/internal/domain/service"
	"sync"
	"time"
)

// ConfluentKafkaConsumer implements the MessageConsumer interface using Confluent's Kafka client
type ConfluentKafkaConsumer struct {
	orderService *service.OrderService
	config       *ConsumerConfig
	wg           sync.WaitGroup
}

// NewConfluentKafkaConsumer creates a new Kafka consumer with the given order service
func NewConfluentKafkaConsumer(orderService *service.OrderService, config *ConsumerConfig) *ConfluentKafkaConsumer {
	if config == nil {
		logrus.Fatal("Kafka configuration must be provided")
	}

	return &ConfluentKafkaConsumer{
		orderService: orderService,
		config:       config,
	}
}

// Start begins consuming messages in a goroutine
func (c *ConfluentKafkaConsumer) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consume(ctx)
	}()
}

// consume handles the actual message consumption
func (c *ConfluentKafkaConsumer) consume(ctx context.Context) {
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": c.config.BootstrapServers,
		"group.id":          c.config.GroupID,
		"auto.offset.reset": c.config.AutoOffsetReset,
	}

	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create consumer")
		return
	}

	consumer.SubscribeTopics(c.config.Topics, nil)

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()
		logrus.Info("Context canceled, stopping consumer polling")
		// This will cause consumer.Poll to return nil, breaking the loop
		consumer.Close()
	}()

	logrus.WithFields(logrus.Fields{
		"bootstrap_servers": c.config.BootstrapServers,
		"group_id":          c.config.GroupID,
		"topics":            c.config.Topics,
	}).Info("Confluent Kafka consumer started and waiting for messages")

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

			switch e := ev.(type) {
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
					"topic":              *e.TopicPartition.Topic,
					"partition":          e.TopicPartition.Partition,
					"offset":             e.TopicPartition.Offset,
				}).Info("Message processed")

			case kafka.Error:
				logrus.WithError(e).Error("Kafka consumer error")
			}
		}
	}

	logrus.Info("Confluent Kafka consumer loop exited")
}

// Wait waits for all consumer goroutines to finish
func (c *ConfluentKafkaConsumer) Wait() {
	c.wg.Wait()
}
