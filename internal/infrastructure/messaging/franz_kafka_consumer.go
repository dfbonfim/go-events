package messaging

import (
	"context"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"goEvents/internal/domain/service"
	"sync"
	"time"
)

// FranzKafkaConsumer implements the MessageConsumer interface using Franz-Go Kafka client
type FranzKafkaConsumer struct {
	orderService *service.OrderService
	config       *ConsumerConfig
	client       *kgo.Client
	wg           sync.WaitGroup
}

// NewFranzKafkaConsumer creates a new Kafka consumer with the given order service
func NewFranzKafkaConsumer(orderService *service.OrderService, config *ConsumerConfig) *FranzKafkaConsumer {
	if config == nil {
		logrus.Fatal("Kafka configuration must be provided")
	}

	return &FranzKafkaConsumer{
		orderService: orderService,
		config:       config,
	}
}

// Start begins consuming messages in a goroutine
func (c *FranzKafkaConsumer) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consume(ctx)
	}()
}

// consume handles the actual message consumption
func (c *FranzKafkaConsumer) consume(ctx context.Context) {
	// Create Franz-Go client configuration
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.config.BootstrapServers),
		kgo.ConsumerGroup(c.config.GroupID),
		kgo.ConsumeTopics(c.config.Topics...),
	}

	// Set initial offset based on configuration
	if c.config.AutoOffsetReset == "earliest" {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	// Create new client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create Franz-Go client")
		return
	}
	c.client = client
	defer client.Close()

	logrus.WithFields(logrus.Fields{
		"bootstrap_servers": c.config.BootstrapServers,
		"group_id":          c.config.GroupID,
		"topics":            c.config.Topics,
	}).Info("Franz-Go Kafka consumer started and waiting for messages")

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Context canceled, stopping consumer")
			return
		default:
			startTime := time.Now()

			fetches := client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}

			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					logrus.WithError(err.Err).Error("Kafka consumer error")
				}
				continue
			}

			// Iterate over records
			fetches.EachRecord(func(record *kgo.Record) {
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
					"topic":              record.Topic,
					"partition":          record.Partition,
					"offset":             record.Offset,
				}).Info("Message processed")
			})
		}
	}
}

// Wait waits for all consumer goroutines to finish
func (c *FranzKafkaConsumer) Wait() {
	c.wg.Wait()
}
