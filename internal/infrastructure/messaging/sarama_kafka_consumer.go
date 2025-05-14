package messaging

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"goEvents/internal/domain/service"
	"sync"
	"time"
)

// SaramaKafkaConsumer implements the MessageConsumer interface using Sarama Kafka client
type SaramaKafkaConsumer struct {
	orderService   *service.OrderService
	config         *ConsumerConfig
	client         sarama.ConsumerGroup
	wg             sync.WaitGroup
	consumerClosed chan struct{}
}

// NewSaramaKafkaConsumer creates a new Kafka consumer with the given order service
func NewSaramaKafkaConsumer(orderService *service.OrderService, config *ConsumerConfig) *SaramaKafkaConsumer {
	if config == nil {
		logrus.Fatal("Kafka configuration must be provided")
	}

	return &SaramaKafkaConsumer{
		orderService:   orderService,
		config:         config,
		consumerClosed: make(chan struct{}),
	}
}

// Start begins consuming messages in a goroutine
func (c *SaramaKafkaConsumer) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consume(ctx)
	}()
}

// consume handles the actual message consumption
func (c *SaramaKafkaConsumer) consume(ctx context.Context) {
	// Initialize Sarama configuration
	config := sarama.NewConfig()

	// Set consumer group retry settings
	config.Consumer.Retry.Backoff = 2 * time.Second
	config.Consumer.Return.Errors = true

	// Set initial offset based on configuration
	if c.config.AutoOffsetReset == "earliest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// Create consumer group
	client, err := sarama.NewConsumerGroup([]string{c.config.BootstrapServers}, c.config.GroupID, config)
	if err != nil {
		logrus.WithError(err).Fatal("Error creating Sarama consumer group")
		return
	}

	c.client = client

	// Track errors from the consumer group
	go func() {
		for err := range client.Errors() {
			logrus.WithError(err).Error("Error from Sarama consumer")
		}
	}()

	// Create a handler for the consumer group
	handler := &saramaConsumerGroupHandler{
		orderService: c.orderService,
	}

	logrus.WithFields(logrus.Fields{
		"bootstrap_servers": c.config.BootstrapServers,
		"group_id":          c.config.GroupID,
		"topics":            c.config.Topics,
	}).Info("Sarama Kafka consumer started and waiting for messages")

	// Consume in a loop until context is canceled
	go func() {
		defer close(c.consumerClosed)
		for {
			// Consume should be called inside an infinite loop, as each call only consumes a single batch of messages
			// The context passed to Consume controls the lifetime of the consumer session
			if err := client.Consume(ctx, c.config.Topics, handler); err != nil {
				logrus.WithError(err).Error("Error from consumer")
			}

			// Check if context was canceled, indicating shutdown
			if ctx.Err() != nil {
				logrus.Info("Context canceled, stopping Sarama consumer")
				return
			}
		}
	}()

	// Wait for consumer to be closed
	<-c.consumerClosed
	logrus.Info("Sarama Kafka consumer closed")

	// Close the client
	if err := client.Close(); err != nil {
		logrus.WithError(err).Error("Error closing Sarama consumer group")
	}
}

// Wait waits for all consumer goroutines to finish
func (c *SaramaKafkaConsumer) Wait() {
	c.wg.Wait()
}

// saramaConsumerGroupHandler implements the sarama.ConsumerGroupHandler interface
type saramaConsumerGroupHandler struct {
	orderService *service.OrderService
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *saramaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *saramaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *saramaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// Loop over messages in the claim
	for message := range claim.Messages() {
		startTime := time.Now()
		messageID := uuid.New().String()

		// Process the message using the domain service
		description := "Note-" + messageID
		_, err := h.orderService.CreateOrder(description, 1)
		if err != nil {
			logrus.WithError(err).Error("Error creating order")
		}

		// Calculate processing time in milliseconds
		processingTimeMs := time.Since(startTime).Milliseconds()

		logrus.WithFields(logrus.Fields{
			"message_id":         messageID,
			"processing_time_ms": processingTimeMs,
			"topic":              message.Topic,
			"partition":          message.Partition,
			"offset":             message.Offset,
		}).Info("Message processed")

		// Mark the message as processed
		session.MarkMessage(message, "")
	}
	return nil
}
