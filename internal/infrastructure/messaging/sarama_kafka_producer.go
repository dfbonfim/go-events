package messaging

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

// SaramaKafkaProducer implements the MessageProducer interface using Sarama Kafka client
type SaramaKafkaProducer struct {
	producer    sarama.SyncProducer
	mutex       sync.Mutex
	initialized bool
	config      *sarama.Config
	brokers     []string
	topic       string
}

// NewSaramaKafkaProducer creates a new Kafka producer using Sarama library
func NewSaramaKafkaProducer(bootstrapServers string) *SaramaKafkaProducer {
	if bootstrapServers == "" {
		bootstrapServers = "localhost:9092"
	}

	// Create producer configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	return &SaramaKafkaProducer{
		initialized: false,
		config:      config,
		brokers:     []string{bootstrapServers},
		topic:       "orders",
	}
}

// Initialize creates the Kafka producer
func (p *SaramaKafkaProducer) Initialize() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.initialized {
		return nil
	}

	var err error
	p.producer, err = sarama.NewSyncProducer(p.brokers, p.config)
	if err != nil {
		return fmt.Errorf("failed to create Sarama Kafka producer: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"brokers": p.brokers,
		"topic":   p.topic,
	}).Info("Sarama Kafka producer initialized")

	p.initialized = true
	return nil
}

// PublishOrder publishes order messages to Kafka
func (p *SaramaKafkaProducer) PublishOrder(orderID string) error {
	if err := p.Initialize(); err != nil {
		return err
	}

	startTime := time.Now()

	// In a real-world scenario, you would likely not send 100,000 messages in a loop
	// This is just to maintain the same behavior as the ConfluentKafkaProducer
	for i := 0; i < 100000; i++ {
		// Create a message
		msg := &sarama.ProducerMessage{
			Topic: p.topic,
			Key:   sarama.StringEncoder(uuid.New().String()),
			Value: sarama.StringEncoder(orderID),
		}

		// Send the message
		_, _, err := p.producer.SendMessage(msg)
		if err != nil {
			logrus.WithError(err).Error("Failed to send message with Sarama")
			return err
		}
	}

	processingTimeMs := time.Since(startTime).Milliseconds()
	logrus.WithFields(logrus.Fields{
		"order_id":           orderID,
		"processing_time_ms": processingTimeMs,
	}).Info("Completed sending messages with Sarama")

	return nil
}

// Shutdown gracefully shuts down the producer
func (p *SaramaKafkaProducer) Shutdown(ctx context.Context) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.initialized || p.producer == nil {
		return
	}

	// Create a channel to signal when shutdown is complete
	done := make(chan bool)

	go func() {
		if err := p.producer.Close(); err != nil {
			logrus.WithError(err).Error("Error closing Sarama producer")
		} else {
			logrus.Info("Sarama producer closed successfully")
		}
		close(done)
	}()

	// Wait for either close to complete or context to be canceled
	select {
	case <-done:
		logrus.Info("Sarama producer shutdown completed successfully")
	case <-ctx.Done():
		logrus.Warn("Sarama producer shutdown timed out")
	}

	p.initialized = false
	p.producer = nil
}
