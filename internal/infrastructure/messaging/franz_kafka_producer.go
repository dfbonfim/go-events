package messaging

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
	"sync"
	"time"
)

// FranzKafkaProducer implements the MessageProducer interface using Franz-Go Kafka client
type FranzKafkaProducer struct {
	client      *kgo.Client
	mutex       sync.Mutex
	initialized bool
	opts        []kgo.Opt
}

// NewFranzKafkaProducer creates a new Kafka producer using Franz-Go library
func NewFranzKafkaProducer(bootstrapServers string) *FranzKafkaProducer {
	if bootstrapServers == "" {
		bootstrapServers = "localhost:9092"
	}

	// Create producer configuration
	opts := []kgo.Opt{
		kgo.SeedBrokers(bootstrapServers),
		kgo.ProducerBatchMaxBytes(1000000),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	return &FranzKafkaProducer{
		initialized: false,
		opts:        opts,
	}
}

// Initialize creates the Kafka producer
func (p *FranzKafkaProducer) Initialize() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.initialized {
		return nil
	}

	var err error
	p.client, err = kgo.NewClient(p.opts...)
	if err != nil {
		return fmt.Errorf("failed to create Franz-Go Kafka producer: %w", err)
	}

	logrus.Info("Franz-Go Kafka producer initialized")

	p.initialized = true
	return nil
}

// PublishOrder publishes order messages to Kafka
func (p *FranzKafkaProducer) PublishOrder(orderID string) error {
	if err := p.Initialize(); err != nil {
		return err
	}

	startTime := time.Now()
	topic := "orders"

	// In a real-world scenario, you would likely not send 100,000 messages in a loop
	// This is just to maintain the same behavior as the other implementations
	for i := 0; i < 100000; i++ {
		record := &kgo.Record{
			Topic: topic,
			Key:   []byte(uuid.New().String()),
			Value: []byte(orderID),
		}

		// Send the message
		if err := p.client.ProduceSync(context.Background(), record).FirstErr(); err != nil {
			logrus.WithError(err).Error("Failed to send message with Franz-Go")
			return err
		}
	}

	processingTimeMs := time.Since(startTime).Milliseconds()
	logrus.WithFields(logrus.Fields{
		"order_id":           orderID,
		"processing_time_ms": processingTimeMs,
	}).Info("Completed sending messages with Franz-Go")

	return nil
}

// Shutdown gracefully shuts down the producer
func (p *FranzKafkaProducer) Shutdown(ctx context.Context) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.initialized || p.client == nil {
		return
	}

	// Create a channel to signal when shutdown is complete
	done := make(chan bool)

	go func() {
		p.client.Close()
		logrus.Info("Franz-Go producer closed successfully")
		close(done)
	}()

	// Wait for either close to complete or context to be canceled
	select {
	case <-done:
		logrus.Info("Franz-Go producer shutdown completed successfully")
	case <-ctx.Done():
		logrus.Warn("Franz-Go producer shutdown timed out")
	}

	p.initialized = false
	p.client = nil
}
