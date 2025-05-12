package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	"goEvents/pkg/db"
	"sync"
)

type Producer struct {
	repository  db.Repository
	producer    *kafka.Producer
	mutex       sync.Mutex
	initialized bool
}

func NewProducer(repository db.Repository) *Producer {
	return &Producer{
		repository:  repository,
		initialized: false,
	}
}

// Initialize creates the Kafka producer
func (p *Producer) Initialize() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.initialized {
		return nil
	}

	var err error
	p.producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		return fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Start a goroutine to handle delivery reports
	go func() {
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					logrus.WithError(ev.TopicPartition.Error).Error("Failed to deliver message")
				} else {
					logrus.WithFields(logrus.Fields{
						"topic":     *ev.TopicPartition.Topic,
						"partition": ev.TopicPartition.Partition,
						"offset":    ev.TopicPartition.Offset,
					}).Debug("Message delivered successfully")
				}
			}
		}
	}()

	p.initialized = true
	return nil
}

func (p *Producer) Publish(message string) error {
	if err := p.Initialize(); err != nil {
		return err
	}

	topic := "myTopic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}

	for i := 0; i < 100000; i++ {
		err := p.producer.Produce(msg, nil)
		if err != nil {
			logrus.WithError(err).Error("Failed to produce message")
			return err
		}
	}

	return nil
}

// Shutdown gracefully shuts down the producer
func (p *Producer) Shutdown(ctx context.Context) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.initialized || p.producer == nil {
		return
	}

	// Create a done channel to signal when flushing is complete
	done := make(chan bool)

	go func() {
		// Flush remaining messages
		unflushed := p.producer.Flush(5000) // Wait up to 5 seconds
		if unflushed > 0 {
			logrus.Warnf("%d messages were not flushed before timeout", unflushed)
		}

		// Close the producer
		p.producer.Close()
		logrus.Info("Kafka producer closed")

		close(done)
	}()

	// Wait for either flush/close to complete or context to be canceled
	select {
	case <-done:
		logrus.Info("Producer shutdown completed successfully")
	case <-ctx.Done():
		logrus.Warn("Producer shutdown timed out")
	}

	p.initialized = false
	p.producer = nil
}

// PublishMessage is kept for backward compatibility
// Deprecated: Use Producer.Publish instead
func PublishMessage(message string) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		logrus.WithError(err).Error("Failed to create producer")
		return
	}
	defer func() {
		producer.Flush(15 * 1000) // Wait up to 15 seconds to send pending messages
		producer.Close()
	}()

	topic := "myTopic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(message),
	}

	for i := 0; i < 10; i++ {
		err = producer.Produce(msg, nil)
		if err != nil {
			logrus.WithError(err).Error("Failed to produce message")
			return
		}
	}
}
