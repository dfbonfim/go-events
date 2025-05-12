// Passo 2 e 3: Criar e implementar um consumidor Kafka básico no consumer.go
package kafka

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"goEvents/pkg/db"
	"sync"
	"time"
)

type Consumer struct {
	repository db.Repository
	wg         sync.WaitGroup
}

func NewConsumer(repository db.Repository) *Consumer {
	return &Consumer{
		repository: repository,
	}
}

func (c *Consumer) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consume(ctx)
	}()
}

func (c *Consumer) consume(ctx context.Context) {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		logrus.WithError(err).Fatal("Failed to create consumer")
		return
	}

	consumer.SubscribeTopics([]string{"myTopic"}, nil)

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
				messageID := uuid.New()

				// Create and save a new pedido
				pedido := &db.Pedido{
					Produto:    fmt.Sprintf("Produto-%s", messageID),
					Quantidade: 1,
					Status:     "pendente",
				}

				err := c.repository.SavePedido(pedido)
				if err != nil {
					logrus.WithError(err).Error("Error saving pedido")
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

func (c *Consumer) Wait() {
	c.wg.Wait()
}
