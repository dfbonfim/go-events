package messaging

import (
	"context"
)

// MessageProducer defines the interface for message producing systems
type MessageProducer interface {
	// Initialize sets up the producer
	Initialize() error

	// PublishOrder publishes an order message to the configured topic
	PublishOrder(orderID string) error

	// Shutdown gracefully shuts down the producer
	Shutdown(ctx context.Context)
}
