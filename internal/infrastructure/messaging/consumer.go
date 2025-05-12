package messaging

import (
	"context"
)

// MessageConsumer defines the interface for message consuming systems
type MessageConsumer interface {
	// Start begins consuming messages in a goroutine
	Start(ctx context.Context)

	// Wait waits for all consumer goroutines to finish
	Wait()
}

// ConsumerConfig holds common configuration for message consumers
type ConsumerConfig struct {
	// BootstrapServers is a comma-separated list of host:port addresses of brokers
	BootstrapServers string
	// GroupID is the consumer group identifier
	GroupID string
	// Topics is a list of topics to subscribe to
	Topics []string
	// AutoOffsetReset defines where to start consuming if no offset is found
	// Values: "earliest", "latest"
	AutoOffsetReset string
}
