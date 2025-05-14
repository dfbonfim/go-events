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
