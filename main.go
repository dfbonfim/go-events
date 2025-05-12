package main

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"goEvents/pkg/db"
	"goEvents/pkg/handlers"
	"goEvents/pkg/kafka"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.InfoLevel)

	// Create a context that will be canceled on SIGINT or SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to receive OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Listen for OS signals in a separate goroutine
	go func() {
		sig := <-sigs
		logrus.WithField("signal", sig.String()).Info("Received shutdown signal")
		cancel() // Cancel the context, triggering graceful shutdown
	}()

	// Initialize database repository
	dbRepo := db.NewGormRepository("")
	err := dbRepo.Init()
	if err != nil {
		logrus.Fatalf("Failed to initialize database: %v", err)
	}

	// Initialize Kafka consumer with DB repository dependency
	consumer := kafka.NewConsumer(dbRepo)

	// Initialize Kafka producer with DB repository dependency
	producer := kafka.NewProducer(dbRepo)
	if err := producer.Initialize(); err != nil {
		logrus.WithError(err).Fatal("Failed to initialize Kafka producer")
	}

	// Start multiple consumers with context
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(consumerIndex int) {
			defer wg.Done()
			logrus.WithField("consumer_index", consumerIndex).Info("Starting consumer")
			consumer.Start(ctx)
		}(i)
	}

	// Initialize handlers with DB repository and pass the producer
	handler := handlers.NewHandler(dbRepo)

	// Setup Gin router
	router := gin.Default()

	// Register new handlers with dependency injection
	router.GET("/ping", handler.PingHandler)
	router.GET("/hello", handler.HelloHandler)

	// Create HTTP server with the router
	srv := &http.Server{
		Addr:    ":8081",
		Handler: router,
	}

	// Start HTTP server in a separate goroutine
	go func() {
		logrus.Info("Starting HTTP server on :8081")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.WithError(err).Fatal("Could not start HTTP server")
		}
	}()

	// Wait for context cancelation (from OS signals)
	<-ctx.Done()
	logrus.Info("Shutting down application...")

	// Create a timeout context for shutdowns
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server gracefully
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logrus.WithError(err).Error("HTTP server shutdown error")
	} else {
		logrus.Info("HTTP server shutdown complete")
	}

	// Shutdown Kafka producer
	logrus.Info("Shutting down Kafka producer")
	producer.Shutdown(shutdownCtx)

	// Wait for all consumers to finish
	logrus.Info("Waiting for all Kafka consumers to finish")
	consumer.Wait()

	// Wait for any remaining goroutines
	wg.Wait()

	logrus.Info("Application shutdown completed")
}
