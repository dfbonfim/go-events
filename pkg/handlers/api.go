package handlers

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"goEvents/pkg/db"
	"goEvents/pkg/kafka"
	"net/http"
)

// Handler struct for dependency injection
type Handler struct {
	producer *kafka.Producer
}

// NewHandler creates a new Handler with dependencies
func NewHandler(repository db.Repository) *Handler {
	return &Handler{
		producer: kafka.NewProducer(repository),
	}
}

// PingHandler handles ping requests
func (h *Handler) PingHandler(c *gin.Context) {
	if err := h.producer.Initialize(); err != nil {
		logrus.WithError(err).Error("Failed to initialize producer")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to initialize producer",
		})
		return
	}

	err := h.producer.Publish("123")
	if err != nil {
		logrus.WithError(err).Error("Failed to publish message")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to publish message",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
}

// HelloHandler handles hello requests
func (h *Handler) HelloHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Hello, World!",
	})
}
