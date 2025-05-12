package api

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"goEvents/internal/domain/service"
	"goEvents/internal/infrastructure/messaging"
	"net/http"
)

// Handler handles HTTP requests
type Handler struct {
	orderService *service.OrderService
	producer     messaging.MessageProducer
}

// NewHandler creates a new API handler
func NewHandler(orderService *service.OrderService, producer messaging.MessageProducer) *Handler {
	return &Handler{
		orderService: orderService,
		producer:     producer,
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

	err := h.producer.PublishOrder("123")
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