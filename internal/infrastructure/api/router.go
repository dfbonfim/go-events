package api

import (
	"github.com/gin-gonic/gin"
)

// SetupRouter configures the HTTP router
func SetupRouter(handler *Handler) *gin.Engine {
	router := gin.Default()

	// Register routes
	router.GET("/ping", handler.PingHandler)
	router.GET("/hello", handler.HelloHandler)

	return router
}