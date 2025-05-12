package service

import (
	"github.com/sirupsen/logrus"
	"goEvents/internal/domain/model"
	"goEvents/internal/domain/repository"
)

// OrderService handles the business logic for orders
type OrderService struct {
	orderRepository repository.OrderRepository
}

// NewOrderService creates a new order service with the given repository
func NewOrderService(orderRepository repository.OrderRepository) *OrderService {
	return &OrderService{
		orderRepository: orderRepository,
	}
}

// CreateOrder creates a new order with the given details
func (s *OrderService) CreateOrder(description string, quantity int) (*model.Order, error) {
	order := &model.Order{
		Description: description,
		Quantity:    quantity,
		Status:      "pending",
	}

	err := s.orderRepository.SaveOrder(order)
	if err != nil {
		return nil, err
	}

	// Log the created order ID
	logrus.WithFields(logrus.Fields{
		"order_id":    order.ID,
		"description": order.Description,
		"quantity":    order.Quantity,
		"status":      order.Status,
	}).Info("Order created successfully")

	return order, nil
}