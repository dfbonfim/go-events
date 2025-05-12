package repository

import "goEvents/internal/domain/model"

// OrderRepository defines the contract for order persistence operations
type OrderRepository interface {
	SaveOrder(order *model.Order) error
}