package model

// Order represents an order in the domain
type Order struct {
	ID          uint
	Description string
	Quantity    int
	Status      string
}