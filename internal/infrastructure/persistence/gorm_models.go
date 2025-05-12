package persistence

// OrderEntity is the database entity for orders
type OrderEntity struct {
	ID          uint `gorm:"primaryKey"`
	Description string
	Quantity    int
	Status      string
}
