package persistence

import (
	"fmt"
	"goEvents/internal/domain/model"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
)

// GormRepository implements the domain repository interfaces
type GormRepository struct {
	db  *gorm.DB
	dsn string
}

// NewGormRepository creates a new instance of GormRepository
func NewGormRepository(dsn string) *GormRepository {
	// Use provided DSN or default if empty
	if dsn == "" {
		dsn = "kafka:kafka@tcp(127.0.0.1:3306)/db?charset=utf8mb4&parseTime=True&loc=Local"
	}

	return &GormRepository{
		db:  nil, // Will be initialized when Init is called
		dsn: dsn,
	}
}

// Init initializes database connection
func (r *GormRepository) Init() error {
	db, err := gorm.Open(mysql.Open(r.dsn), &gorm.Config{})
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}

	r.db = db

	// Create table if it doesn't exist
	err = db.AutoMigrate(&OrderEntity{})
	if err != nil {
		return fmt.Errorf("error in AutoMigrate: %w", err)
	}

	return nil
}

// SaveOrder saves a new order to the database
func (r *GormRepository) SaveOrder(order *model.Order) error {
	// Map domain model to entity
	entity := &OrderEntity{
		ID:          order.ID,
		Description: order.Description,
		Quantity:    order.Quantity,
		Status:      order.Status,
	}

	result := r.db.Create(entity)
	if err := result.Error; err != nil {
		log.Println("Error saving order:", err)
		return err
	}

	// Update domain model with generated ID
	order.ID = entity.ID

	return nil
}
