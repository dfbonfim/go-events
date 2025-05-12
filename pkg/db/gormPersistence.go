package db

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
)

type Pedido struct {
	ID         uint `gorm:"primaryKey"`
	Produto    string
	Quantidade int
	Status     string
}

// Repository interface for database operations
type Repository interface {
	SavePedido(pedido *Pedido) error
	Init() error
}

// GormRepository implements Repository interface
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
		log.Println("Error connecting to database:", err)
		return err
	}

	r.db = db

	// Create table if it doesn't exist
	err = db.AutoMigrate(&Pedido{})
	if err != nil {
		log.Println("Error in AutoMigrate:", err)
		return err
	}

	return nil
}

// SavePedido saves a new pedido to the database
func (r *GormRepository) SavePedido(pedido *Pedido) error {
	result := r.db.Create(pedido)
	if err := result.Error; err != nil {
		log.Println("Error saving pedido:", err)
		return err
	}

	log.Println("Pedido saved with ID:", pedido.ID)
	return nil
}

// Persist is kept for backward compatibility
// Deprecated: Use Repository.SavePedido instead
func Persist() {
	repo := NewGormRepository("")
	err := repo.Init()
	if err != nil {
		log.Fatal("Error initializing database:", err)
	}

	// Create a new pedido
	novoPedido := Pedido{
		Produto:    "Notebook",
		Quantidade: 2,
		Status:     "pendente",
	}

	err = repo.SavePedido(&novoPedido)
	if err != nil {
		log.Fatal("Error saving pedido:", err)
	}
}
