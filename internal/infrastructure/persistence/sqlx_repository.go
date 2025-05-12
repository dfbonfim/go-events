package persistence

import (
	"fmt"
	"goEvents/internal/domain/model"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// SQLxRepository implements the domain repository interfaces using sqlx
type SQLxRepository struct {
	db         *sqlx.DB
	dsn        string
	poolConfig DBPoolConfig
}

// NewSQLxRepository creates a new instance of SQLxRepository with default pool configuration
func NewSQLxRepository(dsn string) *SQLxRepository {
	// Use provided DSN or default if empty
	if dsn == "" {
		dsn = "kafka:kafka@tcp(127.0.0.1:3306)/db?charset=utf8mb4&parseTime=True&loc=Local"
	}

	return &SQLxRepository{
		db:         nil, // Will be initialized when Init is called
		dsn:        dsn,
		poolConfig: DefaultPoolConfig(),
	}
}

// NewSQLxRepositoryWithConfig creates a new instance of SQLxRepository with custom pool configuration
func NewSQLxRepositoryWithConfig(dsn string, poolConfig DBPoolConfig) *SQLxRepository {
	// Use provided DSN or default if empty
	if dsn == "" {
		dsn = "kafka:kafka@tcp(127.0.0.1:3306)/db?charset=utf8mb4&parseTime=True&loc=Local"
	}

	return &SQLxRepository{
		db:         nil, // Will be initialized when Init is called
		dsn:        dsn,
		poolConfig: poolConfig,
	}
}

// Init initializes database connection with connection pool settings
func (r *SQLxRepository) Init() error {
	db, err := sqlx.Connect("mysql", r.dsn)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}

	// Configure pool settings
	db.SetMaxOpenConns(r.poolConfig.MaxOpenConns)
	db.SetMaxIdleConns(r.poolConfig.MaxIdleConns)
	db.SetConnMaxLifetime(r.poolConfig.ConnMaxLifetime)
	db.SetConnMaxIdleTime(r.poolConfig.ConnMaxIdleTime)

	r.db = db

	// Create table if it doesn't exist
	schema := `
	CREATE TABLE IF NOT EXISTS order_entity_sqlx (
		id INT AUTO_INCREMENT PRIMARY KEY,
		description VARCHAR(255),
		quantity INT,
		status VARCHAR(50)
	);`

	_, err = r.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("error creating table: %w", err)
	}

	return nil
}

// SaveOrder saves a new order to the database
func (r *SQLxRepository) SaveOrder(order *model.Order) error {
	// Map domain model to entity
	entity := &OrderEntitySQLx{
		ID:          order.ID,
		Description: order.Description,
		Quantity:    order.Quantity,
		Status:      order.Status,
	}

	// Insert the record
	query := `INSERT INTO order_entity_sqlx (description, quantity, status) 
              VALUES (:description, :quantity, :status)`

	result, err := r.db.NamedExec(query, entity)
	if err != nil {
		log.Println("Error saving order:", err)
		return err
	}

	// Get the last inserted ID
	lastId, err := result.LastInsertId()
	if err != nil {
		log.Println("Error getting last inserted ID:", err)
		return err
	}

	// Update domain model with generated ID
	order.ID = uint(lastId)

	return nil
}

// Close closes the database connection
func (r *SQLxRepository) Close() error {
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}
