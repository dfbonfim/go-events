package persistence

import "time"

// DBPoolConfig holds configuration for database connection pools
type DBPoolConfig struct {
	// Maximum number of open connections to the database
	MaxOpenConns int
	// Maximum number of idle connections in the pool
	MaxIdleConns int
	// Maximum amount of time a connection may be reused
	ConnMaxLifetime time.Duration
	// Maximum amount of time a connection may be idle
	ConnMaxIdleTime time.Duration
}

// DefaultPoolConfig returns a configuration with reasonable defaults
func DefaultPoolConfig() DBPoolConfig {
	return DBPoolConfig{
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxLifetime: 5 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}
}
