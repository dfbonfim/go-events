package persistence

// OrderEntitySQLx is the database entity for orders when using SQLx
type OrderEntitySQLx struct {
	ID          uint   `db:"id"`
	Description string `db:"description"`
	Quantity    int    `db:"quantity"`
	Status      string `db:"status"`
}

func (OrderEntity) TableName() string {
	return "order_entities"
}
