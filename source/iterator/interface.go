package iterator

import (
	"context"
)

// Repository interface.
type Repository interface {
	// GetData - get rows from table.
	GetData(ctx context.Context, table, key string, fields []string,
		offset, limit int) ([]map[string]interface{}, error)
	// Close - shutdown repository.
	Close() error
}
