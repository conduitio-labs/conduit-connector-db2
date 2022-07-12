package config

import (
	"fmt"

	"github.com/conduitio-labs/conduit-connector-db2/validator"
)

const (
	KeyConnection string = "connection"
	KeyTable      string = "table"
	KeyPrimaryKey string = "primaryKey"
)

// Config contains configurable values
// shared between source and destination DB2 connector.
type Config struct {
	// Connection string connection to DB2 database.
	Connection string `validate:"required"`
	// Table is a name of the table that the connector should write to or read from.
	Table string `validate:"required,max=128"`
	// Key - Column name that records should use for their `Key` fields.
	Key string `validate:"required,max=128"`
}

// Parse attempts to parse a provided map[string]string into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		Connection: cfg[KeyConnection],
		Table:      cfg[KeyTable],
		Key:        cfg[KeyPrimaryKey],
	}

	if err := validator.Validate(&config); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}
