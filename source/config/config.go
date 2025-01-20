// Copyright © 2022 Meroxa, Inc & Yalantis.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:generate paramgen -output=paramgen.go Config

package config

import (
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-db2/common"
)

// Config holds source specific configurable values.
type Config struct {
	common.Configuration

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"orderingColumn" validate:"required"`
	// Columns  list of column names that should be included in each Record's payload.
	Columns []string `json:"columns"`
	// BatchSize is a size of rows batch.
	BatchSize int `json:"batchSize" default:"1000" validate:"gt=0,lt=100001"`
	// PrimaryKeys list of column names should use for their `Key` fields.
	PrimaryKeys []string `json:"primaryKeys"`
	// Snapshot whether or not the plugin will take a snapshot of the entire table before starting cdc.
	Snapshot bool `json:"snapshot" default:"true"`
}

// Init sets uppercase "orderingColumn", "columns" and "primaryKeys".
func (c Config) Init() Config {
	c.OrderingColumn = strings.ToUpper(c.OrderingColumn)

	// Convert columns to uppercase
	if len(c.Columns) > 0 {
		upperColumns := make([]string, len(c.Columns))
		for i, col := range c.Columns {
			upperColumns[i] = strings.ToUpper(col)
		}
		c.Columns = upperColumns
	}

	// Convert primary keys to uppercase
	if len(c.PrimaryKeys) > 0 {
		upperKeys := make([]string, len(c.PrimaryKeys))
		for i, key := range c.PrimaryKeys {
			upperKeys[i] = strings.ToUpper(key)
		}
		c.PrimaryKeys = upperKeys
	}

	return c
}

// Validate executes manual validations beyond what is defined in struct tags.
func (c *Config) Validate() error {
	// Validate common configuration
	err := c.Configuration.Validate()
	if err != nil {
		return err
	}

	// Validate OrderingColumn
	if len(c.OrderingColumn) > common.MaxConfigStringLength {
		return common.NewLessThanError(ConfigOrderingColumn, common.MaxConfigStringLength)
	}

	// Validate Columns
	if len(c.Columns) > 0 {
		// Check if Columns contain OrderingColumn when specified
		hasOrderingColumn := false
		for _, col := range c.Columns {
			if len(col) > 128 {
				return fmt.Errorf(`column %q length must be less than or equal to 128 characters`, col)
			}
			if col == c.OrderingColumn {
				hasOrderingColumn = true
			}
		}
		if !hasOrderingColumn {
			return fmt.Errorf(`columns must contain orderingColumn %q`, c.OrderingColumn)
		}
	}

	// Validate PrimaryKeys
	for _, key := range c.PrimaryKeys {
		if len(key) > 128 {
			return fmt.Errorf(
				`primaryKey %q length must be less than or equal to 128 characters`, key)
		}
	}

	return nil
}
