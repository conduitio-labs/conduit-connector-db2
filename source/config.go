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

package source

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/conduitio-labs/conduit-connector-db2/config"
	"github.com/conduitio-labs/conduit-connector-db2/validator"
)

const (
	// KeyOrderingColumn is a config name for an ordering column.
	KeyOrderingColumn = "orderingColumn"
	// KeyColumns is a config name for columns.
	KeyColumns = "columns"
	// KeyBatchSize is a config name for a batch size.
	KeyBatchSize = "batchSize"
	// KeyPrimaryKeys is a config name for primary keys.
	KeyPrimaryKeys = "primaryKeys"
	// KeySnapshot is a config name for snapshotMode.
	KeySnapshot = "snapshot"

	// snapshotDefault is a default value for the Snapshot field.
	snapshotDefault = true

	// defaultBatchSize is a default value for a BatchSize field.
	defaultBatchSize = 1000
)

// Config holds source specific configurable values.
type Config struct {
	config.Config

	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `key:"orderingColumn" validate:"required,max=128"`
	// Columns  list of column names that should be included in each Record's payload.
	Columns []string `key:"columns" validate:"contains_or_default=OrderingColumn,dive,max=128"`
	// BatchSize is a size of rows batch.
	BatchSize int `key:"batchSize" validate:"gte=1,lte=100000"`
	// PrimaryKeys list of column names should use for their `Key` fields.
	PrimaryKeys []string `validate:"dive,max=128"`
	// Snapshot whether or not the plugin will take a snapshot of the entire table before starting cdc.
	Snapshot bool
}

// Parse maps the incoming map to the Config and validates it.
func Parse(cfg map[string]string) (Config, error) {
	common, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	sourceConfig := Config{
		Config:         common,
		OrderingColumn: strings.ToUpper(cfg[KeyOrderingColumn]),
		BatchSize:      defaultBatchSize,
		Snapshot:       snapshotDefault,
	}

	if columns := cfg[KeyColumns]; columns != "" {
		sourceConfig.Columns = strings.Split(strings.ToUpper(strings.TrimSpace(columns)), ",")
	}

	if keys := cfg[KeyPrimaryKeys]; keys != "" {
		sourceConfig.PrimaryKeys = strings.Split(strings.ToUpper(strings.TrimSpace(keys)), ",")
	}

	if batchSize := cfg[KeyBatchSize]; batchSize != "" {
		sourceConfig.BatchSize, err = strconv.Atoi(batchSize)
		if err != nil {
			return Config{}, fmt.Errorf("parse batchSize: %w", err)
		}
	}

	if cfg[KeySnapshot] != "" {
		snapshot, err := strconv.ParseBool(cfg[KeySnapshot])
		if err != nil {
			return Config{}, fmt.Errorf("parse %q: %w", KeySnapshot, err)
		}

		sourceConfig.Snapshot = snapshot
	}

	if err = validator.Validate(&sourceConfig); err != nil {
		return Config{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
}
