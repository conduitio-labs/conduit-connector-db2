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

package config

import (
	"fmt"
	"testing"

	"github.com/conduitio-labs/conduit-connector-db2/common"
	"github.com/matryer/is"
)

const (
	testConnection   = "db2://username:password@host:50000/database"
	testLongString   = "this_is_a_very_long_string_which_exceeds_max_config_string_limit_abcdefghijklmnopqrstuvwxyz_zyxwvutsrqponmlkjihgfedcba_xxxxxxxxxxxxx" //nolint:lll // long string for testing.
	testTableName    = "test_table"
	testOrderingCol  = "updated_at"
	defaultBatchSize = 1000
)

func TestValidateConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      Config
		wantErr error
	}{
		{
			name: "success_minimal_config",
			in: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
				Snapshot:       true,
			},
		},
		{
			name: "success_full_config",
			in: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "updated_at",
				Columns:        []string{"id", "name", "updated_at"},
				PrimaryKeys:    []string{"id"},
				BatchSize:      defaultBatchSize,
				Snapshot:       true,
			},
		},
		{
			name: "failure_column_too_long",
			in: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "id",
				Columns:        []string{testLongString},
				BatchSize:      defaultBatchSize,
			},
			wantErr: fmt.Errorf(
				`error validating "columns": column %q length must be less than or equal to 128 characters`, testLongString,
			),
		},
		{
			name: "failure_primary_key_too_long",
			in: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "id",
				PrimaryKeys:    []string{testLongString},
				BatchSize:      defaultBatchSize,
			},
			wantErr: fmt.Errorf(
				`error validating "primaryKeys": primaryKey %q length must be less than or equal to 128 characters`, testLongString,
			),
		},
		{
			name: "failure_columns_missing_ordering_column",
			in: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "updated_at",
				Columns:        []string{"id", "name"},
				BatchSize:      defaultBatchSize,
			},
			wantErr: fmt.Errorf(`error validating "columns": columns must contain orderingColumn "updated_at"`),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			err := tt.in.Validate()
			if tt.wantErr == nil {
				is.NoErr(err)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.wantErr.Error())
			}
		})
	}
}

func TestConfigInit(t *testing.T) {
	t.Parallel()
	is := is.New(t)

	tests := []struct {
		name     string
		input    Config
		expected Config
	}{
		{
			name: "convert_all_to_uppercase",
			input: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "updated_at",
				Columns:        []string{"id", "name", "updated_at"},
				PrimaryKeys:    []string{"id"},
				BatchSize:      defaultBatchSize,
			},
			expected: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "UPDATED_AT",
				Columns:        []string{"ID", "NAME", "UPDATED_AT"},
				PrimaryKeys:    []string{"ID"},
				BatchSize:      defaultBatchSize,
			},
		},
		{
			name: "empty_columns_and_primary_keys",
			input: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "id",
				BatchSize:      defaultBatchSize,
			},
			expected: Config{
				Configuration: common.Configuration{
					Connection: testConnection,
				},
				OrderingColumn: "ID",
				BatchSize:      defaultBatchSize,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.input.Init()

			is.Equal(result.OrderingColumn, tt.expected.OrderingColumn)
			is.Equal(len(result.Columns), len(tt.expected.Columns))
			is.Equal(len(result.PrimaryKeys), len(tt.expected.PrimaryKeys))

			for i, col := range result.Columns {
				is.Equal(col, tt.expected.Columns[i])
			}

			for i, key := range result.PrimaryKeys {
				is.Equal(key, tt.expected.PrimaryKeys[i])
			}

			is.Equal(result.BatchSize, tt.expected.BatchSize)
			is.Equal(result.Connection, tt.expected.Connection)
		})
	}
}
