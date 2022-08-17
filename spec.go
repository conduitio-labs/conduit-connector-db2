// Copyright © 2022 Meroxa, Inc.
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

package db2

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-db2/config"
	"github.com/conduitio-labs/conduit-connector-db2/source"
)

type Spec struct{}

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "db2",
		Summary: "The DB2 source and destination plugin for Conduit, written in Go.",
		Description: "The DB2 connector is one of Conduit plugins. " +
			"It provides both, a source and a destination DB2 connector.",
		Version: "v0.1.0",
		Author:  "Meroxa, Inc.",
		SourceParams: map[string]sdk.Parameter{
			config.KeyConnection: {
				Description: "Connection string to DB2",
				Required:    true,
				Default:     "",
			},
			config.KeyTable: {
				Description: "A name of the table that the connector should write to.",
				Required:    true,
				Default:     "",
			},
			config.KeyPrimaryKey: {
				Description: "A name of column that connector will use for create record key",
				Required:    true,
				Default:     "",
			},
			source.KeyOrderingColumn: {
				Description: "A name of a column that the connector will use for ordering rows.",
				Required:    true,
				Default:     "",
			},
			source.KeyColumns: {
				Description: "The list of column names that should be included in each Record's payload",
				Required:    false,
				Default:     "",
			},
			source.KeyBatchSize: {
				Description: "The size of rows batch",
				Required:    false,
				Default:     "1000",
			},
		},
		DestinationParams: map[string]sdk.Parameter{
			config.KeyConnection: {
				Description: "Connection string to DB2",
				Required:    true,
				Default:     "",
			},
			config.KeyTable: {
				Description: "A name of the table that the connector should write to.",
				Required:    true,
				Default:     "",
			},
			config.KeyPrimaryKey: {
				Description: "A column name that used to detect if the target table" +
					" already contains the record (destination).",
				Required: true,
				Default:  "",
			},
		},
	}
}
