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

package destination

import (
	"context"
	"database/sql"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-db2/config"
	"github.com/conduitio-labs/conduit-connector-db2/destination/writer"

	_ "github.com/ibmdb/go_ibm_db" //nolint:revive,nolintlint
)

// Destination DB2 Connector persists records to a db2 database.
type Destination struct {
	sdk.UnimplementedDestination

	writer Writer
	config config.Config
}

// New creates new instance of the Destination.
func New() sdk.Destination {
	return &Destination{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyConnection: {
			Description: "Connection string to DB2",
			Required:    true,
			Default:     "",
		},
		config.KeyTable: {
			Description: "name of the table that the connector should write to.",
			Required:    true,
			Default:     "",
		},
		config.KeyPrimaryKey: {
			Description: "A column name that used to detect if the target table" +
				" already contains the record (destination). It must be unique",
			Required: true,
			Default:  "",
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := config.Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	db, err := sql.Open("go_ibm_db", d.config.Connection)
	if err != nil {
		return fmt.Errorf("connect to db2: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping db2: %w", err)
	}

	d.writer, err = writer.NewWriter(ctx, writer.Params{
		DB:        db,
		Table:     d.config.Table,
		KeyColumn: d.config.Key,
	})

	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		err := sdk.Util.Destination.Route(ctx, record,
			d.writer.Upsert,
			d.writer.Upsert,
			d.writer.Delete,
			d.writer.Upsert,
		)
		if err != nil {
			return i, fmt.Errorf("route %s: %w", record.Operation.String(), err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		return d.writer.Close(ctx)
	}

	return nil
}
